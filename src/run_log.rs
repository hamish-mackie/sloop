//! Per-run NDJSON output capture. Agent and aftercare stdout/stderr are
//! untrusted evidence: they are stored as ordered chunks, never parsed as
//! lines and never routed through the dispatcher.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputSource {
    Agent,
    Aftercare,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// One captured chunk. UTF-8 chunks stay readable; anything else round-trips
/// through base64 rather than being lossily converted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "encoding", rename_all = "snake_case")]
pub enum OutputChunk {
    Utf8 { text: String },
    Base64 { data: String },
}

impl OutputChunk {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        match std::str::from_utf8(bytes) {
            Ok(text) => Self::Utf8 { text: text.into() },
            Err(_) => Self::Base64 {
                data: BASE64.encode(bytes),
            },
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Self::Utf8 { text } => text.into_bytes(),
            Self::Base64 { data } => BASE64.decode(data).unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRecord {
    pub sequence: u64,
    pub timestamp: String,
    pub source: OutputSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    pub stream: OutputStream,
    #[serde(flatten)]
    pub chunk: OutputChunk,
}

/// Append-only writer shared by the stdout and stderr reader threads of one
/// run. Sequence numbers are capture order across both pipes.
#[derive(Clone)]
pub struct RunLogWriter {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    file: File,
    next_sequence: u64,
}

impl RunLogWriter {
    /// Opens (creating directories as needed) the run's output log and
    /// resumes sequence numbering after any records already present, so a
    /// restarted daemon appends instead of renumbering.
    pub fn open(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let next_sequence = last_sequence(path)?.map_or(1, |last| last + 1);
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        // A crash can leave a partial record with no trailing newline; the
        // next record must start on its own line or it would be corrupted too.
        if !ends_with_newline(path)? {
            file.write_all(b"\n")?;
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                file,
                next_sequence,
            })),
        })
    }

    /// Appends one chunk and durably flushes it. Capture must be on disk
    /// before exit evidence claims it is complete.
    pub fn append(
        &self,
        source: OutputSource,
        stage: Option<&str>,
        stream: OutputStream,
        bytes: &[u8],
    ) -> io::Result<u64> {
        let timestamp = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into());
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("run log lock poisoned"))?;
        let record = OutputRecord {
            sequence: inner.next_sequence,
            timestamp,
            source,
            stage: stage.map(str::to_owned),
            stream,
            chunk: OutputChunk::from_bytes(bytes),
        };
        let mut line = serde_json::to_vec(&record).map_err(io::Error::other)?;
        line.push(b'\n');
        let original_len = inner.file.metadata()?.len();
        if let Err(error) = inner
            .file
            .write_all(&line)
            .and_then(|()| inner.file.sync_data())
        {
            // Keep the previous complete-record boundary after a short write,
            // especially ENOSPC, so future appends cannot join corrupt JSON.
            let _ = inner.file.set_len(original_len);
            return Err(error);
        }
        inner.next_sequence += 1;
        Ok(record.sequence)
    }
}

/// True when the file is empty or its last byte is a newline, meaning the
/// next append starts a fresh record.
fn ends_with_newline(path: &Path) -> io::Result<bool> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() == 0 {
        return Ok(true);
    }
    file.seek(SeekFrom::End(-1))?;
    let mut last = [0u8; 1];
    file.read_exact(&mut last)?;
    Ok(last[0] == b'\n')
}

/// The sequence of the last complete record, ignoring a truncated tail left
/// by a crash; earlier records stay valid evidence.
fn last_sequence(path: &Path) -> io::Result<Option<u64>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut last = None;
    for line in BufReader::new(file).lines() {
        if let Ok(record) = serde_json::from_str::<OutputRecord>(&line?) {
            last = Some(record.sequence);
        }
    }
    Ok(last)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputPage {
    pub entries: Vec<OutputRecord>,
    /// Sequence of the last returned entry; the cursor a future paginated
    /// call would resume after.
    pub next_cursor: u64,
    /// True when the page reached the end of the captured records.
    pub complete: bool,
}

/// Reads a finite page of records with `sequence > after`, in order. A
/// missing file is an empty page: the run may exist before any output does.
pub fn read_page(path: &Path, after: u64, limit: usize) -> io::Result<OutputPage> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(OutputPage {
                entries: Vec::new(),
                next_cursor: after,
                complete: true,
            });
        }
        Err(error) => return Err(error),
    };

    let mut entries = Vec::new();
    let mut complete = true;
    for line in BufReader::new(file).lines() {
        // An unparsable line is an incomplete tail, not corruption of the
        // records before it.
        let Ok(record) = serde_json::from_str::<OutputRecord>(&line?) else {
            continue;
        };
        if record.sequence <= after {
            continue;
        }
        if entries.len() == limit {
            complete = false;
            break;
        }
        entries.push(record);
    }
    let next_cursor = entries.last().map_or(after, |record| record.sequence);
    Ok(OutputPage {
        entries,
        next_cursor,
        complete,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use tempfile::tempdir;

    use super::{OutputChunk, OutputSource, OutputStream, RunLogWriter, read_page};

    #[test]
    fn utf8_and_binary_chunks_serialize_to_the_documented_shapes() {
        let writer_dir = tempdir().unwrap();
        let path = writer_dir.path().join("runs/R1/output.ndjson");
        let writer = RunLogWriter::open(&path).unwrap();

        writer
            .append(OutputSource::Agent, None, OutputStream::Stdout, b"hello\n")
            .unwrap();
        writer
            .append(
                OutputSource::Aftercare,
                Some("test"),
                OutputStream::Stderr,
                &[0xff, 0x00],
            )
            .unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        let records: Vec<Value> = contents
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(records[0]["sequence"], 1);
        assert_eq!(records[0]["source"], "agent");
        assert_eq!(records[0]["stream"], "stdout");
        assert_eq!(records[0]["encoding"], "utf8");
        assert_eq!(records[0]["text"], "hello\n");
        assert_eq!(records[0].get("stage"), None);
        assert!(records[0]["timestamp"].as_str().unwrap().ends_with('Z'));

        assert_eq!(records[1]["sequence"], 2);
        assert_eq!(records[1]["source"], "aftercare");
        assert_eq!(records[1]["stage"], "test");
        assert_eq!(records[1]["encoding"], "base64");
        assert_eq!(records[1]["data"], "/wA=");
    }

    #[test]
    fn binary_chunks_round_trip_without_loss() {
        let bytes = [0xff, 0xfe, 0x00, 0x41];
        let chunk = OutputChunk::from_bytes(&bytes);
        assert!(matches!(chunk, OutputChunk::Base64 { .. }));
        assert_eq!(chunk.into_bytes(), bytes);
    }

    #[test]
    fn reopening_appends_after_existing_records() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("output.ndjson");

        let writer = RunLogWriter::open(&path).unwrap();
        writer
            .append(OutputSource::Agent, None, OutputStream::Stdout, b"one")
            .unwrap();
        drop(writer);

        let writer = RunLogWriter::open(&path).unwrap();
        let sequence = writer
            .append(OutputSource::Agent, None, OutputStream::Stdout, b"two")
            .unwrap();
        assert_eq!(sequence, 2);

        let page = read_page(&path, 0, 10).unwrap();
        assert_eq!(page.entries.len(), 2);
        assert_eq!(
            page.entries[1].chunk,
            OutputChunk::Utf8 { text: "two".into() }
        );
    }

    #[test]
    fn a_truncated_tail_hides_no_earlier_records() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("output.ndjson");
        let writer = RunLogWriter::open(&path).unwrap();
        writer
            .append(OutputSource::Agent, None, OutputStream::Stdout, b"kept")
            .unwrap();
        drop(writer);

        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"{\"sequence\":2,\"timest").unwrap();
        drop(file);

        let page = read_page(&path, 0, 10).unwrap();
        assert_eq!(page.entries.len(), 1);
        assert!(page.complete);

        // The partial record was never complete, so its sequence is free to
        // reuse, and the new record must land on its own line.
        let writer = RunLogWriter::open(&path).unwrap();
        let sequence = writer
            .append(OutputSource::Agent, None, OutputStream::Stdout, b"next")
            .unwrap();
        assert_eq!(sequence, 2);

        let page = read_page(&path, 0, 10).unwrap();
        assert_eq!(page.entries.len(), 2);
        assert_eq!(
            page.entries[1].chunk,
            OutputChunk::Utf8 {
                text: "next".into()
            }
        );
    }

    #[test]
    fn pagination_is_stable_across_sequence_cursors() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("output.ndjson");
        let writer = RunLogWriter::open(&path).unwrap();
        for index in 0..5 {
            writer
                .append(
                    OutputSource::Agent,
                    None,
                    OutputStream::Stdout,
                    format!("chunk {index}").as_bytes(),
                )
                .unwrap();
        }

        let first = read_page(&path, 0, 2).unwrap();
        assert_eq!(first.entries.len(), 2);
        assert_eq!(first.next_cursor, 2);
        assert!(!first.complete);

        let second = read_page(&path, first.next_cursor, 10).unwrap();
        assert_eq!(second.entries.len(), 3);
        assert_eq!(second.next_cursor, 5);
        assert!(second.complete);

        let missing = read_page(&directory.path().join("absent.ndjson"), 0, 10).unwrap();
        assert!(missing.entries.is_empty() && missing.complete);
    }

    #[test]
    fn records_round_trip_through_serde() {
        let record = super::OutputRecord {
            sequence: 7,
            timestamp: "2026-07-13T20:00:01Z".into(),
            source: OutputSource::Aftercare,
            stage: Some("test".into()),
            stream: OutputStream::Stderr,
            chunk: OutputChunk::Utf8 { text: "x".into() },
        };
        let encoded = serde_json::to_value(&record).unwrap();
        assert_eq!(
            encoded,
            json!({
                "sequence": 7,
                "timestamp": "2026-07-13T20:00:01Z",
                "source": "aftercare",
                "stage": "test",
                "stream": "stderr",
                "encoding": "utf8",
                "text": "x"
            })
        );
        let decoded: super::OutputRecord = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, record);
    }
}
