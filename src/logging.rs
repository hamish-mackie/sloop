use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(Clone)]
pub struct OperationalLog {
    file: Arc<Mutex<File>>,
}

impl OperationalLog {
    pub fn open(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn emit(&self, level: LogLevel, target: &'static str, event: &'static str) {
        self.emit_fields(level, target, event, None);
    }

    /// Writes caller-selected operational context. Callers must keep fields
    /// to identifiers, classifications, and errors; prompts and credentials
    /// never belong in the operational log.
    pub fn emit_with_fields(
        &self,
        level: LogLevel,
        target: &'static str,
        event: &'static str,
        fields: Value,
    ) {
        self.emit_fields(level, target, event, Some(fields));
    }

    fn emit_fields(
        &self,
        level: LogLevel,
        target: &'static str,
        event: &'static str,
        fields: Option<Value>,
    ) {
        let timestamp = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into());
        let record = OperationalRecord {
            timestamp,
            level,
            target,
            event,
            fields,
        };
        let Ok(mut file) = self.file.lock() else {
            return;
        };
        let Ok(mut line) = serde_json::to_vec(&record) else {
            return;
        };
        line.push(b'\n');
        let Ok(original_len) = file.metadata().map(|metadata| metadata.len()) else {
            return;
        };
        if file.write_all(&line).and_then(|()| file.flush()).is_err() {
            // A full filesystem can leave a short write. Remove it so a later
            // successful record still begins on a valid NDJSON boundary.
            let _ = file.set_len(original_len);
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Error,
    Warn,
    Info,
}

#[derive(Debug, Serialize)]
struct OperationalRecord {
    timestamp: String,
    level: LogLevel,
    target: &'static str,
    event: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<Value>,
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::Value;
    use tempfile::tempdir;

    use super::{LogLevel, OperationalLog};

    #[test]
    fn writes_one_structured_record_per_line() {
        let root = tempdir().unwrap();
        let path = root.path().join("logs/daemon.ndjson");
        let log = OperationalLog::open(&path).unwrap();

        log.emit(LogLevel::Info, "sloop::daemon", "daemon_started");

        let contents = fs::read_to_string(path).unwrap();
        let record: Value = serde_json::from_str(contents.trim_end()).unwrap();
        assert_eq!(record["level"], "info");
        assert_eq!(record["target"], "sloop::daemon");
        assert_eq!(record["event"], "daemon_started");
        assert!(record["timestamp"].as_str().unwrap().ends_with('Z'));
    }

    #[test]
    fn writes_safe_context_as_structured_fields() {
        let root = tempdir().unwrap();
        let path = root.path().join("logs/daemon.ndjson");
        let log = OperationalLog::open(&path).unwrap();

        log.emit_with_fields(
            LogLevel::Error,
            "sloop::dispatcher",
            "run_exit_persist_failed",
            serde_json::json!({"run_id": "R7", "error": "database is busy"}),
        );

        let contents = fs::read_to_string(path).unwrap();
        let record: Value = serde_json::from_str(contents.trim_end()).unwrap();
        assert_eq!(record["fields"]["run_id"], "R7");
        assert_eq!(record["fields"]["error"], "database is busy");
    }
}
