use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
    fn local_minute(&self, timestamp_ms: i64) -> u16;
    fn sleep_until(&self, deadline_ms: i64) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as i64)
            .unwrap_or(0)
    }

    fn local_minute(&self, timestamp_ms: i64) -> u16 {
        local_minute(timestamp_ms)
    }

    fn sleep_until(&self, deadline_ms: i64) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            loop {
                let delay_ms = deadline_ms.saturating_sub(self.now_ms());
                if delay_ms <= 0 {
                    return;
                }
                // Recheck wall time so suspend and clock corrections cannot
                // leave a long monotonic sleep armed past the local opening.
                tokio::time::sleep(Duration::from_millis(delay_ms.min(30_000) as u64)).await;
            }
        })
    }
}

/// File-backed clock used by integration tests that run the daemon as a child
/// process. Advancing the integer timestamp in the file wakes pending timers.
#[derive(Debug)]
pub struct FileClock {
    path: PathBuf,
}

impl FileClock {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn read_now_ms(&self) -> i64 {
        std::fs::read_to_string(&self.path)
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0)
    }
}

impl Clock for FileClock {
    fn now_ms(&self) -> i64 {
        self.read_now_ms()
    }

    fn local_minute(&self, timestamp_ms: i64) -> u16 {
        local_minute(timestamp_ms)
    }

    fn sleep_until(&self, deadline_ms: i64) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            while self.now_ms() < deadline_ms {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    }
}

pub fn format_timestamp(timestamp_ms: i64) -> Option<String> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ms) * 1_000_000)
        .ok()?
        .format(&Rfc3339)
        .ok()
}

/// Finds the next real instant whose local wall clock matches `minute`.
/// Starting at the next whole minute makes the current minute mean its next
/// occurrence, while scanning real instants handles DST transitions.
pub fn next_local_minute_ms(clock: &dyn Clock, now_ms: i64, minute: u16) -> Option<i64> {
    let mut candidate = now_ms
        .div_euclid(60_000)
        .checked_add(1)?
        .checked_mul(60_000)?;
    for _ in 0..=(49 * 60) {
        if clock.local_minute(candidate) == minute {
            return Some(candidate);
        }
        candidate = candidate.checked_add(60_000)?;
    }
    None
}

/// A timestamp's local wall-clock fields. History views render times the way
/// an operator reads a clock, so they need the local date as well as the
/// local time: a span that crosses midnight has to widen from `HH:MM` to a
/// dated form, and only the calendar day can tell them that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalTime {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
}

impl LocalTime {
    /// `HH:MM` — the default form, used whenever the surrounding context
    /// already fixes the day.
    pub fn clock(&self) -> String {
        format!("{:02}:{:02}", self.hour, self.minute)
    }

    /// `MM-DD HH:MM` — the widened form for a timestamp whose day the reader
    /// cannot infer. Year is omitted; `sloop` history is read in the small.
    pub fn dated(&self) -> String {
        format!(
            "{:02}-{:02} {:02}:{:02}",
            self.month, self.day, self.hour, self.minute
        )
    }

    /// Whether two instants fall on the same local calendar day, which is what
    /// decides between `clock` and `dated`.
    pub fn same_day(&self, other: &Self) -> bool {
        (self.year, self.month, self.day) == (other.year, other.month, other.day)
    }
}

/// Breaks a millisecond timestamp into local wall-clock fields. Uses the same
/// `localtime_r` path as `local_minute` so scheduling and rendering agree on
/// what "local" means, including across DST.
pub fn local_time(timestamp_ms: i64) -> Option<LocalTime> {
    let seconds = timestamp_ms.div_euclid(1_000) as libc::time_t;
    let mut local = unsafe { std::mem::zeroed::<libc::tm>() };
    let result = unsafe { libc::localtime_r(&seconds, &mut local) };
    if result.is_null() {
        return None;
    }
    Some(LocalTime {
        year: local.tm_year + 1900,
        month: (local.tm_mon + 1) as u8,
        day: local.tm_mday as u8,
        hour: local.tm_hour as u8,
        minute: local.tm_min as u8,
    })
}

fn local_minute(timestamp_ms: i64) -> u16 {
    let seconds = timestamp_ms.div_euclid(1_000) as libc::time_t;
    let mut local = unsafe { std::mem::zeroed::<libc::tm>() };
    let result = unsafe { libc::localtime_r(&seconds, &mut local) };
    if result.is_null() {
        return 0;
    }
    (local.tm_hour as u16) * 60 + local.tm_min as u16
}
