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

fn local_minute(timestamp_ms: i64) -> u16 {
    let seconds = timestamp_ms.div_euclid(1_000) as libc::time_t;
    let mut local = unsafe { std::mem::zeroed::<libc::tm>() };
    let result = unsafe { libc::localtime_r(&seconds, &mut local) };
    if result.is_null() {
        return 0;
    }
    (local.tm_hour as u16) * 60 + local.tm_min as u16
}
