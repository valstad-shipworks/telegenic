//! The clock every worker and waiter reads.

pub(crate) use std::time::{Instant, SystemTime};

pub(crate) fn system_now() -> SystemTime {
    SystemTime::now()
}

pub(crate) fn unix_nanos_now() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}
