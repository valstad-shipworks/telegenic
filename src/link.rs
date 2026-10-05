//! Link-health counters shared by a camera's control and stream workers.

use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative link-health counters of one camera, across reconnects and
/// stream channels. Unlike [`gige::LinkStats`](crate::gige::LinkStats), which
/// covers the current control connection only, these never reset.
#[cfg_attr(feature = "valuable", derive(valuable::Valuable))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LinkStats {
    /// GVCP commands sent again after their acknowledge timed out.
    pub gvcp_retransmits: u64,
    /// Frames closed with packets missing or out of range.
    pub frames_incomplete: u64,
    /// Frames closed because no packet arrived for the retention window.
    pub frames_timed_out: u64,
}

#[derive(Debug, Default)]
pub(crate) struct LinkCounters {
    gvcp_retransmits: AtomicU64,
    frames_incomplete: AtomicU64,
    frames_timed_out: AtomicU64,
}

impl LinkCounters {
    pub(crate) fn retransmit(&self) {
        self.gvcp_retransmits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn frame_incomplete(&self) {
        self.frames_incomplete.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn frame_timed_out(&self) {
        self.frames_timed_out.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> LinkStats {
        LinkStats {
            gvcp_retransmits: self.gvcp_retransmits.load(Ordering::Relaxed),
            frames_incomplete: self.frames_incomplete.load(Ordering::Relaxed),
            frames_timed_out: self.frames_timed_out.load(Ordering::Relaxed),
        }
    }
}
