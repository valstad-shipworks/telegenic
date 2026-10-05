//! The join/wake handle shared between a worker thread and its owner.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use mio::Waker;

use crate::handle::ResponseHandle;

/// Owner/worker split handle: the owner can ask the worker to stop and wake it
/// from a blocking poll; the worker reports liveness and, once, its exit. The
/// owning side joins the worker on drop; the non-owning twin handed into the
/// thread cannot accidentally tear it down.
#[derive(Debug)]
pub(crate) struct ThreadHandle {
    is_owner: bool,
    is_alive: Arc<AtomicBool>,
    should_die: Arc<AtomicBool>,
    exited: ResponseHandle<()>,
    handle: Option<JoinHandle<()>>,
    waker: Option<Arc<Waker>>,
}

impl ThreadHandle {
    pub fn new() -> Self {
        Self {
            is_owner: true,
            is_alive: Arc::new(AtomicBool::new(true)),
            should_die: Arc::new(AtomicBool::new(false)),
            exited: ResponseHandle::new(),
            handle: None,
            waker: None,
        }
    }

    pub fn set_handle(&mut self, handle: JoinHandle<()>) {
        self.handle = Some(handle);
    }

    pub fn set_waker(&mut self, waker: Arc<Waker>) {
        self.waker = Some(waker);
    }

    pub fn wake(&self) -> io::Result<()> {
        match &self.waker {
            Some(w) => w.wake(),
            None => Ok(()),
        }
    }

    pub fn is_alive(&self) -> bool {
        self.is_alive.load(Ordering::Relaxed)
    }

    pub fn should_live(&self) -> bool {
        !self.should_die.load(Ordering::Relaxed)
    }

    pub fn has_died(&self) {
        self.is_alive.store(false, Ordering::Relaxed);
        self.exited.fulfill(Ok(()));
    }

    /// Wait up to `timeout` for the worker to report its exit; `true` once it
    /// has.
    pub fn wait_exited(&self, timeout: Duration) -> bool {
        self.exited.wait_timeout(timeout).is_ok()
    }

    /// Ask the worker to exit at the next loop turn. Does **not** join — the
    /// owning `Drop` does that. Safe to call from any thread.
    pub fn request_stop(&self) {
        self.should_die.store(true, Ordering::Relaxed);
        let _ = self.wake();
    }

    /// Produce the non-owning twin to hand into the spawned thread.
    pub fn to_pass_in(&self) -> Self {
        Self {
            is_owner: false,
            is_alive: self.is_alive.clone(),
            should_die: self.should_die.clone(),
            exited: self.exited.clone(),
            handle: None,
            waker: self.waker.clone(),
        }
    }
}

/// Reports the worker's exit when dropped, so an owner waiting on it is
/// released even if the worker unwinds.
pub(crate) struct ExitGuard(pub(crate) ThreadHandle);

impl Drop for ExitGuard {
    fn drop(&mut self) {
        self.0.has_died();
    }
}

impl Drop for ThreadHandle {
    fn drop(&mut self) {
        if !self.is_owner {
            return;
        }
        self.should_die.store(true, Ordering::Relaxed);
        let _ = self.wake();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
