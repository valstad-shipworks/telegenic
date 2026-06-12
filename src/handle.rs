//! The response handle returned by every GVCP transaction.
//!
//! [`ResponseHandle<T>`] is an `Arc<(OnceLock, Event)>` the worker
//! fulfils, offering sync `wait_timeout`
//! and a `Future` impl. The recorded value is `Result<T, Arc<CameraError>>`
//! so the error side is cheaply cloneable across waiters.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use event_listener::{Event, Listener};

use crate::error::CameraError;

pub type Response<T> = Result<T, Arc<CameraError>>;

pub struct ResponseHandle<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for ResponseHandle<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct Inner<T> {
    cell: OnceLock<(SystemTime, Response<T>)>,
    event: Event,
}

impl<T: Clone> ResponseHandle<T> {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                cell: OnceLock::new(),
                event: Event::new(),
            }),
        }
    }

    /// Worker-side: deposit the outcome. Idempotent (`OnceLock`).
    pub(crate) fn fulfill(&self, value: Response<T>) {
        let _ = self.inner.cell.set((SystemTime::now(), value));
        self.inner.event.notify(usize::MAX);
    }

    pub(crate) fn fail(&self, err: CameraError) {
        self.fulfill(Err(Arc::new(err)));
    }

    pub fn is_set(&self) -> bool {
        self.inner.cell.get().is_some()
    }

    pub fn timestamp(&self) -> Option<SystemTime> {
        self.inner.cell.get().map(|(t, _)| *t)
    }

    /// Non-blocking peek. `Err(Timeout)` if nothing has been recorded yet.
    pub fn get(&self) -> Response<T> {
        match self.inner.cell.get() {
            Some((_, v)) => v.clone(),
            None => Err(Arc::new(CameraError::Timeout)),
        }
    }

    pub fn wait(&self) -> Response<T> {
        self.wait_timeout(Duration::MAX)
    }

    pub fn wait_timeout(&self, timeout: Duration) -> Response<T> {
        // Register the listener before checking, so a fulfill racing between
        // the check and the listen can't be lost.
        let listener = self.inner.event.listen();
        if self.is_set() {
            return self.get();
        }
        if listener.wait_timeout(timeout).is_some() || self.is_set() {
            self.get()
        } else {
            Err(Arc::new(CameraError::Timeout))
        }
    }
}

impl<T: Clone> Future for ResponseHandle<T> {
    type Output = Response<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let listener = self.inner.event.listen();
        if self.is_set() {
            return Poll::Ready(self.get());
        }
        let mut pinned = std::pin::pin!(listener);
        pinned.as_mut().poll(cx).map(|_| self.get())
    }
}

impl<T> std::fmt::Debug for ResponseHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseHandle")
            .field("fulfilled", &self.inner.cell.get().is_some())
            .finish()
    }
}

/// Unwrap a shared error for APIs that return `CameraError` by value.
pub(crate) fn unwrap_arc(e: Arc<CameraError>) -> CameraError {
    match Arc::try_unwrap(e) {
        Ok(err) => err,
        Err(shared) => CameraError::Spawn(shared.to_string()),
    }
}
