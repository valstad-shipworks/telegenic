//! The response handle returned by every GVCP transaction.
//!
//! [`ResponseHandle<T>`] is an `Arc<(OnceLock, Event)>` the worker
//! fulfils, offering sync `wait_timeout`
//! and a `Future` impl. The recorded value is `Result<T, Arc<CameraError>>`
//! so the error side is cheaply cloneable across waiters.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, SystemTime};

use event_listener::{Event, Listener};
use parking_lot::Mutex;

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
    // `event` wakes blocking waiters; `wakers` wakes every task
    // polling a clone. Both are signalled after `cell` is set.
    event: Event,
    wakers: Mutex<Vec<Waker>>,
}

impl<T: Clone> ResponseHandle<T> {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                cell: OnceLock::new(),
                event: Event::new(),
                wakers: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Worker-side: deposit the outcome. Idempotent (`OnceLock`).
    pub(crate) fn fulfill(&self, value: Response<T>) {
        let _ = self.inner.cell.set((crate::clock::system_now(), value));
        self.inner.event.notify(usize::MAX);
        for waker in std::mem::take(&mut *self.inner.wakers.lock()) {
            waker.wake();
        }
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
        if self.is_set() {
            return self.get();
        }
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
        if self.is_set() {
            return Poll::Ready(self.get());
        }
        {
            let mut wakers = self.inner.wakers.lock();
            match wakers.iter_mut().find(|w| w.will_wake(cx.waker())) {
                Some(w) => w.clone_from(cx.waker()),
                None => wakers.push(cx.waker().clone()),
            }
        }
        if self.is_set() {
            Poll::Ready(self.get())
        } else {
            Poll::Pending
        }
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
        Err(shared) => duplicate(&shared),
    }
}

fn duplicate(e: &CameraError) -> CameraError {
    match e {
        CameraError::Io(io) => CameraError::Io(match io.raw_os_error() {
            Some(code) => std::io::Error::from_raw_os_error(code),
            None => std::io::Error::new(io.kind(), io.to_string()),
        }),
        CameraError::ConnectTimeout => CameraError::ConnectTimeout,
        CameraError::Disconnected => CameraError::Disconnected,
        CameraError::Timeout => CameraError::Timeout,
        CameraError::ControlDenied => CameraError::ControlDenied,
        CameraError::ControlLost => CameraError::ControlLost,
        CameraError::Nak { command, status } => CameraError::Nak {
            command: *command,
            status: *status,
        },
        CameraError::Protocol(m) => CameraError::Protocol(m.clone()),
        CameraError::Unsupported(what) => CameraError::Unsupported(what),
        CameraError::Spawn(m) => CameraError::Spawn(m.clone()),
        CameraError::InvalidOption { option, driver } => CameraError::InvalidOption {
            option: option.clone(),
            driver,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Wake;
    use std::time::Instant;

    struct ThreadWaker(std::thread::Thread);
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    fn block_on<F: Future>(fut: F) -> F::Output {
        let mut fut = Box::pin(fut);
        let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
        let mut cx = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return v;
            }
            std::thread::park();
        }
    }

    /// Awaiting a handle must wake when it is fulfilled *after* the first poll
    /// parks. Parking executor + watchdog so a lost wakeup fails slow instead of
    /// hanging forever.
    #[test]
    fn async_await_wakes_on_late_notify() {
        let handle = ResponseHandle::<()>::new();
        let fulfiller = handle.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            fulfiller.fail(CameraError::Timeout);
        });

        let waiter = std::thread::current();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(3));
            waiter.unpark();
        });

        let start = Instant::now();
        let result = block_on(handle);
        assert!(result.is_err());
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "async poll did not wake on notify (lost-wakeup regression)"
        );
    }

    #[test]
    fn every_clone_awaiting_wakes() {
        let handle = ResponseHandle::<u32>::new();
        let waiters: Vec<_> = (0..3)
            .map(|_| {
                let h = handle.clone();
                std::thread::spawn(move || block_on(h))
            })
            .collect();
        std::thread::sleep(Duration::from_millis(50));
        handle.fulfill(Ok(7));
        for w in waiters {
            assert_eq!(w.join().unwrap().unwrap(), 7);
        }
    }

    #[test]
    fn errors_read_through_a_shared_handle_keep_their_variant() {
        let read = |err: CameraError| {
            let handle = ResponseHandle::<()>::new();
            handle.fail(err);
            handle.wait().map_err(unwrap_arc).unwrap_err()
        };
        match read(CameraError::Io(std::io::Error::from_raw_os_error(13))) {
            CameraError::Io(e) => assert_eq!(e.raw_os_error(), Some(13)),
            other => panic!("{other:?}"),
        }
        match read(CameraError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ))) {
            CameraError::Io(e) => {
                assert_eq!(e.kind(), std::io::ErrorKind::ConnectionRefused);
                assert_eq!(e.to_string(), "refused");
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            read(CameraError::ControlLost),
            CameraError::ControlLost
        ));
        assert!(matches!(
            read(CameraError::InvalidOption {
                option: "RtPriority(50)".into(),
                driver: "gvsp",
            }),
            CameraError::InvalidOption { option, driver: "gvsp" } if option == "RtPriority(50)"
        ));
    }
}
