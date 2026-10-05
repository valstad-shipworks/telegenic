//! Receive timestamps for the control and stream sockets, so a datagram's
//! arrival time is taken when the kernel saw the bytes rather than when the
//! worker got around to reading them. At GVSP line rate the two can be
//! milliseconds apart.

use std::io;

use fast_talker::{Config, Timestamped};

/// A worker socket whose receives carry their arrival time: the kernel's
/// stamp where the platform has one, the user-space receive time otherwise.
pub(crate) type StampedSocket = Timestamped<mio::net::UdpSocket>;

/// Makes `socket` non-blocking for the worker's poll loop and turns on
/// kernel receive stamps, leaving the NIC's own timestamping setting alone.
pub(crate) fn stamped(socket: std::net::UdpSocket, which: &str) -> io::Result<StampedSocket> {
    socket.set_nonblocking(true)?;
    let socket =
        Timestamped::with_config(mio::net::UdpSocket::from_std(socket), Config::kernel_only());
    tracing::debug!(socket = which, source = ?socket.source(), "receive timestamps");
    Ok(socket)
}
