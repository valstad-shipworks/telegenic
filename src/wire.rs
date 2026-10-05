//! Packet-level observation of a camera's GigE Vision traffic.
//!
//! A [`TelemetrySink`] installed on a [`GigECamera`](crate::gige::GigECamera)
//! before it connects sees every GVCP datagram on the control channel; one
//! installed on a stream channel sees every GVSP datagram, plus the resend
//! requests the receiver sends back. Each hook carries the time the packet
//! crossed the wire — from the kernel when the socket supports
//! `SO_TIMESTAMPING`, so a receive time is taken when the bytes landed rather
//! than when the worker got around to reading them.
//!
//! The packet types own their payloads, so a sink is enough to record a
//! session and replay it later. That copy is the reason nothing is built
//! unless a sink is actually installed — with no sink the hot GVSP path pays
//! one `Option` test per datagram and nothing else.

use std::sync::Arc;
use std::time::SystemTime;

use crate::gige::proto::gvcp::{self, GvcpStatus};
use crate::gige::proto::gvsp::{self, ContentType};

/// Observer for packets crossing one of a camera's sockets. A sink is handed
/// to a channel before it opens and is shared with that channel's I/O thread;
/// every hook fires on that thread, so implementations should be cheap and
/// non-blocking.
pub trait TelemetrySink<TX: Send + Sized, RX: Send + Sized>: Send + Sync + 'static {
    /// Called from the I/O thread that will invoke the hooks, before its event
    /// loop starts, allowing thread-affine setup (allocations, thread-local
    /// state, pinning).
    fn warmup(&self) {}
    /// Called when the last byte of `tx` has been written to the socket.
    fn sent(&self, tx: &TX, timestamp: SystemTime);
    /// Called when `rx` has been decoded off the socket. `timestamp` is the
    /// kernel receive timestamp when available, otherwise the decode time.
    fn received(&self, rx: &RX, timestamp: SystemTime);
}

impl<TX: Send, RX: Send> std::fmt::Debug for dyn TelemetrySink<TX, RX> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("dyn TelemetrySink")
    }
}

/// Shared sink observing one camera's GVCP control channel.
pub type ControlTelemetry = Arc<dyn TelemetrySink<ControlTx, ControlRx>>;

/// Shared sink observing one GVSP stream channel.
pub type StreamTelemetry = Arc<dyn TelemetrySink<GvcpCmd, GvspPacket>>;

/// A GVCP command packet, owned. Commands travel in both directions: the
/// driver sends register and memory transactions, the device sends events.
#[derive(Debug, Clone)]
pub struct GvcpCmd {
    pub flags: u8,
    pub command: u16,
    pub req_id: u16,
    pub payload: Box<[u8]>,
}

impl GvcpCmd {
    fn parse(datagram: &[u8]) -> Option<Self> {
        let cmd = gvcp::Cmd::parse(datagram)?;
        Some(Self {
            flags: cmd.flags,
            command: cmd.command,
            req_id: cmd.req_id,
            payload: Box::from(cmd.payload),
        })
    }
}

/// A GVCP acknowledge packet, owned.
#[derive(Debug, Clone)]
pub struct GvcpAck {
    pub status: GvcpStatus,
    pub answer: u16,
    pub ack_id: u16,
    pub payload: Box<[u8]>,
}

impl GvcpAck {
    fn parse(datagram: &[u8]) -> Option<Self> {
        let ack = gvcp::Ack::parse(datagram)?;
        Some(Self {
            status: ack.status,
            answer: ack.answer,
            ack_id: ack.ack_id,
            payload: Box::from(ack.payload),
        })
    }
}

/// A datagram the driver put on the control socket.
#[derive(Debug, Clone)]
pub enum ControlTx {
    /// A register or memory transaction, a heartbeat, or the control release
    /// sent at shutdown. `retry` marks a retransmission of a transaction whose
    /// acknowledge went overdue — same bytes, same request id, as the device
    /// sees them.
    Cmd { cmd: GvcpCmd, retry: bool },
    /// Our acknowledge of a device event that asked for one.
    Ack(GvcpAck),
}

/// A datagram the driver took off the control socket.
#[derive(Debug, Clone)]
pub enum ControlRx {
    /// A device acknowledge, including `PENDING_ACK` deadline extensions and
    /// acknowledges arriving with no matching transaction in flight.
    Ack(GvcpAck),
    /// A device-initiated command — an event or event-data message.
    Cmd(GvcpCmd),
}

/// One GVSP datagram, owned. `data` is the packet body after the GVSP header:
/// the leader for [`ContentType::Leader`], the trailer for
/// [`ContentType::Trailer`], and a slice of the image for
/// [`ContentType::Payload`].
#[derive(Debug, Clone)]
pub struct GvspPacket {
    pub status: GvcpStatus,
    pub extended_ids: bool,
    pub frame_id: u64,
    pub packet_id: u32,
    pub content_type: ContentType,
    pub data: Box<[u8]>,
}

/// Decodes a datagram the driver is about to send or has just received into
/// the packet type its sink expects. Returns `None` for a datagram that does
/// not parse, which the caller drops anyway.
pub(crate) fn control_tx(datagram: &[u8], retry: bool) -> Option<ControlTx> {
    if gvcp::is_cmd(datagram) {
        GvcpCmd::parse(datagram).map(|cmd| ControlTx::Cmd { cmd, retry })
    } else {
        GvcpAck::parse(datagram).map(ControlTx::Ack)
    }
}

pub(crate) fn control_rx(datagram: &[u8]) -> Option<ControlRx> {
    if gvcp::is_cmd(datagram) {
        GvcpCmd::parse(datagram).map(ControlRx::Cmd)
    } else {
        GvcpAck::parse(datagram).map(ControlRx::Ack)
    }
}

pub(crate) fn stream_tx(datagram: &[u8]) -> Option<GvcpCmd> {
    GvcpCmd::parse(datagram)
}

pub(crate) fn stream_rx(datagram: &[u8]) -> Option<GvspPacket> {
    let view = gvsp::GvspView::parse(datagram)?;
    Some(GvspPacket {
        status: view.status,
        extended_ids: view.extended_ids,
        frame_id: view.frame_id,
        packet_id: view.packet_id,
        content_type: view.content_type,
        data: Box::from(view.data),
    })
}
