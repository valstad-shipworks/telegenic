//! The GVCP control worker: owns the control socket, serializes transactions
//! (one in flight at a time, as devices commonly require), matches
//! acknowledges by packet id, keeps control alive via heartbeat, and fans out
//! device-initiated events.

use std::collections::VecDeque;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use fast_talker::sockets::OpenError;
use flume::{Receiver, TryRecvError};
use mio::{Events, Interest, Poll, Token, Waker};

use crate::clock::{self, Instant};
use crate::error::CameraError;
use crate::gige::proto::bootstrap;
use crate::gige::proto::gvcp::{self, Ack};
use crate::gige::{GigeConfig, GvcpEvent, Shared};
use crate::handle::ResponseHandle;
use crate::rx_timestamp::{self, StampedSocket};
use crate::thread_util::{ExitGuard, ThreadHandle};
use crate::tuning::{self, SocketRole, ThreadRole, TuningReport};
use crate::wire::{self, ControlTelemetry};

pub(crate) const TOK_SOCKET: Token = Token(0);
pub(crate) const TOK_WAKER: Token = Token(1);

const RECV_BUF: usize = 0xffff;
/// Most bytes reserved up front for a memory read; longer reads grow as
/// their chunks arrive.
const READ_MEM_PREALLOC: usize = 1 << 20;
/// Longest a run of PENDING_ACKs may hold one transaction past its send,
/// so a device (or a corrupted ack) cannot wedge the channel and the
/// heartbeats queued behind it.
const PENDING_ACK_BUDGET: Duration = Duration::from_secs(120);

/// Messages from `GigECamera` clones to the worker.
pub(crate) enum ToWorker {
    ReadReg(u32, ResponseHandle<u32>),
    ReadRegs(Vec<u32>, ResponseHandle<Vec<u32>>),
    WriteRegs(Vec<(u32, u32)>, ResponseHandle<()>),
    ReadMem {
        addr: u32,
        len: u32,
        handle: ResponseHandle<Vec<u8>>,
    },
    WriteMem {
        addr: u32,
        data: Vec<u8>,
        handle: ResponseHandle<()>,
    },
    SubscribeEvents(flume::Sender<GvcpEvent>),
    Shutdown,
}

enum Op {
    ReadReg(ResponseHandle<u32>),
    ReadRegs {
        handle: ResponseHandle<Vec<u32>>,
        count: usize,
    },
    WriteRegs(ResponseHandle<()>),
    ReadMem {
        handle: ResponseHandle<Vec<u8>>,
        acc: Vec<u8>,
        want: usize,
        next_addr: u32,
    },
    WriteMem {
        handle: ResponseHandle<()>,
        data: Vec<u8>,
        offset: usize,
        base_addr: u32,
    },
    Heartbeat,
}

impl Op {
    fn fail(self, err: CameraError) {
        match self {
            Op::ReadReg(h) => h.fail(err),
            Op::ReadRegs { handle, .. } => handle.fail(err),
            Op::WriteRegs(h) => h.fail(err),
            Op::ReadMem { handle, .. } => handle.fail(err),
            Op::WriteMem { handle, .. } => handle.fail(err),
            Op::Heartbeat => {}
        }
    }

    fn expected_ack(&self) -> u16 {
        match self {
            Op::ReadReg(_) | Op::ReadRegs { .. } | Op::Heartbeat => gvcp::READ_REGISTER_ACK,
            Op::WriteRegs(_) => gvcp::WRITE_REGISTER_ACK,
            Op::ReadMem { .. } => gvcp::READ_MEMORY_ACK,
            Op::WriteMem { .. } => gvcp::WRITE_MEMORY_ACK,
        }
    }
}

struct Inflight {
    sent: Vec<u8>,
    id: u16,
    deadline: Instant,
    pending_limit: Instant,
    tries_left: u8,
    op: Op,
}

pub(crate) struct Runner {
    socket: StampedSocket,
    device_addr: SocketAddr,
    rx: Receiver<ToWorker>,
    shared: Arc<Shared>,
    thread: ThreadHandle,
    cfg: GigeConfig,

    queue: VecDeque<Op>,
    /// Pre-encoded datagram for each queued op, kept in lockstep with `queue`.
    queued_payloads: VecDeque<PendingSend>,
    inflight: Option<Inflight>,
    next_id: u16,
    event_txs: Vec<flume::Sender<GvcpEvent>>,
    heartbeat_period: Duration,
    heartbeat_due: Instant,
    control_lost: bool,
    telemetry: Option<ControlTelemetry>,
}

/// What to encode when an op reaches the head of the queue.
enum PendingSend {
    ReadRegs(Vec<u32>),
    WriteRegs(Vec<(u32, u32)>),
    ReadMemChunk,
    WriteMemChunk,
}

impl Runner {
    pub(crate) fn new(
        socket: StampedSocket,
        rx: Receiver<ToWorker>,
        shared: Arc<Shared>,
        thread: ThreadHandle,
        cfg: GigeConfig,
        telemetry: Option<ControlTelemetry>,
    ) -> Self {
        let heartbeat_period = heartbeat_period(&cfg);
        Self {
            socket,
            device_addr: cfg.addr,
            rx,
            shared,
            thread,
            cfg,
            queue: VecDeque::new(),
            queued_payloads: VecDeque::new(),
            inflight: None,
            next_id: 0,
            event_txs: Vec::new(),
            heartbeat_period,
            heartbeat_due: Instant::now() + heartbeat_period,
            control_lost: false,
            telemetry,
        }
    }

    /// Put a datagram on the control socket, reporting it to the sink once the
    /// kernel has taken it. Every outbound byte goes through here.
    fn send_to(&self, datagram: &[u8], dst: SocketAddr, retry: bool) -> std::io::Result<usize> {
        let sent = self.socket.send_to(datagram, dst)?.len;
        if let Some(sink) = &self.telemetry
            && let Some(tx) = wire::control_tx(datagram, retry)
        {
            sink.sent(&tx, clock::system_now());
        }
        Ok(sent)
    }

    pub(crate) fn run(mut self, mut poll: Poll) {
        if let Some(sink) = &self.telemetry {
            sink.warmup();
        }
        let mut events = Events::with_capacity(16);
        let mut buf = [0u8; RECV_BUF];
        while self.thread.should_live() && !self.control_lost {
            if let Err(e) = poll.poll(&mut events, self.poll_timeout()) {
                if e.kind() == ErrorKind::Interrupted {
                    continue;
                }
                tracing::error!("gvcp worker poll error: {e}");
                break;
            }
            for ev in events.iter() {
                if ev.token() == TOK_SOCKET {
                    self.drain_socket(&mut buf);
                }
            }
            if self.drain_commands() {
                break;
            }
            self.check_inflight_deadline();
            self.check_heartbeat();
            self.pump();
        }
        self.shutdown();
    }

    fn poll_timeout(&self) -> Option<Duration> {
        Some(Duration::from_millis(10))
    }

    fn drain_socket(&mut self, buf: &mut [u8]) {
        loop {
            match self.socket.recv_from(buf) {
                Ok(r) => {
                    // Events may come from a device source port other than
                    // 3956, so filter on IP only.
                    if r.from.ip() != self.device_addr.ip() {
                        continue;
                    }
                    if let Some(sink) = &self.telemetry
                        && let Some(rx) = wire::control_rx(&buf[..r.len])
                    {
                        sink.received(&rx, r.timestamp.time);
                    }
                    self.on_datagram(&buf[..r.len], r.from);
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                // A port unreachable an earlier send drew, reported on this
                // receive. The socket stays usable, and readiness only re-arms
                // once a receive would block (mio on IOCP, edge-triggered epoll).
                Err(ref e)
                    if matches!(
                        e.kind(),
                        ErrorKind::ConnectionReset | ErrorKind::ConnectionRefused
                    ) =>
                {
                    tracing::debug!("gvcp recv error: {e}");
                    continue;
                }
                Err(e) => {
                    tracing::warn!("gvcp recv error: {e}");
                    return;
                }
            }
        }
    }

    fn on_datagram(&mut self, datagram: &[u8], src: SocketAddr) {
        if gvcp::is_cmd(datagram) {
            self.on_event(datagram, src);
            return;
        }
        let Some(ack) = Ack::parse(datagram) else {
            tracing::trace!("malformed ack ({} bytes)", datagram.len());
            return;
        };
        let Some(inflight) = &mut self.inflight else {
            self.shared.stats.lock().unsolicited += 1;
            return;
        };
        if ack.ack_id != inflight.id {
            self.shared.stats.lock().unsolicited += 1;
            return;
        }
        if let Some(ms) = ack.pending_ack_timeout_ms() {
            let extended = Instant::now() + Duration::from_millis(u64::from(ms));
            inflight.deadline = extended.min(inflight.pending_limit.max(inflight.deadline));
            self.shared.stats.lock().pending_acks += 1;
            return;
        }
        let Some(inflight) = self.inflight.take() else {
            return;
        };
        self.on_ack(inflight, ack);
    }

    fn on_ack(&mut self, inflight: Inflight, ack: Ack<'_>) {
        self.shared.stats.lock().acks += 1;
        if ack.status.is_error() {
            self.shared.stats.lock().naks += 1;
            if matches!(inflight.op, Op::Heartbeat) {
                tracing::warn!("heartbeat rejected with {}, control lost", ack.status);
                self.control_lost = true;
                return;
            }
            let command = inflight.op.expected_ack().wrapping_sub(1);
            inflight.op.fail(CameraError::Nak {
                command,
                status: ack.status,
            });
            return;
        }
        let expected_ack = inflight.op.expected_ack();
        if ack.answer != expected_ack {
            inflight.op.fail(CameraError::Protocol(format!(
                "expected ack {expected_ack:#06x}, got {:#06x}",
                ack.answer
            )));
            return;
        }
        match inflight.op {
            Op::ReadReg(handle) => match ack.register_values().next() {
                Some(v) => handle.fulfill(Ok(v)),
                None => handle.fail(CameraError::Protocol("empty read register ack".into())),
            },
            Op::ReadRegs { handle, count } => {
                let values: Vec<u32> = ack.register_values().collect();
                if values.len() == count {
                    handle.fulfill(Ok(values));
                } else {
                    handle.fail(CameraError::Protocol(format!(
                        "read register ack carried {} values, expected {count}",
                        values.len()
                    )));
                }
            }
            Op::WriteRegs(handle) => handle.fulfill(Ok(())),
            Op::ReadMem {
                handle,
                mut acc,
                want,
                next_addr,
            } => {
                let Some(data) = ack.payload.get(4..) else {
                    handle.fail(CameraError::Protocol("short read memory ack".into()));
                    return;
                };
                let take = data.len().min(want - acc.len());
                acc.extend_from_slice(&data[..take]);
                if acc.len() >= want {
                    handle.fulfill(Ok(acc));
                } else if data.is_empty() {
                    handle.fail(CameraError::Protocol("empty read memory ack".into()));
                } else {
                    let next_addr = next_addr.wrapping_add(take as u32);
                    let op = Op::ReadMem {
                        handle,
                        acc,
                        want,
                        next_addr,
                    };
                    self.continue_op(op, PendingSend::ReadMemChunk);
                }
            }
            Op::WriteMem {
                handle,
                data,
                mut offset,
                base_addr,
            } => {
                offset += chunk_len(data.len() - offset);
                if offset >= data.len() {
                    handle.fulfill(Ok(()));
                } else {
                    let op = Op::WriteMem {
                        handle,
                        data,
                        offset,
                        base_addr,
                    };
                    self.continue_op(op, PendingSend::WriteMemChunk);
                }
            }
            Op::Heartbeat => {
                self.shared.stats.lock().heartbeats += 1;
                if ack
                    .register_values()
                    .next()
                    .is_none_or(|ccp| ccp & bootstrap::CCP_CONTROL == 0)
                {
                    tracing::warn!("device control was lost (CCP cleared)");
                    self.control_lost = true;
                }
            }
        }
    }

    fn on_event(&mut self, datagram: &[u8], src: SocketAddr) {
        let Some(cmd) = gvcp::Cmd::parse(datagram) else {
            tracing::trace!("malformed inbound command");
            return;
        };
        if cmd.command != gvcp::EVENT_CMD && cmd.command != gvcp::EVENTDATA_CMD {
            tracing::trace!("unexpected inbound command {:#06x}", cmd.command);
            return;
        }
        if cmd.flags & gvcp::FLAG_ACK_REQUIRED != 0 {
            // Acknowledge to the message channel's source socket, not the
            // device's GVCP port.
            let ack = gvcp::encode_event_ack(cmd.command, cmd.req_id);
            if let Err(e) = self.send_to(&ack, src, false) {
                tracing::warn!("event ack send failed: {e}");
            }
        }
        self.shared.stats.lock().events += 1;
        let event = GvcpEvent::parse(cmd.command, cmd.payload);
        self.event_txs
            .retain(|tx| match tx.try_send(event.clone()) {
                Ok(()) => true,
                Err(flume::TrySendError::Full(_)) => {
                    tracing::trace!("event channel full, event dropped");
                    true
                }
                Err(flume::TrySendError::Disconnected(_)) => false,
            });
    }

    fn drain_commands(&mut self) -> bool {
        loop {
            match self.rx.try_recv() {
                Ok(ToWorker::ReadReg(addr, handle)) => {
                    self.enqueue(Op::ReadReg(handle), PendingSend::ReadRegs(vec![addr]));
                }
                Ok(ToWorker::ReadRegs(addrs, handle)) => {
                    let count = addrs.len();
                    self.enqueue(Op::ReadRegs { handle, count }, PendingSend::ReadRegs(addrs));
                }
                Ok(ToWorker::WriteRegs(pairs, handle)) => {
                    self.enqueue(Op::WriteRegs(handle), PendingSend::WriteRegs(pairs));
                }
                Ok(ToWorker::ReadMem { addr, len, handle }) => {
                    let op = Op::ReadMem {
                        handle,
                        acc: Vec::with_capacity((len as usize).min(READ_MEM_PREALLOC)),
                        want: len as usize,
                        next_addr: addr,
                    };
                    self.enqueue(op, PendingSend::ReadMemChunk);
                }
                Ok(ToWorker::WriteMem { addr, data, handle }) => {
                    let op = Op::WriteMem {
                        handle,
                        data,
                        offset: 0,
                        base_addr: addr,
                    };
                    self.enqueue(op, PendingSend::WriteMemChunk);
                }
                Ok(ToWorker::SubscribeEvents(tx)) => self.event_txs.push(tx),
                Ok(ToWorker::Shutdown) => return true,
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => return false,
            }
        }
    }

    fn enqueue(&mut self, op: Op, send: PendingSend) {
        self.queue.push_back(op);
        self.queued_payloads.push_back(send);
    }

    /// Send the next chunk of a multi-transaction memory op, letting a due
    /// heartbeat cut in at the chunk boundary — a long transfer (e.g. the
    /// GenICam XML fetch) must not hold the device's heartbeat window shut.
    fn continue_op(&mut self, op: Op, send: PendingSend) {
        if matches!(self.queue.front(), Some(Op::Heartbeat)) {
            self.queue.insert(1, op);
            self.queued_payloads.insert(1, send);
        } else {
            self.send_op(op, send);
        }
    }

    /// Start the next queued op if nothing is in flight.
    fn pump(&mut self) {
        if self.inflight.is_some() {
            return;
        }
        let (Some(op), Some(send)) = (self.queue.pop_front(), self.queued_payloads.pop_front())
        else {
            return;
        };
        self.send_op(op, send);
    }

    fn send_op(&mut self, op: Op, send: PendingSend) {
        self.next_id = gvcp::next_id(self.next_id);
        let id = self.next_id;
        let datagram = match (&op, send) {
            (_, PendingSend::ReadRegs(addrs)) => gvcp::encode_read_reg(&addrs, id),
            (_, PendingSend::WriteRegs(pairs)) => gvcp::encode_write_reg(&pairs, id),
            (
                Op::ReadMem {
                    acc,
                    want,
                    next_addr,
                    ..
                },
                PendingSend::ReadMemChunk,
            ) => {
                let remaining = want - acc.len();
                let count = chunk_len(remaining.next_multiple_of(4));
                gvcp::encode_read_mem(*next_addr, count as u16, id).to_vec()
            }
            (
                Op::WriteMem {
                    data,
                    offset,
                    base_addr,
                    ..
                },
                PendingSend::WriteMemChunk,
            ) => {
                let take = chunk_len(data.len() - offset);
                gvcp::encode_write_mem(
                    base_addr.wrapping_add(*offset as u32),
                    &data[*offset..offset + take],
                    id,
                )
            }
            (_, PendingSend::ReadMemChunk | PendingSend::WriteMemChunk) => {
                op.fail(CameraError::Protocol("internal op/payload mismatch".into()));
                return;
            }
        };
        if let Err(e) = self.send_to(&datagram, self.device_addr, false) {
            op.fail(CameraError::Io(e));
            return;
        }
        self.shared.stats.lock().commands += 1;
        let now = Instant::now();
        self.inflight = Some(Inflight {
            sent: datagram,
            id,
            deadline: now + self.cfg.gvcp_timeout,
            pending_limit: now + PENDING_ACK_BUDGET,
            tries_left: self.cfg.retries,
            op,
        });
    }

    fn check_inflight_deadline(&mut self) {
        let Some(inflight) = &mut self.inflight else {
            return;
        };
        if Instant::now() < inflight.deadline {
            return;
        }
        if inflight.tries_left > 0 {
            inflight.tries_left -= 1;
            inflight.deadline = Instant::now() + self.cfg.gvcp_timeout;
            self.shared.stats.lock().retries += 1;
            self.shared.link.retransmit();
            tracing::trace!(
                id = inflight.id,
                tries_left = inflight.tries_left,
                "ack overdue, retrying transaction"
            );
            if let Some(inflight) = &self.inflight
                && let Err(e) = self.send_to(&inflight.sent, self.device_addr, true)
            {
                tracing::warn!("retry send failed: {e}");
            }
            return;
        }
        self.shared.stats.lock().timeouts += 1;
        let Some(inflight) = self.inflight.take() else {
            return;
        };
        if matches!(inflight.op, Op::Heartbeat) {
            tracing::warn!("heartbeat timed out, considering control lost");
            self.control_lost = true;
            return;
        }
        inflight.op.fail(CameraError::Timeout);
    }

    fn check_heartbeat(&mut self) {
        if Instant::now() < self.heartbeat_due {
            return;
        }
        self.heartbeat_due = Instant::now() + self.heartbeat_period;
        let pending_heartbeat = matches!(
            self.inflight,
            Some(Inflight {
                op: Op::Heartbeat,
                ..
            })
        ) || self.queue.iter().any(|op| matches!(op, Op::Heartbeat));
        if !pending_heartbeat {
            // Jump the queue: heartbeats keep device control alive and must
            // not wait behind a backlog of user transactions.
            self.queue.push_front(Op::Heartbeat);
            self.queued_payloads.push_front(PendingSend::ReadRegs(vec![
                bootstrap::CONTROL_CHANNEL_PRIVILEGE,
            ]));
        }
    }

    fn shutdown(&mut self) {
        tracing::debug!(
            control_lost = self.control_lost,
            "gvcp worker shutting down"
        );
        let err = if self.control_lost {
            self.shared.set_control_lost();
            CameraError::ControlLost
        } else {
            CameraError::Disconnected
        };
        if let Some(inflight) = self.inflight.take() {
            inflight.op.fail(clone_err(&err));
        }
        for op in self.queue.drain(..) {
            op.fail(clone_err(&err));
        }
        self.queued_payloads.clear();
        if !self.control_lost {
            // Best-effort control release so the device is immediately
            // claimable by the next application.
            self.next_id = gvcp::next_id(self.next_id);
            let release =
                gvcp::encode_write_reg(&[(bootstrap::CONTROL_CHANNEL_PRIVILEGE, 0)], self.next_id);
            let _ = self.send_to(&release, self.device_addr, false);
        }
        self.event_txs.clear();
        self.thread.has_died();
        // Requests can sit behind the Shutdown message or land while this
        // teardown runs; fail them so no ResponseHandle is left pending
        // forever. `ControlPort::send` re-checks liveness after sending (and
        // has_died is already set above), so anything that slips past this
        // drain is failed by the sender instead — fulfilment is idempotent.
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                ToWorker::ReadReg(_, h) => h.fail(clone_err(&err)),
                ToWorker::ReadRegs(_, h) => h.fail(clone_err(&err)),
                ToWorker::WriteRegs(_, h) => h.fail(clone_err(&err)),
                ToWorker::ReadMem { handle, .. } => handle.fail(clone_err(&err)),
                ToWorker::WriteMem { handle, .. } => handle.fail(clone_err(&err)),
                ToWorker::SubscribeEvents(_) | ToWorker::Shutdown => {}
            }
        }
    }
}

fn clone_err(e: &CameraError) -> CameraError {
    match e {
        CameraError::ControlLost => CameraError::ControlLost,
        _ => CameraError::Disconnected,
    }
}

fn chunk_len(remaining: usize) -> usize {
    remaining.min(gvcp::DATA_SIZE_MAX)
}

fn heartbeat_period(cfg: &GigeConfig) -> Duration {
    Duration::from_millis(u64::from(cfg.heartbeat_timeout_ms / 3).max(10))
        .min(Duration::from_secs(1))
}

/// Bind the control socket and launch the worker thread. Returns the owner
/// [`ThreadHandle`], the socket's local address, and what the thread and
/// socket options came to.
pub(crate) fn spawn(
    rx: Receiver<ToWorker>,
    shared: Arc<Shared>,
    cfg: GigeConfig,
    telemetry: Option<ControlTelemetry>,
) -> Result<(ThreadHandle, SocketAddr, TuningReport), CameraError> {
    tuning::check_thread(ThreadRole::Control, &cfg.thread)?;
    tuning::check_socket(SocketRole::UdpControl, &cfg.control_socket)?;
    let bind_addr = cfg
        .local_addr
        .unwrap_or_else(|| SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), 0));
    let (socket, socket_report) =
        tuning::bind_udp(SocketRole::UdpControl, bind_addr, &cfg.control_socket).map_err(|e| {
            match e {
                OpenError::Io(e) => {
                    CameraError::Spawn(format!("bind control socket {bind_addr}: {e}"))
                }
                e => CameraError::Io(e.into()),
            }
        })?;
    let local_addr = socket
        .local_addr()
        .map_err(|e| CameraError::Spawn(e.to_string()))?;
    let mut socket =
        rx_timestamp::stamped(socket, "gvcp").map_err(|e| CameraError::Spawn(e.to_string()))?;

    let poll = Poll::new().map_err(|e| CameraError::Spawn(e.to_string()))?;
    poll.registry()
        .register(&mut socket, TOK_SOCKET, Interest::READABLE)
        .map_err(|e| CameraError::Spawn(e.to_string()))?;
    let waker = Arc::new(
        Waker::new(poll.registry(), TOK_WAKER).map_err(|e| CameraError::Spawn(e.to_string()))?,
    );

    let mut thread = ThreadHandle::new();
    thread.set_waker(waker);
    let thread_for_worker = thread.to_pass_in();

    let (started_tx, started_rx) = flume::bounded(1);
    let join = std::thread::Builder::new()
        .name("telegenic-gvcp".into())
        .spawn(move || {
            let _exit = ExitGuard(thread_for_worker.to_pass_in());
            let _tuning = match tuning::apply_thread(ThreadRole::Control, &cfg.thread) {
                Ok(report) => {
                    let _ = started_tx.send(Ok(report.summary()));
                    report
                }
                Err(e) => {
                    let _ = started_tx.send(Err(e));
                    return;
                }
            };
            let runner = Runner::new(socket, rx, shared, thread_for_worker, cfg, telemetry);
            runner.run(poll);
        })
        .map_err(|e| CameraError::Spawn(e.to_string()))?;
    thread.set_handle(join);
    let thread_report = started_rx
        .recv()
        .map_err(|_| CameraError::Spawn("gvcp worker exited during startup".into()))??;

    Ok((
        thread,
        local_addr,
        TuningReport {
            thread: thread_report,
            socket: socket_report.summary(),
        },
    ))
}

#[cfg(test)]
mod tests {
    //! Acknowledge matching, request-id wraps, corrupted acknowledges and
    //! chunked memory reads, driven datagram by datagram against the worker
    //! with no device behind it.

    use super::*;
    use crate::gige::GvcpEvent;
    use crate::link::LinkCounters;
    use proptest::prelude::*;
    use proptest::test_runner::{Config, RngSeed};

    fn config(cases: u32) -> Config {
        Config {
            cases,
            rng_seed: RngSeed::Fixed(0x7e1e_9e41_c0de),
            failure_persistence: None,
            ..Config::default()
        }
    }

    struct Rig {
        runner: Runner,
        device: SocketAddr,
        _device_socket: std::net::UdpSocket,
        _commands: flume::Sender<ToWorker>,
    }

    impl Rig {
        fn new() -> Self {
            let device_socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let device = device_socket.local_addr().unwrap();
            let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let socket = rx_timestamp::stamped(socket, "gvcp").unwrap();
            let mut cfg = GigeConfig::new(device.ip());
            cfg.addr = device;
            let (commands, rx) = flume::unbounded();
            let shared = Arc::new(Shared::new(Arc::new(LinkCounters::default())));
            Self {
                runner: Runner::new(socket, rx, shared, ThreadHandle::new(), cfg, None),
                device,
                _device_socket: device_socket,
                _commands: commands,
            }
        }

        fn read_register(&mut self, addr: u32) -> (ResponseHandle<u32>, u16) {
            let handle = ResponseHandle::new();
            self.runner.enqueue(
                Op::ReadReg(handle.clone()),
                PendingSend::ReadRegs(vec![addr]),
            );
            self.runner.pump();
            (handle, self.inflight_id())
        }

        fn inflight_id(&self) -> u16 {
            self.runner
                .inflight
                .as_ref()
                .expect("a transaction in flight")
                .id
        }

        fn deliver(&mut self, datagram: &[u8]) {
            self.runner.on_datagram(datagram, self.device);
        }

        fn unsolicited(&self) -> u64 {
            self.runner.shared.stats.lock().unsolicited
        }
    }

    fn ack(status: u16, answer: u16, id: u16, payload: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&status.to_be_bytes());
        b.extend_from_slice(&answer.to_be_bytes());
        b.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        b.extend_from_slice(&id.to_be_bytes());
        b.extend_from_slice(payload);
        b
    }

    fn previous_id(id: u16) -> u16 {
        if id <= 1 { 0xfffe } else { id - 1 }
    }

    fn value_of(handle: &ResponseHandle<u32>) -> Option<Result<u32, String>> {
        handle
            .is_set()
            .then(|| handle.get().map_err(|e| e.to_string()))
    }

    proptest! {
        #![proptest_config(config(64))]

        /// From any request id — including just before the wrap — every
        /// transaction is answered by the acknowledge carrying its own id:
        /// acknowledges for the previous or the next id (a late duplicate, a
        /// stray) are counted as unsolicited and change nothing, across as
        /// many wraps as the run takes.
        #[test]
        fn acks_match_their_transaction_across_request_id_wraps(
            start in prop_oneof![0u16..16, 0xffe0u16..=0xffff, any::<u16>()],
            values in prop::collection::vec(any::<u32>(), 1..300),
        ) {
            let mut rig = Rig::new();
            rig.runner.next_id = start;
            let mut stray = 0;
            for (k, &value) in values.iter().enumerate() {
                let (handle, id) = rig.read_register(0x0a00);
                prop_assert!(id != 0 && id != gvcp::DISCOVERY_ID, "id {}", id);
                let want = ((u32::from(start.min(0xfffe)) + k as u32) % 0xfffe) as u16 + 1;
                prop_assert_eq!(id, want);
                for other in [previous_id(id), gvcp::next_id(id)] {
                    rig.deliver(&ack(0, gvcp::READ_REGISTER_ACK, other, &(!value).to_be_bytes()));
                    stray += 1;
                }
                prop_assert!(!handle.is_set());
                rig.deliver(&ack(0, gvcp::READ_REGISTER_ACK, id, &value.to_be_bytes()));
                prop_assert_eq!(value_of(&handle), Some(Ok(value)));
                prop_assert_eq!(rig.unsolicited(), stray);
            }
        }

        /// One flipped header bit in the acknowledge of a register read
        /// never completes it with a value other than the register's: the
        /// datagram is ignored (the transaction keeps waiting), fails the
        /// transaction, or completes it with the right value. GVCP carries
        /// no checksum of its own, so flipped payload bits are the UDP
        /// checksum's to catch and are not exercised here.
        #[test]
        fn a_corrupted_ack_header_never_yields_a_wrong_value(value in any::<u32>(), bit in 0usize..64) {
            let mut rig = Rig::new();
            let (handle, id) = rig.read_register(0x0a00);
            let mut datagram = ack(0, gvcp::READ_REGISTER_ACK, id, &value.to_be_bytes());
            datagram[bit / 8] ^= 1 << (bit % 8);
            rig.deliver(&datagram);
            match value_of(&handle) {
                None => prop_assert!(rig.runner.inflight.is_some()),
                Some(Ok(v)) => prop_assert_eq!(v, value, "flipped bit {}", bit),
                Some(Err(_)) => prop_assert!(rig.runner.inflight.is_none()),
            }
        }

        /// A PENDING_ACK — or a corrupted acknowledge that reads as one (a
        /// single flipped bit turns READ_REGISTER_ACK 0x0081 into
        /// PENDING_ACK 0x0089, its register value then read as the
        /// timeout) — never pushes the deadline past the protocol's 16-bit
        /// millisecond limit.
        #[test]
        fn no_ack_extends_a_deadline_past_the_pending_ack_limit(value in any::<u32>(), bit in 0usize..64) {
            let mut rig = Rig::new();
            let (_handle, id) = rig.read_register(0x0a00);
            let mut datagram = ack(0, gvcp::READ_REGISTER_ACK, id, &value.to_be_bytes());
            datagram[bit / 8] ^= 1 << (bit % 8);
            let before = Instant::now();
            rig.deliver(&datagram);
            if let Some(inflight) = &rig.runner.inflight {
                let limit = before + Duration::from_millis(u64::from(u16::MAX)) + Duration::from_secs(1);
                prop_assert!(inflight.deadline <= limit, "deadline pushed {:?} out", inflight.deadline - before);
            }
        }

        /// A memory read longer than one transaction goes out in chunks at
        /// consecutive addresses; a device that answers each chunk with
        /// fewer bytes than asked has the next chunk start where its data
        /// ended, and the result is exactly the memory read. A late copy of
        /// an earlier chunk's acknowledge changes nothing.
        #[test]
        fn chunked_memory_reads_reassemble_exactly(
            base in 0u32..0x1000,
            want in 1usize..3000,
            shorts in prop::collection::vec(1usize..=gvcp::DATA_SIZE_MAX, 1..40),
            seed in any::<u8>(),
        ) {
            let base = base * 4;
            let memory: Vec<u8> = (0..0x8000u32).map(|i| (i as u8).wrapping_mul(31) ^ seed).collect();
            let mut rig = Rig::new();
            let handle = ResponseHandle::new();
            let op = Op::ReadMem {
                handle: handle.clone(),
                acc: Vec::new(),
                want,
                next_addr: base,
            };
            rig.runner.enqueue(op, PendingSend::ReadMemChunk);
            rig.runner.pump();
            let mut expected_addr = base as usize;
            let mut previous: Option<Vec<u8>> = None;
            for k in 0.. {
                if handle.is_set() {
                    break;
                }
                prop_assert!(k < 10_000, "the read never finished");
                let inflight = rig.runner.inflight.as_ref().expect("a chunk in flight");
                let cmd = gvcp::Cmd::parse(&inflight.sent).unwrap();
                let addr = u32::from_be_bytes(cmd.payload[..4].try_into().unwrap()) as usize;
                let count = u32::from_be_bytes(cmd.payload[4..8].try_into().unwrap()) as usize;
                prop_assert_eq!(addr, expected_addr);
                let remaining = base as usize + want - addr;
                prop_assert_eq!(count, remaining.next_multiple_of(4).min(gvcp::DATA_SIZE_MAX));
                let n = shorts[k % shorts.len()].min(count);
                let mut payload = (addr as u32).to_be_bytes().to_vec();
                payload.extend_from_slice(&memory[addr..addr + n]);
                let id = inflight.id;
                if let Some(late) = &previous {
                    rig.deliver(late);
                }
                let datagram = ack(0, gvcp::READ_MEMORY_ACK, id, &payload);
                rig.deliver(&datagram);
                previous = Some(datagram);
                expected_addr += n;
            }
            let got = handle.get().map_err(|e| e.to_string());
            prop_assert_eq!(got, Ok(memory[base as usize..base as usize + want].to_vec()));
        }

        /// Device events decode from any payload without panicking, each
        /// field from its offset, and only events that ask for one get an
        /// acknowledge; other commands on the control socket are dropped.
        #[test]
        fn device_commands_decode_and_only_requested_events_are_acknowledged(
            command in prop_oneof![
                Just(gvcp::EVENT_CMD),
                Just(gvcp::EVENTDATA_CMD),
                any::<u16>(),
            ],
            flags in any::<u8>(),
            id in any::<u16>(),
            payload in prop::collection::vec(any::<u8>(), 0..64),
        ) {
            let event = GvcpEvent::parse(command, &payload);
            let at = |i: usize, n: usize| payload.get(i..i + n).map(|b| b.iter().fold(0u64, |a, &x| a << 8 | u64::from(x)));
            prop_assert_eq!(u64::from(event.event_id), at(2, 2).unwrap_or(0));
            prop_assert_eq!(u64::from(event.stream_channel), at(4, 2).unwrap_or(0));
            prop_assert_eq!(u64::from(event.block_id), at(6, 2).unwrap_or(0));
            prop_assert_eq!(event.timestamp, at(8, 8).unwrap_or(0));
            prop_assert_eq!(&event.data[..], payload.get(16..).unwrap_or_default());
            prop_assert_eq!(&event.raw, &payload);

            let is_event = command == gvcp::EVENT_CMD || command == gvcp::EVENTDATA_CMD;
            let ack_expected = is_event && flags & gvcp::FLAG_ACK_REQUIRED != 0;
            let source = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            // Loopback delivery is queued, not immediate: give an expected
            // ack time to land under a loaded test run.
            let wait = if ack_expected { 1000 } else { 5 };
            source.set_read_timeout(Some(Duration::from_millis(wait))).unwrap();
            let mut rig = Rig::new();
            let (tx, events) = flume::unbounded();
            rig.runner.event_txs.push(tx);
            let mut datagram = vec![gvcp::PACKET_TYPE_CMD, flags];
            datagram.extend_from_slice(&command.to_be_bytes());
            datagram.extend_from_slice(&(payload.len() as u16).to_be_bytes());
            datagram.extend_from_slice(&id.to_be_bytes());
            datagram.extend_from_slice(&payload);
            rig.runner.on_datagram(&datagram, source.local_addr().unwrap());
            prop_assert_eq!(events.try_iter().count(), usize::from(is_event));
            let mut buf = [0u8; 64];
            let acked = source.recv_from(&mut buf).ok().map(|(n, _)| {
                let a = gvcp::Ack::parse(&buf[..n]).unwrap();
                (a.answer, a.ack_id)
            });
            if ack_expected {
                prop_assert_eq!(acked, Some((command + 1, id)));
            } else {
                prop_assert_eq!(acked, None);
            }
        }
    }
}
