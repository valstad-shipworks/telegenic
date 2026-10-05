//! The per-channel GVSP receiver worker: reassembles frames from packets,
//! requests resends for holes, and fans out completed frames.

use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hasher};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use fast_talker::options::ThreadOption;
use flume::{Receiver, TryRecvError};
use mio::{Events, Interest, Poll, Token, Waker};

use crate::clock::{self, Instant};
use crate::error::CameraError;
use crate::gige::proto::gvcp::{self, GvcpStatus};
use crate::gige::proto::gvsp::{self, ContentType, GvspView, ImageLeader};
use crate::gige::stream::frame::{BufSlot, Frame, FramePool, FrameStatus, PayloadKind, PooledBuf};
use crate::gige::stream::{StreamConfig, StreamShared, StreamStats};
use crate::rx_timestamp::{self, StampedSocket};
use crate::thread_util::{ExitGuard, ThreadHandle};
use crate::tuning::{self, OptionReport, ThreadRole};
use crate::wire::{self, StreamTelemetry};

pub(crate) const TOK_SOCKET: Token = Token(0);
pub(crate) const TOK_WAKER: Token = Token(1);

/// Frames arriving this many ids late are dropped instead of reopened.
const DISCARD_LATE_FRAME_THRESHOLD: i64 = 100;
/// The block id a device counter starts from, and returns to when the
/// device restarts it (stream channel reopened, or on acquisition start).
const RESTART_BLOCK_ID: u64 = 1;
/// Closed frames remembered for telling late copies from a restarted
/// counter; covers the whole late window.
const HISTORY_LEN: usize = 128;
const _: () = assert!(HISTORY_LEN as i64 >= DISCARD_LATE_FRAME_THRESHOLD);
/// Leading packets (leader, first payload packets) fingerprinted per frame.
const FINGERPRINTED_PACKETS: usize = 4;
/// Upper bound on packets per frame, whatever a trailer or payload size
/// claims: bounds the per-frame bookkeeping.
const MAX_FRAME_PACKETS: usize = 1 << 20;
/// Stream-side GVCP id counter seed, clear of the control channel's range.
const RESEND_ID_SEED: u16 = 65300;

pub(crate) enum ToStreamWorker {
    Subscribe(flume::Sender<Arc<Frame>>),
    Shutdown,
}

/// Hashes of a frame's leading packets and its trailer: enough to tell a
/// byte-identical late copy of a closed frame from a new frame that reuses
/// its block id.
#[derive(Debug, Clone, Copy, Default)]
struct Fingerprints {
    head: [Option<u64>; FINGERPRINTED_PACKETS],
    trailer: Option<(u32, u64)>,
}

impl Fingerprints {
    /// Records `view`'s fingerprint, returning whether `reference` holds a
    /// different one for the same packet.
    fn record(&mut self, view: &GvspView<'_>, reference: Option<&Fingerprints>) -> bool {
        let content = match view.content_type {
            ContentType::Leader => 1,
            ContentType::Trailer => 2,
            ContentType::Payload => 3,
            _ => return false,
        };
        let mut hasher = DefaultHasher::new();
        hasher.write_u8(content);
        hasher.write(view.data);
        let fp = hasher.finish();
        if view.content_type == ContentType::Trailer {
            let entry = Some((view.packet_id, fp));
            self.trailer = entry;
            return reference.is_some_and(|r| r.trailer.is_some() && r.trailer != entry);
        }
        let Some(slot) = self.head.get_mut(view.packet_id as usize) else {
            return false;
        };
        *slot = Some(fp);
        reference
            .and_then(|r| r.head[view.packet_id as usize])
            .is_some_and(|known| known != fp)
    }
}

/// A frame the receiver closed (or dropped unopened), kept for judging
/// packets that arrive with its block id afterwards.
struct ClosedBlock {
    frame_id: u64,
    extended_ids: bool,
    /// `None` for frames dropped without reassembly, whose stragglers are
    /// always late.
    fingerprints: Option<Fingerprints>,
}

struct FrameInFlight {
    frame_id: u64,
    extended_ids: bool,
    slot: BufSlot,
    n_packets: usize,
    /// Data bytes per payload packet. Starts as the SCPS-derived estimate;
    /// replaced by the observed size of payload packet 1 — devices may cap
    /// or align their block below the theoretical `scps - overhead`. Until
    /// then (`block_known` false) payload packets are held, not placed.
    block_size: usize,
    block_known: bool,
    held: Vec<(usize, Vec<u8>)>,
    held_bytes: usize,
    trailer_seen: bool,
    /// Highest packet id such that 0..=it are all received; -1 initially.
    last_valid_packet: i64,
    received_size: usize,
    /// Highest `offset + len` written so far; the frame's data extent.
    data_end: usize,
    last_packet_time: Instant,
    leader: Option<ImageLeader>,
    system_timestamp_ns: u64,
    /// Set on protocol violations; blocks further data writes and decides
    /// the closing status.
    error: Option<FrameStatus>,
    resend_disabled: bool,
    resend_ratio_reached: bool,
    n_resend_requests: usize,
    fingerprints: Fingerprints,
    /// Set while this frame reuses the block id of a recently closed one
    /// and has so far matched it byte for byte: it is then taken for late
    /// copies and dropped, unless a packet differs, which shows the device
    /// counter restarted.
    replay_of: Option<Fingerprints>,
    /// Packets routed here while `replay_of` is set, counted as ignored
    /// until the frame proves to be a restart.
    absorbed: u64,
}

impl FrameInFlight {
    fn set_n_packets(&mut self, n: usize) {
        self.n_packets = n;
        self.slot.packets.resize(n, Default::default());
        self.last_valid_packet = self.last_valid_packet.min(n as i64 - 1);
    }

    /// Writes one payload packet's data at its block's offset. The block
    /// size must be known.
    fn place(&mut self, packet_id: usize, data: &[u8], stats: &mut StreamStats) {
        if data.len() > self.block_size {
            stats.size_mismatch_errors += 1;
            self.error = Some(FrameStatus::WrongPacketId);
            return;
        }
        let Some(offset) = (packet_id - 1).checked_mul(self.block_size) else {
            self.error = Some(FrameStatus::WrongPacketId);
            return;
        };
        let mut data = data;
        let capacity = self.slot.data.len();
        if offset.saturating_add(data.len()) > capacity {
            // The final payload packet may legally be padded past the
            // payload size; anything else is a real mismatch.
            if packet_id != self.n_packets - 2 {
                stats.size_mismatch_errors += 1;
            }
            if offset >= capacity {
                return;
            }
            data = &data[..capacity - offset];
        }
        self.slot.data[offset..offset + data.len()].copy_from_slice(data);
        self.received_size += data.len();
        self.data_end = self.data_end.max(offset + data.len());
    }
}

/// How a frame's block id relates to the receiver's position in the
/// device's id sequence.
enum Arrival {
    New,
    Late,
    MaybeReplay(Fingerprints),
}

/// Signed distance from block id `last` to `id`, positive ahead. Device
/// counters skip 0 when they wrap (16-bit ids 0xffff -> 1, extended ones
/// u64::MAX -> 1), so ids are compared on the ring 1..=max; an id of 0,
/// which the counter never produces, falls back to plain wrapping.
fn block_id_distance(id: u64, last: u64, extended_ids: bool) -> i64 {
    let max = if extended_ids {
        u64::MAX
    } else {
        u64::from(u16::MAX)
    };
    let (id, last) = (id & max, last & max);
    let modulus = u128::from(max) + u128::from(id == 0 || last == 0);
    let ahead = (u128::from(id) + modulus - u128::from(last)) % modulus;
    if ahead <= modulus / 2 {
        i64::try_from(ahead).unwrap_or(i64::MAX)
    } else {
        i64::try_from(modulus - ahead).map_or(i64::MIN, |behind| -behind)
    }
}

pub(crate) struct StreamRunner {
    socket: StampedSocket,
    device_gvcp_addr: SocketAddr,
    rx: Receiver<ToStreamWorker>,
    shared: Arc<StreamShared>,
    thread: ThreadHandle,
    cfg: StreamConfig,

    pool: FramePool,
    frames: Vec<FrameInFlight>,
    subscribers: Vec<flume::Sender<Arc<Frame>>>,
    shutdown_requested: bool,
    stats: StreamStats,
    scps_packet_size: usize,
    payload_size: usize,
    resend_enabled: bool,
    resend_id: u16,
    resend_buf: [u8; gvcp::RESEND_MAX_LEN],
    last_frame_id: u64,
    first_packet: bool,
    history: VecDeque<ClosedBlock>,
    tick_frequency: u64,
    telemetry: Option<StreamTelemetry>,
}

impl StreamRunner {
    pub(crate) fn run(mut self, mut poll: Poll) {
        if let Some(sink) = &self.telemetry {
            sink.warmup();
        }
        let mut events = Events::with_capacity(16);
        let mut buf = [0u8; 0xffff];
        loop {
            if !self.thread.should_live() {
                break;
            }
            if let Err(e) = poll.poll(&mut events, self.poll_timeout()) {
                if e.kind() == ErrorKind::Interrupted {
                    continue;
                }
                tracing::error!("gvsp worker poll error: {e}");
                break;
            }
            // Commands before packets: a Subscribe sent before a frame's
            // datagrams must be registered before that frame can complete,
            // or the frame fans out to nobody and is lost.
            self.drain_commands();
            if self.shutdown_requested {
                break;
            }
            for ev in events.iter() {
                if ev.token() == TOK_SOCKET {
                    self.drain_socket(&mut buf);
                }
            }
            self.check_frame_completion(Instant::now(), None);
            self.publish_stats();
        }
        self.flush_frames();
        self.publish_stats();
        self.subscribers.clear();
        tracing::debug!("gvsp worker shutting down");
        self.thread.has_died();
    }

    fn poll_timeout(&self) -> Option<Duration> {
        Some(if self.frames.is_empty() {
            Duration::from_millis(100)
        } else {
            self.cfg.packet_timeout
        })
    }

    fn publish_stats(&self) {
        *self.shared.stats.lock() = self.stats;
    }

    fn drain_commands(&mut self) {
        loop {
            match self.rx.try_recv() {
                Ok(ToStreamWorker::Subscribe(tx)) => self.subscribers.push(tx),
                Ok(ToStreamWorker::Shutdown) => self.shutdown_requested = true,
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => return,
            }
        }
    }

    fn drain_socket(&mut self, buf: &mut [u8]) {
        loop {
            match self.socket.recv_from(buf) {
                Ok(r) => {
                    if let Some(drops) = r.drops {
                        self.stats.socket_drops = u64::from(drops);
                    }
                    if r.from.ip() != self.device_gvcp_addr.ip() {
                        continue;
                    }
                    let datagram = &buf[..r.len];
                    self.stats.packets += 1;
                    self.stats.bytes += r.len as u64;
                    if let Some(sink) = &self.telemetry
                        && let Some(rx) = wire::stream_rx(datagram)
                    {
                        sink.received(&rx, r.timestamp.time);
                    }
                    self.process_packet(datagram, Instant::now());
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => {
                    tracing::warn!("gvsp recv error: {e}");
                    return;
                }
            }
        }
    }

    fn process_packet(&mut self, datagram: &[u8], now: Instant) {
        let Some(view) = GvspView::parse(datagram) else {
            self.stats.ignored_packets += 1;
            return;
        };
        if self.first_packet {
            self.last_frame_id = view.frame_id.wrapping_sub(1);
            self.first_packet = false;
        }
        let Some(index) = self.find_or_create_frame(&view, now) else {
            return;
        };
        let provisional = self.frames[index].replay_of.is_some();
        if provisional {
            self.frames[index].absorbed += 1;
            self.stats.ignored_packets += 1;
        }

        if view.status.is_error() {
            if matches!(
                view.status,
                GvcpStatus::PACKET_UNAVAILABLE
                    | GvcpStatus::PACKET_REMOVED_FROM_MEMORY
                    | GvcpStatus::PACKET_AND_PREV_REMOVED_FROM_MEMORY
            ) {
                self.frames[index].resend_disabled = true;
                self.stats.resend_disabled += 1;
            }
            self.stats.error_packets += 1;
            return;
        }

        let packet_id = view.packet_id as usize;
        let frame = &self.frames[index];
        if frame
            .slot
            .packets
            .get(packet_id)
            .is_some_and(|p| p.received)
        {
            if !provisional {
                self.stats.duplicated_packets += 1;
            }
            return;
        }

        match view.content_type {
            ContentType::Leader => self.process_leader(index, &view),
            ContentType::Payload => self.process_payload(index, &view),
            ContentType::Trailer => self.process_trailer(index, &view),
            _ => {
                if !provisional {
                    self.stats.ignored_packets += 1;
                }
                return;
            }
        }

        let frame = &mut self.frames[index];
        if packet_id < frame.n_packets {
            if frame.slot.packets[packet_id].resend_requested {
                self.stats.resent_packets += 1;
            }
            frame.slot.packets[packet_id].received = true;
        }
        let mut i = frame.last_valid_packet + 1;
        while (i as usize) < frame.n_packets && frame.slot.packets[i as usize].received {
            i += 1;
        }
        frame.last_valid_packet = i - 1;
        let diverged = frame.fingerprints.record(&view, frame.replay_of.as_ref());
        if provisional && diverged {
            self.adopt_restart(index);
        }

        self.missing_packet_check(index, view.packet_id, now);
        let frame_id = self.frames[index].frame_id;
        self.check_frame_completion(now, Some(frame_id));
    }

    /// A frame reusing a closed frame's block id carries different bytes:
    /// the device restarted its counter, so this is a real frame and the
    /// id sequence continues from it.
    fn adopt_restart(&mut self, index: usize) {
        let frame = &mut self.frames[index];
        tracing::debug!(
            frame_id = frame.frame_id,
            last = self.last_frame_id,
            "block id counter restarted"
        );
        frame.replay_of = None;
        frame.resend_disabled = false;
        self.stats.ignored_packets = self.stats.ignored_packets.saturating_sub(frame.absorbed);
        self.last_frame_id = frame.frame_id;
    }

    /// Classifies a packet whose block id is at or behind the last one
    /// opened (within the late window).
    fn classify_behind(&self, view: &GvspView<'_>) -> Arrival {
        if view.frame_id != RESTART_BLOCK_ID {
            return Arrival::Late;
        }
        let closed = self
            .history
            .iter()
            .rev()
            .find(|c| c.frame_id == view.frame_id && c.extended_ids == view.extended_ids);
        match closed {
            None => Arrival::New,
            Some(ClosedBlock {
                fingerprints: None, ..
            }) => Arrival::Late,
            Some(ClosedBlock {
                fingerprints: Some(fp),
                ..
            }) => Arrival::MaybeReplay(*fp),
        }
    }

    fn remember(&mut self, frame_id: u64, extended_ids: bool, fingerprints: Option<Fingerprints>) {
        if self.history.len() >= HISTORY_LEN {
            self.history.pop_front();
        }
        self.history.push_back(ClosedBlock {
            frame_id,
            extended_ids,
            fingerprints,
        });
    }

    fn find_or_create_frame(&mut self, view: &GvspView<'_>, now: Instant) -> Option<usize> {
        if let Some(i) = self.frames.iter().position(|f| f.frame_id == view.frame_id) {
            self.frames[i].last_packet_time = now;
            return Some(i);
        }

        let inc = block_id_distance(view.frame_id, self.last_frame_id, view.extended_ids);
        let mut replay_of = None;
        if inc < 1 && inc > -DISCARD_LATE_FRAME_THRESHOLD {
            match self.classify_behind(view) {
                Arrival::New => {}
                Arrival::MaybeReplay(fp) => replay_of = Some(fp),
                Arrival::Late => {
                    tracing::trace!(
                        frame_id = view.frame_id,
                        last = self.last_frame_id,
                        "discarding late frame"
                    );
                    self.stats.ignored_packets += 1;
                    return None;
                }
            }
        }
        let provisional = replay_of.is_some();
        if !provisional {
            self.discard_provisional();
        }

        let n_packets = self.compute_n_expected_packets(view);
        if n_packets == 0 {
            self.stats.ignored_packets += 1;
            if provisional {
                return None;
            }
            // Unsupported payload (multipart/H264/GenDC/...) or an
            // unparsable first packet: count and drop the whole frame.
            self.stats.unsupported_frames += 1;
            self.last_frame_id = view.frame_id;
            self.remember(view.frame_id, view.extended_ids, None);
            return None;
        }

        let Some(mut slot) = self.pool.try_claim() else {
            if provisional {
                self.stats.ignored_packets += 1;
                return None;
            }
            // Advance the id so the frame's remaining packets are discarded
            // as late instead of each counting another underrun.
            self.stats.underruns += 1;
            self.last_frame_id = view.frame_id;
            self.remember(view.frame_id, view.extended_ids, None);
            return None;
        };
        slot.reset(n_packets);

        if !provisional {
            if inc > 1 {
                tracing::trace!(
                    skipped = inc - 1,
                    after = self.last_frame_id,
                    "frame ids skipped"
                );
                self.stats.missing_frames += inc as u64 - 1;
            }
            self.last_frame_id = view.frame_id;
        }
        let block_size = self
            .scps_packet_size
            .saturating_sub(gvsp::packet_protocol_overhead(view.extended_ids));
        self.frames.push(FrameInFlight {
            frame_id: view.frame_id,
            extended_ids: view.extended_ids,
            slot,
            n_packets,
            block_size,
            block_known: false,
            held: Vec::new(),
            held_bytes: 0,
            trailer_seen: false,
            last_valid_packet: -1,
            received_size: 0,
            data_end: 0,
            last_packet_time: now,
            leader: None,
            system_timestamp_ns: 0,
            error: None,
            resend_disabled: provisional,
            resend_ratio_reached: false,
            n_resend_requests: 0,
            fingerprints: Fingerprints::default(),
            replay_of,
            absorbed: 0,
        });
        Some(self.frames.len() - 1)
    }

    /// Most packets a frame can have before its block size is known: one
    /// leader, one trailer, and payload packets of at least one byte each.
    fn packet_limit(&self) -> usize {
        self.payload_size.saturating_add(2).min(MAX_FRAME_PACKETS)
    }

    /// Expected packets for a frame, judged from whichever packet arrives
    /// first (the leader may be lost). Mirrors `_compute_n_expected_packets`.
    fn compute_n_expected_packets(&self, view: &GvspView<'_>) -> usize {
        let block_size = self
            .scps_packet_size
            .saturating_sub(gvsp::packet_protocol_overhead(view.extended_ids));
        if block_size == 0 {
            return 0;
        }
        let estimate = (self.payload_size.div_ceil(block_size) + 2).min(MAX_FRAME_PACKETS);
        match view.content_type {
            ContentType::Leader => {
                let payload_type = ImageLeader::parse(view.data).map(|l| l.payload_type);
                match payload_type {
                    Some(
                        gvsp::PAYLOAD_TYPE_IMAGE
                        | gvsp::PAYLOAD_TYPE_CHUNK_DATA
                        | gvsp::PAYLOAD_TYPE_EXTENDED_CHUNK_DATA,
                    ) => estimate,
                    _ => 0,
                }
            }
            ContentType::Payload => estimate,
            // A trailer claiming more packets than the payload can fill is
            // rejected when processed; the frame is sized as if from a
            // payload packet meanwhile.
            ContentType::Trailer => match view.packet_id as usize + 1 {
                n @ 2.. if n <= self.packet_limit() => n,
                _ => estimate,
            },
            ContentType::AllIn => 1,
            _ => 0,
        }
    }

    fn process_leader(&mut self, index: usize, view: &GvspView<'_>) {
        let frame = &mut self.frames[index];
        if frame.error.is_some() {
            return;
        }
        if view.packet_id != 0 {
            frame.error = Some(FrameStatus::WrongPacketId);
            return;
        }
        frame.system_timestamp_ns = clock::unix_nanos_now();
        frame.leader = ImageLeader::parse(view.data);
    }

    /// Payload packets carry no offset: packet `k` holds block `k - 1`, and
    /// every block but the last is exactly as long as payload packet 1.
    /// Packets that arrive before packet 1 are held until it fixes the
    /// block size, then placed.
    fn process_payload(&mut self, index: usize, view: &GvspView<'_>) {
        let payload_size = self.payload_size;
        let limit = self.packet_limit();
        let frame = &mut self.frames[index];
        if frame.error.is_some() {
            return;
        }
        let packet_id = view.packet_id as usize;

        if packet_id == 1 && !frame.block_known {
            frame.block_known = true;
            if !view.data.is_empty() {
                frame.block_size = view.data.len();
            }
            let fits = payload_size.div_ceil(frame.block_size) + 2;
            if !frame.trailer_seen {
                frame.set_n_packets(fits.min(MAX_FRAME_PACKETS));
            } else if frame.n_packets > fits {
                frame.error = Some(FrameStatus::WrongPacketId);
                return;
            }
        }

        let last_payload = if frame.block_known || frame.trailer_seen {
            frame.n_packets.saturating_sub(2)
        } else {
            limit - 2
        };
        if packet_id < 1 || packet_id > last_payload {
            tracing::trace!(
                "payload packet id {packet_id} outside 1..={last_payload} (frame {}, {} data bytes, ext={})",
                frame.frame_id,
                view.data.len(),
                view.extended_ids,
            );
            frame.error = Some(FrameStatus::WrongPacketId);
            return;
        }

        if !frame.block_known {
            frame.held_bytes += view.data.len();
            if frame.held_bytes > frame.slot.data.len() + gvsp::MAXIMUM_PACKET_SIZE {
                self.stats.size_mismatch_errors += 1;
                frame.error = Some(FrameStatus::WrongPacketId);
                return;
            }
            if packet_id + 2 > frame.n_packets {
                frame.set_n_packets(packet_id + 2);
            }
            frame.held.push((packet_id, view.data.to_vec()));
            return;
        }

        frame.place(packet_id, view.data, &mut self.stats);
        for (id, data) in std::mem::take(&mut frame.held) {
            if frame.error.is_some() {
                break;
            }
            if id > frame.n_packets.saturating_sub(2) {
                frame.error = Some(FrameStatus::WrongPacketId);
                break;
            }
            frame.place(id, &data, &mut self.stats);
        }
        frame.held_bytes = 0;
    }

    fn process_trailer(&mut self, index: usize, view: &GvspView<'_>) {
        let payload_size = self.payload_size;
        let limit = self.packet_limit();
        let frame = &mut self.frames[index];
        frame.trailer_seen = true;
        if frame.error.is_some() {
            return;
        }
        let packet_id = view.packet_id as usize;
        let bound = if frame.block_known {
            (payload_size.div_ceil(frame.block_size) + 2).min(MAX_FRAME_PACKETS)
        } else {
            limit
        };
        // A payload packet at or past the trailer's id contradicts it.
        let contradicted = frame
            .slot
            .packets
            .iter()
            .skip(packet_id)
            .any(|p| p.received);
        if packet_id < 1 || packet_id >= bound || contradicted {
            frame.error = Some(FrameStatus::WrongPacketId);
            return;
        }
        // The trailer's id is authoritative for how many packets the
        // frame has: an early one means the actual payload is smaller
        // than the buffer.
        if frame.n_packets != packet_id + 1 {
            frame.set_n_packets(packet_id + 1);
        }
    }

    /// Port of `_missing_packet_check`: walk the hole span behind
    /// `packet_id`, arm per-packet deadlines on first sight, and coalesce
    /// expired holes into ranged resend requests.
    fn missing_packet_check(&mut self, index: usize, packet_id: u32, now: Instant) {
        let frame = &mut self.frames[index];
        if !self.resend_enabled || frame.resend_disabled || frame.resend_ratio_reached {
            return;
        }
        let max_requests = (frame.n_packets as f64 * self.cfg.packet_request_ratio) as usize;
        if max_requests == 0 {
            return;
        }
        let packet_id = packet_id as usize;
        if packet_id >= frame.n_packets {
            return;
        }

        let mut first_missing: Option<usize> = None;
        let mut i = (frame.last_valid_packet + 1).max(0) as usize;
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        while i <= packet_id + 1 {
            let need_resend = if i <= packet_id && !frame.slot.packets[i].received {
                let deadline = frame.slot.packets[i]
                    .resend_deadline
                    .get_or_insert(now + self.cfg.initial_packet_timeout);
                now > *deadline
            } else {
                false
            };

            if need_resend && first_missing.is_none() {
                first_missing = Some(i);
            }
            if (i > packet_id || !need_resend)
                && let Some(first) = first_missing.take()
            {
                let last = i - 1;
                let n_missing = last - first + 1;
                if frame.n_resend_requests + n_missing > max_requests {
                    frame.n_resend_requests += n_missing;
                    frame.resend_ratio_reached = true;
                    self.stats.resend_ratio_reached += 1;
                    return;
                }
                frame.n_resend_requests += n_missing;
                for p in &mut frame.slot.packets[first..=last] {
                    p.resend_requested = true;
                    p.resend_deadline = Some(now + self.cfg.packet_timeout);
                }
                ranges.push((first, last));
            }
            i += 1;
        }

        let (frame_id, extended_ids) = (frame.frame_id, frame.extended_ids);
        for (first, last) in ranges {
            self.send_resend_request(frame_id, first as u32, last as u32, extended_ids);
            self.stats.resend_requests += (last - first + 1) as u64;
        }
    }

    fn send_resend_request(&mut self, frame_id: u64, first: u32, last: u32, extended_ids: bool) {
        tracing::trace!(frame_id, first, last, "requesting packet resend");
        // Private wrap, back to the seed: resend ids must stay out of the
        // control worker's 1..=0xfffe range.
        self.resend_id = if self.resend_id == u16::MAX {
            RESEND_ID_SEED
        } else {
            self.resend_id + 1
        };
        let len = gvcp::encode_packet_resend(
            &mut self.resend_buf,
            frame_id,
            first,
            last,
            extended_ids,
            self.resend_id,
        );
        if let Err(e) = self
            .socket
            .send_to(&self.resend_buf[..len], self.device_gvcp_addr)
        {
            tracing::trace!("resend request send failed: {e}");
            return;
        }
        if let Some(sink) = &self.telemetry
            && let Some(tx) = wire::stream_tx(&self.resend_buf[..len])
        {
            sink.sent(&tx, clock::system_now());
        }
    }

    /// Port of `_check_frame_completion`: frames close strictly head-of-line.
    fn check_frame_completion(&mut self, now: Instant, current_frame_id: Option<u64>) {
        let mut index = 0;
        let mut can_close = true;
        while index < self.frames.len() {
            let frame = &self.frames[index];
            let all_received = frame.last_valid_packet == frame.n_packets as i64 - 1;

            if frame.replay_of.is_some() {
                if all_received
                    || now.duration_since(frame.last_packet_time) >= self.cfg.frame_retention
                {
                    self.discard_frame(index);
                } else {
                    index += 1;
                }
                continue;
            }
            if can_close && all_received {
                // Blocks are disjoint and none longer than the block size,
                // so data bytes short of the extent mean a payload packet
                // came up short of its block.
                let status = frame
                    .error
                    .unwrap_or(if frame.received_size == frame.data_end {
                        FrameStatus::Complete
                    } else {
                        FrameStatus::MissingPackets
                    });
                self.close_frame(index, status);
                continue;
            }
            // Completeness is tested first: a head frame whose last packet
            // arrived after a newer frame opened is still a good frame.
            if can_close
                && !self.resend_enabled
                && self.frames[index + 1..]
                    .iter()
                    .any(|f| f.replay_of.is_none())
            {
                self.close_frame(index, FrameStatus::MissingPackets);
                continue;
            }
            // Never time out the newest frame whose only packet so far is
            // the leader — some devices send the leader at trigger time,
            // long before the data.
            if can_close
                && (frame.frame_id != self.last_frame_id || frame.last_valid_packet != 0)
                && now.duration_since(frame.last_packet_time) >= self.cfg.frame_retention
            {
                let status = frame.error.unwrap_or(FrameStatus::Timeout);
                self.close_frame(index, status);
                continue;
            }

            can_close = false;
            if current_frame_id != Some(frame.frame_id)
                && now.duration_since(frame.last_packet_time) >= self.cfg.packet_timeout
            {
                let last = self.frames[index].n_packets as u32 - 1;
                self.missing_packet_check(index, last, now);
            }
            index += 1;
        }
    }

    fn close_frame(&mut self, index: usize, status: FrameStatus) {
        // A Subscribe can land while drain_socket is mid-pass; pick it up
        // here so a subscription sent before this frame's packets arrived
        // never misses the frame.
        self.drain_commands();
        let frame = self.frames.remove(index);
        self.remember(frame.frame_id, frame.extended_ids, Some(frame.fingerprints));
        if status != FrameStatus::Complete {
            tracing::trace!(
                frame_id = frame.frame_id,
                ?status,
                "frame closed incomplete"
            );
        }
        match status {
            FrameStatus::Complete => self.stats.completed_frames += 1,
            FrameStatus::Timeout => {
                self.stats.timed_out_frames += 1;
                self.stats.failed_frames += 1;
                self.shared.link.frame_timed_out();
            }
            FrameStatus::Aborted => self.stats.aborted_frames += 1,
            _ => {
                self.stats.failed_frames += 1;
                self.shared.link.frame_incomplete();
            }
        }
        if status != FrameStatus::Complete && status != FrameStatus::Aborted {
            self.stats.missing_packets +=
                (frame.n_packets as i64 - (frame.last_valid_packet + 1)).max(0) as u64;
        }

        let leader = frame.leader;
        let payload = match leader.map(|l| l.payload_type) {
            Some(gvsp::PAYLOAD_TYPE_IMAGE | gvsp::PAYLOAD_TYPE_EXTENDED_CHUNK_DATA) => {
                PayloadKind::Image {
                    has_chunks: leader.is_some_and(|l| l.has_chunks),
                }
            }
            Some(gvsp::PAYLOAD_TYPE_CHUNK_DATA) => PayloadKind::ChunkData,
            Some(other) => PayloadKind::Unknown(other),
            None => PayloadKind::Unknown(0),
        };
        let timestamp_ticks = leader.map_or(0, |l| l.timestamp_ticks);
        let timestamp_ns = if self.tick_frequency != 0 {
            gvsp::timestamp_to_ns(timestamp_ticks, self.tick_frequency)
        } else {
            frame.system_timestamp_ns
        };
        let out = Frame {
            status,
            frame_id: frame.frame_id,
            payload,
            pixel_format: leader.map(|l| l.pixel_format).unwrap_or_default(),
            width: leader.map_or(0, |l| l.width),
            height: leader.map_or(0, |l| l.height),
            x_offset: leader.map_or(0, |l| l.x_offset),
            y_offset: leader.map_or(0, |l| l.y_offset),
            x_padding: leader.map_or(0, |l| l.x_padding),
            y_padding: leader.map_or(0, |l| l.y_padding),
            timestamp_ticks,
            timestamp_ns,
            system_timestamp_ns: frame.system_timestamp_ns,
            received_size: frame.received_size,
            data_end: frame.data_end,
            data: PooledBuf::new(frame.slot, self.pool.returner()),
        };
        let out = Arc::new(out);
        // Publish before fan-out so a subscriber that wakes on this frame
        // already sees it reflected in the stats snapshot.
        self.publish_stats();
        let mut dropped = 0u64;
        self.subscribers
            .retain(|tx| match tx.try_send(out.clone()) {
                Ok(()) => true,
                Err(flume::TrySendError::Full(_)) => {
                    dropped += 1;
                    true
                }
                Err(flume::TrySendError::Disconnected(_)) => false,
            });
        if dropped > 0 {
            self.stats.frames_dropped += dropped;
            self.publish_stats();
        }
    }

    /// Drops a frame taken for late copies of a closed one; its packets
    /// were counted as ignored on arrival.
    fn discard_frame(&mut self, index: usize) {
        let frame = self.frames.remove(index);
        drop(PooledBuf::new(frame.slot, self.pool.returner()));
    }

    fn discard_provisional(&mut self) {
        while let Some(i) = self.frames.iter().position(|f| f.replay_of.is_some()) {
            self.discard_frame(i);
        }
    }

    fn flush_frames(&mut self) {
        self.discard_provisional();
        while !self.frames.is_empty() {
            self.close_frame(0, FrameStatus::Aborted);
        }
    }
}

/// Negotiated link parameters the worker needs alongside the user config.
pub(crate) struct LinkParams {
    pub device_gvcp_addr: SocketAddr,
    pub scps_packet_size: u16,
    /// Resolved frame buffer size (`StreamConfig::payload_size` is optional
    /// at the API surface; it is mandatory by the time a worker spawns).
    pub payload_size: usize,
    pub resend_enabled: bool,
    pub tick_frequency: u64,
}

/// Take a prepared (bound, buffer-sized, negotiated) std socket and launch
/// the stream worker thread. Returns the owner [`ThreadHandle`] and what the
/// thread options came to.
pub(crate) fn spawn(
    std_socket: std::net::UdpSocket,
    link: LinkParams,
    rx: Receiver<ToStreamWorker>,
    shared: Arc<StreamShared>,
    cfg: StreamConfig,
    telemetry: Option<StreamTelemetry>,
) -> Result<(ThreadHandle, OptionReport<ThreadOption>), CameraError> {
    let mut socket =
        rx_timestamp::stamped(std_socket, "gvsp").map_err(|e| CameraError::Spawn(e.to_string()))?;

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

    let pool = FramePool::new(cfg.n_buffers, link.payload_size);
    let (started_tx, started_rx) = flume::bounded(1);
    let join = std::thread::Builder::new()
        .name(format!("telegenic-gvsp{}", cfg.channel))
        .spawn(move || {
            let _exit = ExitGuard(thread_for_worker.to_pass_in());
            let _tuning = match tuning::apply_thread(ThreadRole::Stream, &cfg.thread) {
                Ok(report) => {
                    let _ = started_tx.send(Ok(OptionReport::from(&report)));
                    report
                }
                Err(e) => {
                    let _ = started_tx.send(Err(e));
                    return;
                }
            };
            let runner = StreamRunner {
                socket,
                device_gvcp_addr: link.device_gvcp_addr,
                rx,
                shared,
                thread: thread_for_worker,
                pool,
                frames: Vec::with_capacity(4),
                subscribers: Vec::new(),
                shutdown_requested: false,
                stats: StreamStats::default(),
                scps_packet_size: usize::from(link.scps_packet_size),
                payload_size: link.payload_size,
                resend_enabled: link.resend_enabled,
                resend_id: RESEND_ID_SEED,
                resend_buf: [0u8; gvcp::RESEND_MAX_LEN],
                last_frame_id: 0,
                first_packet: true,
                history: VecDeque::with_capacity(HISTORY_LEN),
                tick_frequency: link.tick_frequency,
                telemetry,
                cfg,
            };
            runner.run(poll);
        })
        .map_err(|e| CameraError::Spawn(e.to_string()))?;
    thread.set_handle(join);
    let report = started_rx
        .recv()
        .map_err(|_| CameraError::Spawn("gvsp worker exited during startup".into()))??;

    Ok((thread, report))
}

#[cfg(test)]
mod tests {
    //! Reassembly under corruption, reordering, duplication and loss, and
    //! block-id unwrapping across wraps, driven packet by packet on a
    //! synthetic clock. GVSP carries no checksum: a packet whose header
    //! decodes is taken at its word, so these properties are about what the
    //! receiver does with well-formed packets in any order and with the
    //! datagrams around them, not about detecting flipped payload bits.

    use super::*;
    use crate::gige::stream::StreamStats;
    use crate::link::LinkCounters;
    use proptest::prelude::*;
    use proptest::test_runner::{Config, RngSeed};

    const SCPS: u16 = 576;
    const STD_BLOCK: usize = 540;
    const EXT_BLOCK: usize = 528;
    const STEP: Duration = Duration::from_micros(1);

    fn config(cases: u32) -> Config {
        Config {
            cases,
            rng_seed: RngSeed::Fixed(0x7e1e_9e41_c0de),
            failure_persistence: None,
            ..Config::default()
        }
    }

    /// A delivered frame, copied out so its pool buffer goes back at once.
    #[derive(Debug)]
    struct Got {
        frame_id: u64,
        status: FrameStatus,
        data: Vec<u8>,
        received_size: usize,
        timestamp_ticks: u64,
        timestamp_ns: u64,
        system_timestamp_ns: u64,
    }

    struct Rig {
        runner: StreamRunner,
        delivered: flume::Receiver<Arc<Frame>>,
        got: Vec<Got>,
        device: std::net::UdpSocket,
        _commands: flume::Sender<ToStreamWorker>,
        now: Instant,
    }

    impl Rig {
        fn new(payload_size: usize, resend: bool, tick_frequency: u64) -> Self {
            let device = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            device.set_nonblocking(true).unwrap();
            let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let socket = rx_timestamp::stamped(socket, "gvsp").unwrap();
            let (commands, rx) = flume::unbounded();
            let (tx, delivered) = flume::unbounded();
            let mut cfg = StreamConfig::new();
            cfg.payload_size = Some(payload_size);
            let runner = StreamRunner {
                socket,
                device_gvcp_addr: device.local_addr().unwrap(),
                rx,
                shared: Arc::new(StreamShared {
                    stats: parking_lot::Mutex::new(StreamStats::default()),
                    link: Arc::new(LinkCounters::default()),
                }),
                thread: ThreadHandle::new(),
                pool: FramePool::new(cfg.n_buffers, payload_size),
                frames: Vec::new(),
                subscribers: vec![tx],
                shutdown_requested: false,
                stats: StreamStats::default(),
                scps_packet_size: usize::from(SCPS),
                payload_size,
                resend_enabled: resend,
                resend_id: RESEND_ID_SEED,
                resend_buf: [0u8; gvcp::RESEND_MAX_LEN],
                last_frame_id: 0,
                first_packet: true,
                history: VecDeque::new(),
                tick_frequency,
                telemetry: None,
                cfg,
            };
            Self {
                runner,
                delivered,
                got: Vec::new(),
                device,
                _commands: commands,
                now: Instant::now(),
            }
        }

        fn feed(&mut self, datagram: &[u8]) {
            self.now += STEP;
            self.runner.process_packet(datagram, self.now);
            self.runner.check_frame_completion(self.now, None);
            self.collect();
        }

        fn advance(&mut self, by: Duration) {
            self.now += by;
            self.runner.check_frame_completion(self.now, None);
            self.collect();
        }

        fn collect(&mut self) {
            for f in self.delivered.try_iter() {
                self.got.push(Got {
                    frame_id: f.frame_id,
                    status: f.status,
                    data: f.data().to_vec(),
                    received_size: f.received_size,
                    timestamp_ticks: f.timestamp_ticks,
                    timestamp_ns: f.timestamp_ns,
                    system_timestamp_ns: f.system_timestamp_ns,
                });
            }
        }

        /// Lets every open frame run out its retention window, then closes
        /// whatever is left, and returns everything delivered.
        fn finish(mut self) -> (Vec<Got>, StreamStats) {
            for _ in 0..3 {
                self.advance(Duration::from_secs(1));
            }
            self.runner.flush_frames();
            self.collect();
            (self.got, self.runner.stats)
        }
    }

    fn header(ext: bool, id: u64, packet: u32, content: u8) -> Vec<u8> {
        let mut b = 0u16.to_be_bytes().to_vec();
        if ext {
            b.extend_from_slice(&0u16.to_be_bytes());
            b.extend_from_slice(&(0x8000_0000 | (u32::from(content) << 24)).to_be_bytes());
            b.extend_from_slice(&id.to_be_bytes());
            b.extend_from_slice(&packet.to_be_bytes());
        } else {
            b.extend_from_slice(&(id as u16).to_be_bytes());
            b.extend_from_slice(
                &((u32::from(content) << 24) | (packet & 0x00ff_ffff)).to_be_bytes(),
            );
        }
        b
    }

    fn leader(ext: bool, id: u64, width: usize, timestamp: u64) -> Vec<u8> {
        let mut b = header(ext, id, 0, 1);
        b.extend_from_slice(&0u16.to_be_bytes());
        b.extend_from_slice(&gvsp::PAYLOAD_TYPE_IMAGE.to_be_bytes());
        b.extend_from_slice(&timestamp.to_be_bytes());
        b.extend_from_slice(&gvsp::PixelFormat::MONO8.0.to_be_bytes());
        b.extend_from_slice(&(width as u32).to_be_bytes());
        b.extend_from_slice(&1u32.to_be_bytes());
        b.extend_from_slice(&[0u8; 12]);
        b
    }

    fn trailer(ext: bool, id: u64, packet: u32) -> Vec<u8> {
        let mut b = header(ext, id, packet, 2);
        b.extend_from_slice(&u32::from(gvsp::PAYLOAD_TYPE_IMAGE).to_be_bytes());
        b.extend_from_slice(&1u32.to_be_bytes());
        b
    }

    /// A device's burst for one frame: leader, `data` cut into `block`-byte
    /// payload packets, trailer.
    fn burst(ext: bool, id: u64, data: &[u8], block: usize, timestamp: u64) -> Vec<Vec<u8>> {
        let mut packets = vec![leader(ext, id, data.len(), timestamp)];
        for (i, chunk) in data.chunks(block).enumerate() {
            let mut p = header(ext, id, i as u32 + 1, 3);
            p.extend_from_slice(chunk);
            packets.push(p);
        }
        let n = packets.len() as u32;
        packets.push(trailer(ext, id, n));
        packets
    }

    /// Image bytes no other frame of a run shares.
    fn image(tag: u64, len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u64 * 7 + tag * 13 + 1) as u8)
            .collect()
    }

    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, n: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            ((self.0 >> 33) as usize) % n.max(1)
        }

        fn shuffle<T>(&mut self, v: &mut [T]) {
            for i in (1..v.len()).rev() {
                v.swap(i, self.below(i + 1));
            }
        }
    }

    /// The device's next block id: 16-bit ids wrap 0xffff -> 1, extended
    /// ones u64::MAX -> 1; 0 is never sent.
    fn next_block_id(ext: bool, id: u64) -> u64 {
        let max = if ext { u64::MAX } else { 0xffff };
        if id >= max { 1 } else { id + 1 }
    }

    /// Block ids away from the 16-bit sign boundary at 0x7fff/0x8000 and,
    /// for extended ids, the 64-bit one at 2^63, but including the wraps to
    /// 1.
    fn start_id(ext: bool) -> BoxedStrategy<u64> {
        if ext {
            prop_oneof![
                1u64..1000,
                u64::MAX - 50..=u64::MAX,
                1u64 << 40..1u64 << 62,
                (1u64 << 63) + 200..u64::MAX - 50,
            ]
            .boxed()
        } else {
            prop_oneof![1u64..0x7f00, 0x8100u64..0xfff0, 0xffc0u64..=0xffff].boxed()
        }
    }

    fn assert_exact(frame: &Got, id: u64, data: &[u8]) {
        assert_eq!(frame.frame_id, id);
        assert_eq!(frame.status, FrameStatus::Complete, "frame {id}");
        assert_eq!(frame.received_size, data.len(), "frame {id}");
        assert!(frame.data == data, "frame {id}: bytes differ");
    }

    #[derive(Debug, Clone)]
    struct Burst {
        ext: bool,
        start: u64,
        frames: usize,
        payload_size: usize,
        lengths: Vec<prop::sample::Index>,
        resend: bool,
        shuffle: u64,
        duplicates: usize,
        late: Vec<(prop::sample::Index, prop::sample::Index)>,
    }

    fn bursts() -> impl Strategy<Value = Burst> {
        bursts_in(any::<bool>())
    }

    fn bursts_in(ext: impl Strategy<Value = bool>) -> impl Strategy<Value = Burst> {
        ext.prop_flat_map(|ext| {
            (
                start_id(ext),
                1usize..6,
                1usize..3000,
                prop::collection::vec(any::<prop::sample::Index>(), 6),
                any::<bool>(),
                any::<u64>(),
                0usize..5,
                prop::collection::vec(any::<(prop::sample::Index, prop::sample::Index)>(), 0..6),
            )
                .prop_map(
                    move |(
                        start,
                        frames,
                        payload_size,
                        lengths,
                        resend,
                        shuffle,
                        duplicates,
                        late,
                    )| Burst {
                        ext,
                        start,
                        frames,
                        payload_size,
                        lengths,
                        resend,
                        shuffle,
                        duplicates,
                        late,
                    },
                )
        })
    }

    /// Plays `b` with the device cutting its image into `block`-byte
    /// packets and shuffling each burst, frame after frame; copies of
    /// packets from frames already closed arrive in between. Returns what
    /// was sent and what the receiver made of it.
    #[allow(clippy::type_complexity)]
    fn play(b: &Burst, block: usize) -> (Vec<(u64, Vec<u8>)>, Vec<Got>, StreamStats, (u64, u64)) {
        let mut rig = Rig::new(b.payload_size, b.resend, 0);
        let mut rng = Lcg(b.shuffle);
        let mut sent: Vec<(u64, Vec<u8>, Vec<Vec<u8>>)> = Vec::new();
        let mut id = b.start;
        let (mut duplicates, mut late) = (0, 0);
        for k in 0..b.frames {
            let len = b.lengths[k].index(b.payload_size + 1);
            let data = image(k as u64, len);
            let packets = burst(b.ext, id, &data, block, 0);
            let mut order: Vec<usize> = (0..packets.len()).collect();
            rng.shuffle(&mut order);
            for _ in 0..b.duplicates {
                let at = rng.below(order.len());
                let i = order[rng.below(order.len())];
                order.insert(at, i);
            }
            let mut seen = vec![false; packets.len()];
            for &i in &order {
                if seen.iter().all(|&s| s) {
                    late += 1;
                } else if seen[i] {
                    duplicates += 1;
                }
                seen[i] = true;
                rig.feed(&packets[i]);
            }
            for (which, packet) in &b.late {
                if !sent.is_empty() && rng.below(2) == 0 {
                    let (_, _, earlier) = &sent[which.index(sent.len())];
                    rig.feed(&earlier[packet.index(earlier.len())]);
                    late += 1;
                }
            }
            sent.push((id, data, packets));
            id = next_block_id(b.ext, id);
        }
        let (frames, stats) = rig.finish();
        let sent = sent.into_iter().map(|(id, data, _)| (id, data)).collect();
        (sent, frames, stats, (duplicates, late))
    }

    fn assert_all_exact(sent: &[(u64, Vec<u8>)], frames: &[Got]) {
        let ids: Vec<u64> = frames.iter().map(|f| f.frame_id).collect();
        let want: Vec<u64> = sent.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, want, "frames delivered");
        for ((id, data), frame) in sent.iter().zip(frames) {
            assert_exact(frame, *id, data);
        }
    }

    proptest! {
        #![proptest_config(config(512))]

        /// Any order within a burst, repeats anywhere in it, and copies of
        /// earlier frames' packets arriving after those frames closed: every
        /// frame is delivered once, in order, complete and byte-exact; a
        /// repeat inside an open frame counts as a duplicate, one after its
        /// frame closed is dropped as late, and no stale copy reopens or
        /// touches a frame.
        #[test]
        fn shuffled_and_repeated_bursts_reassemble_exactly(b in bursts()) {
            let block = if b.ext { EXT_BLOCK } else { STD_BLOCK };
            let (sent, frames, stats, (duplicates, late)) = play(&b, block);
            assert_all_exact(&sent, &frames);
            prop_assert_eq!((stats.duplicated_packets, stats.ignored_packets), (duplicates, late));
            prop_assert_eq!((stats.failed_frames, stats.missing_frames), (0, 0));
        }

        /// A device whose payload blocks are smaller than the packet size
        /// implies (devices align or cap them) has its block size adopted
        /// from payload packet 1; with the burst in order every frame is
        /// byte-exact.
        #[test]
        fn a_capped_block_size_is_adopted_from_the_first_payload_packet(
            b in bursts(),
            short_by in 1usize..64,
        ) {
            let block = if b.ext { EXT_BLOCK } else { STD_BLOCK } - short_by;
            let mut rig = Rig::new(b.payload_size, b.resend, 0);
            let mut sent = Vec::new();
            let mut id = b.start;
            for k in 0..b.frames {
                let data = image(k as u64, b.lengths[k].index(b.payload_size + 1));
                for p in burst(b.ext, id, &data, block, 0) {
                    rig.feed(&p);
                }
                sent.push((id, data));
                id = next_block_id(b.ext, id);
            }
            let (frames, _) = rig.finish();
            assert_all_exact(&sent, &frames);
        }

        /// Arbitrary datagrams mixed into valid standard-id bursts, and
        /// valid packets with flipped bits, never panic the receiver, and
        /// every frame it delivers stays inside its buffer: no frame reports
        /// more data than the payload size, and a complete frame's data is
        /// exactly what it counted as received. The corruption here leaves
        /// the id-mode and content-type byte and trailer packet ids alone,
        /// and the device ticks at 1 GHz (where no 64-bit timestamp
        /// overflows the conversion); the ignored variant below lifts all
        /// of that.
        #[test]
        fn corrupted_and_junk_datagrams_never_break_frame_bounds(
            b in bursts_in(Just(false)),
            junk in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..8),
            flips in prop::collection::vec(any::<(prop::sample::Index, prop::sample::Index)>(), 0..12),
            ticks in prop::collection::vec(0u64..300_000, 0..8),
        ) {
            corrupted_run(&b, junk, &flips, &ticks, true, 1_000_000_000)?;
        }

        /// Clean frames whose 16-bit block ids follow the device counter
        /// across any number of wraps, from any start, with any ids the
        /// device skips, are all delivered complete and in order.
        #[test]
        fn standard_block_ids_are_followed_across_wraps(
            start in 1u64..=0xffff,
            gaps in prop::collection::vec(1u64..=50, 1..40),
        ) {
            let (sent, frames, _) = wrap_run(false, start, &gaps);
            assert_all_exact(&sent, &frames);
        }

        /// Extended block ids near the 64-bit wrap and the low range.
        #[test]
        fn extended_block_ids_are_followed_across_the_wrap(
            start in prop_oneof![1u64..200, u64::MAX - 400..=u64::MAX],
            gaps in prop::collection::vec(1u64..=50, 1..40),
        ) {
            let (sent, frames, _) = wrap_run(true, start, &gaps);
            assert_all_exact(&sent, &frames);
        }

        /// Leader timestamps convert to nanoseconds at the device's tick
        /// frequency, exactly, whatever that frequency is; with no known
        /// frequency the frame carries the host's receive time.
        #[test]
        fn leader_timestamps_convert_at_the_device_tick_frequency(
            frequency in prop_oneof![Just(0u64), 1u64..=1_000_000_000],
            raw in any::<u64>(),
        ) {
            let representable = u128::from(u64::MAX) * u128::from(frequency.max(1)) / 1_000_000_000;
            let ticks = (u128::from(raw) % (representable + 1)) as u64;
            let exact = u128::from(ticks) * 1_000_000_000 / u128::from(frequency.max(1));
            let mut rig = Rig::new(100, false, frequency);
            for p in burst(false, 1, &image(0, 100), STD_BLOCK, ticks) {
                rig.feed(&p);
            }
            let (frames, _) = rig.finish();
            prop_assert_eq!(frames.len(), 1);
            let f = &frames[0];
            prop_assert_eq!(f.timestamp_ticks, ticks);
            if frequency == 0 {
                prop_assert_eq!(f.timestamp_ns, f.system_timestamp_ns);
            } else {
                prop_assert_eq!(u128::from(f.timestamp_ns), exact);
            }
        }
    }

    fn corrupted_run(
        b: &Burst,
        junk: Vec<Vec<u8>>,
        flips: &[(prop::sample::Index, prop::sample::Index)],
        ticks: &[u64],
        keep_id_mode: bool,
        tick_frequency: u64,
    ) -> Result<(), TestCaseError> {
        let block = if b.ext { EXT_BLOCK } else { STD_BLOCK };
        let mut rig = Rig::new(b.payload_size, b.resend, tick_frequency);
        let mut stream: Vec<Vec<u8>> = Vec::new();
        let mut id = b.start;
        for k in 0..b.frames {
            let data = image(k as u64, b.lengths[k].index(b.payload_size + 1));
            stream.extend(burst(b.ext, id, &data, block, k as u64));
            id = next_block_id(b.ext, id);
        }
        for (packet, bit) in flips {
            let at = packet.index(stream.len());
            let p = &mut stream[at];
            let bit = bit.index(p.len() * 8);
            let trailer_id = p[4] & 0x7f == 2 && (5..8).contains(&(bit / 8));
            if keep_id_mode && (bit / 8 == 4 || trailer_id) {
                continue;
            }
            p[bit / 8] ^= 1 << (bit % 8);
        }
        let mut rng = Lcg(b.shuffle);
        for mut j in junk {
            if keep_id_mode && j.len() > 4 {
                j[4] = if j[4] & 1 == 0 { 1 } else { 3 };
            }
            let at = rng.below(stream.len() + 1);
            stream.insert(at, j);
        }
        for (i, p) in stream.iter().enumerate() {
            rig.feed(p);
            if let Some(t) = ticks.get(i) {
                rig.advance(Duration::from_micros(*t));
            }
        }
        let (frames, _) = rig.finish();
        for f in &frames {
            prop_assert!(
                f.data.len() <= b.payload_size,
                "frame {} overflows",
                f.frame_id
            );
            prop_assert!(
                f.received_size <= b.payload_size,
                "frame {} overcounts",
                f.frame_id
            );
            if f.status == FrameStatus::Complete {
                prop_assert_eq!(f.received_size, f.data.len());
            }
        }
        Ok(())
    }

    /// Sends one clean frame per entry of `gaps`, the device counter
    /// advancing by that many ids before each (a gap of 1 skips nothing).
    #[allow(clippy::type_complexity)]
    fn wrap_run(
        ext: bool,
        start: u64,
        gaps: &[u64],
    ) -> (Vec<(u64, Vec<u8>)>, Vec<Got>, StreamStats) {
        let mut rig = Rig::new(200, false, 0);
        let mut sent = Vec::new();
        let mut id = start;
        for (k, gap) in gaps.iter().enumerate() {
            if k > 0 {
                for _ in 0..*gap {
                    id = next_block_id(ext, id);
                }
            }
            let data = image(k as u64, 200);
            for p in burst(ext, id, &data, STD_BLOCK, 0) {
                rig.feed(&p);
            }
            sent.push((id, data));
        }
        let (frames, stats) = rig.finish();
        (sent, frames, stats)
    }

    proptest! {
        #![proptest_config(config(256))]

        /// Packets of one burst in any order, with the device's blocks
        /// smaller than the packet size implies: data that arrives before
        /// payload packet 1 must still land at its true offset.
        #[test]
        fn a_capped_block_size_survives_any_packet_order(b in bursts(), short_by in 1usize..64) {
            let block = if b.ext { EXT_BLOCK } else { STD_BLOCK } - short_by;
            let (sent, frames, _, _) = play(&b, block);
            for f in &frames {
                if f.status == FrameStatus::Complete {
                    let (_, data) = sent.iter().find(|(id, _)| *id == f.frame_id).unwrap();
                    prop_assert!(f.data == *data, "frame {} complete with wrong bytes", f.frame_id);
                }
            }
        }

        /// Frames skipped by the device are counted exactly, across the
        /// 16-bit sign boundary as anywhere else.
        #[test]
        fn skipped_standard_block_ids_are_counted_exactly(
            start in 1u64..=0xffff,
            gaps in prop::collection::vec(1u64..=50, 1..40),
        ) {
            let (_, _, stats) = wrap_run(false, start, &gaps);
            let skipped: u64 = gaps[1..].iter().map(|g| g - 1).sum();
            prop_assert_eq!(stats.missing_frames, skipped);
        }

        /// A copy of an earlier frame's packet arriving after that frame
        /// closed is dropped as late from any position, the 16-bit sign
        /// boundary included: no frame is reopened.
        #[test]
        fn late_copies_never_reopen_a_closed_frame(start in 0x7ff0u64..0x8004) {
            let mut rig = Rig::new(200, true, 0);
            let mut sent = Vec::new();
            let mut id = start;
            let mut previous: Option<Vec<u8>> = None;
            for k in 0..4u64 {
                let data = image(k, 200);
                let packets = burst(false, id, &data, STD_BLOCK, 0);
                rig.feed(&packets[0]);
                if let Some(p) = &previous {
                    rig.feed(p);
                }
                for p in &packets[1..] {
                    rig.feed(p);
                }
                previous = Some(packets[packets.len() - 1].clone());
                sent.push((id, data));
                id = next_block_id(false, id);
            }
            let (frames, _) = rig.finish();
            assert_all_exact(&sent, &frames);
        }

        /// The same with any bit open to corruption, in both id modes, at a
        /// 125 MHz tick: one flipped id-mode bit turns a standard packet
        /// into an extended one whose 64-bit block id is whatever its data
        /// bytes were, one flipped content-type bit leaves a payload packet
        /// unwritten, and one flipped high timestamp bit makes the leader's
        /// time unrepresentable in u64 nanoseconds.
        #[test]
        fn any_corrupted_datagram_never_breaks_frame_bounds(
            b in bursts(),
            junk in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..8),
            flips in prop::collection::vec(any::<(prop::sample::Index, prop::sample::Index)>(), 0..12),
            ticks in prop::collection::vec(0u64..300_000, 0..8),
        ) {
            corrupted_run(&b, junk, &flips, &ticks, false, 125_000_000)?;
        }

        /// Extended block ids from anywhere in the 64-bit space, the 2^63
        /// boundary included.
        #[test]
        fn extended_block_ids_are_followed_from_any_start(
            start in prop_oneof![1u64.., (1u64 << 63) - 20..(1u64 << 63) + 20],
            gaps in prop::collection::vec(1u64..=50, 1..10),
        ) {
            let run = std::panic::catch_unwind(|| wrap_run(true, start, &gaps));
            prop_assert!(run.is_ok(), "the receiver panicked");
            let (sent, frames, _) = run.unwrap();
            assert_all_exact(&sent, &frames);
        }

        /// A device whose block-id counter wraps (or restarts) at a modulus
        /// other than the protocol's — e.g. one that resets to 1 on each
        /// acquisition start while the stream channel stays open — has
        /// every frame delivered.
        #[test]
        fn a_block_id_counter_with_any_modulus_is_followed(
            modulus in prop_oneof![2u64..200, 200u64..=0xffff],
            start in 1u64..0xffff,
        ) {
            let mut rig = Rig::new(200, false, 0);
            let mut sent = Vec::new();
            let mut id = (start - 1) % (modulus - 1) + 1;
            for k in 0..(modulus + 5).min(300) {
                let data = image(k, 200);
                for p in burst(false, id, &data, STD_BLOCK, 0) {
                    rig.feed(&p);
                }
                sent.push((id, data));
                id = if id + 1 >= modulus { 1 } else { id + 1 };
            }
            let (frames, _) = rig.finish();
            assert_all_exact(&sent, &frames);
        }
    }

    /// A packet whose content type the receiver does not reassemble (a
    /// flipped content-type bit is enough) must not count as received: the
    /// frame it belongs to cannot complete with that packet's bytes absent.
    #[test]
    fn a_packet_of_unknown_content_type_never_completes_a_frame() {
        let data = image(0, 3 * STD_BLOCK);
        let mut packets = burst(false, 1, &data, STD_BLOCK, 0);
        packets[2][4] = 0x7f;
        let mut rig = Rig::new(data.len(), false, 0);
        for p in &packets {
            rig.feed(p);
        }
        let (frames, _) = rig.finish();
        assert_eq!(frames.len(), 1);
        assert_ne!(
            frames[0].status,
            FrameStatus::Complete,
            "complete without packet 2's bytes"
        );
    }

    /// A leader timestamp that is valid on the wire converts without
    /// panicking the stream worker, however far it lies in the future at the
    /// device's tick frequency.
    #[test]
    fn any_leader_timestamp_closes_its_frame() {
        let mut rig = Rig::new(100, false, 125_000_000);
        for p in burst(false, 1, &image(0, 100), STD_BLOCK, u64::MAX) {
            rig.feed(&p);
        }
        let (frames, _) = rig.finish();
        assert_eq!(frames.len(), 1);
    }

    /// A standard-id stream whose id-mode bit flips in one packet reads that
    /// packet as extended, with a block id taken from its data bytes; the
    /// receiver must survive that id wherever it lands.
    #[test]
    fn a_flipped_id_mode_bit_never_panics_the_receiver() {
        let mut rig = Rig::new(100, false, 0);
        for p in burst(false, 5, &image(0, 100), STD_BLOCK, 0) {
            rig.feed(&p);
        }
        let mut stray = header(true, 0x8000_0000_0000_0010, 1, 3);
        stray.extend_from_slice(&[0u8; 16]);
        rig.feed(&stray);
        let mut stray = header(true, 0x7fff_ffff_ffff_fff0, 1, 3);
        stray.extend_from_slice(&[0u8; 16]);
        rig.feed(&stray);
        let _ = rig.finish();
    }

    /// A trailer is the first packet of a frame often enough (the leader
    /// lost, a burst reordered) that its packet id sizes the frame's
    /// bookkeeping; a corrupted id must not size it past what the payload
    /// size allows.
    #[test]
    fn a_trailer_cannot_size_a_frame_past_its_payload() {
        let mut rig = Rig::new(2000, true, 0);
        rig.feed(&trailer(false, 1, 0x00ff_ffff));
        let slots = rig
            .runner
            .frames
            .first()
            .map_or(0, |f| f.slot.packets.len());
        assert!(
            slots <= 2000 + 2,
            "{slots} packet slots for a 2000-byte payload"
        );
    }

    /// Stream-side request ids for packet resends run from the seed to
    /// 0xffff and wrap back to the seed, never reaching 0.
    #[test]
    fn resend_request_ids_wrap_within_their_range() {
        let mut rig = Rig::new(100, true, 0);
        let mut ids = Vec::new();
        let mut buf = [0u8; 64];
        for _ in 0..600 {
            rig.runner.send_resend_request(1, 1, 1, false);
            let (n, _) = loop {
                match rig.device.recv_from(&mut buf) {
                    Ok(r) => break r,
                    Err(e) if e.kind() == ErrorKind::WouldBlock => std::thread::yield_now(),
                    Err(e) => panic!("{e}"),
                }
            };
            ids.push(gvcp::Cmd::parse(&buf[..n]).unwrap().req_id);
        }
        let span = u32::from(u16::MAX - RESEND_ID_SEED) + 1;
        for (k, id) in ids.iter().enumerate() {
            let want = RESEND_ID_SEED as u32 + (k as u32 + 1) % span;
            assert_eq!(u32::from(*id), want, "resend {k}");
        }
    }
}
