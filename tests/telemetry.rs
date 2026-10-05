//! The [`TelemetrySink`] hooks on both channels against the fake camera:
//! GVCP transactions and events on the control socket, GVSP datagrams on the
//! stream socket — each inside a deterministic snare simulation.

#![cfg(all(
    unix,
    any(
        all(
            target_os = "linux",
            target_env = "gnu",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ),
        target_os = "macos",
        windows
    )
))]

mod fake_camera;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use fake_camera::{FakeCamera, FrameOpts, sim};
use telegenic::gige::proto::bootstrap;
use telegenic::gige::proto::gvsp::ContentType;
use telegenic::gige::{GigECamera, GigeConfig};
use telegenic::wire::{ControlRx, ControlTx, GvcpCmd, GvspPacket, TelemetrySink};
use telegenic::{PacketSize, StreamConfig};

const BLOCK: usize = 500;

fn connect(fake: &FakeCamera, sink: Option<ControlObserver>) -> GigECamera {
    let mut cfg = GigeConfig::new(std::net::Ipv4Addr::LOCALHOST);
    cfg.addr = fake.addr();
    cfg.local_addr = Some(fake_camera::LOOPBACK_ANY_PORT);
    cfg.gvcp_timeout = Duration::from_millis(500);
    cfg.retries = 2;
    let mut cam = GigECamera::with_config(cfg);
    if let Some(sink) = sink {
        cam.set_telemetry(sink);
    }
    cam.connect().expect("connect to fake camera");
    cam
}

#[derive(Default)]
struct ControlLog {
    /// `(command id, retry)` for every command we put on the wire.
    commands: Vec<(u16, bool)>,
    /// Acknowledges we sent back for device events.
    acks_sent: usize,
    /// `(answer, ack id)` for every acknowledge the device sent.
    acks: Vec<(u16, u16)>,
    /// Device-initiated commands (events).
    events: Vec<u16>,
    stamps: Vec<SystemTime>,
}

#[derive(Default)]
struct ControlRecorder {
    warmups: AtomicUsize,
    log: Mutex<ControlLog>,
}

struct ControlObserver(Arc<ControlRecorder>);

impl TelemetrySink<ControlTx, ControlRx> for ControlObserver {
    fn warmup(&self) {
        self.0.warmups.fetch_add(1, Ordering::Relaxed);
    }

    fn sent(&self, tx: &ControlTx, timestamp: SystemTime) {
        let mut log = self.0.log.lock().unwrap();
        match tx {
            ControlTx::Cmd { cmd, retry } => log.commands.push((cmd.command, *retry)),
            ControlTx::Ack(_) => log.acks_sent += 1,
        }
        log.stamps.push(timestamp);
    }

    fn received(&self, rx: &ControlRx, timestamp: SystemTime) {
        let mut log = self.0.log.lock().unwrap();
        match rx {
            ControlRx::Ack(ack) => log.acks.push((ack.answer, ack.ack_id)),
            ControlRx::Cmd(cmd) => log.events.push(cmd.command),
        }
        log.stamps.push(timestamp);
    }
}

#[test]
fn control_sink_sees_transactions_and_events() {
    sim(1).run(|| {
        let started = SystemTime::now();
        let fake = FakeCamera::start();
        let recorder = Arc::new(ControlRecorder::default());
        let cam = connect(&fake, Some(ControlObserver(recorder.clone())));

        cam.write_register(bootstrap::HEARTBEAT_TIMEOUT, 4000)
            .expect("queue write")
            .wait_timeout(Duration::from_secs(1))
            .expect("write acked");

        cam.enable_events().expect("enable events");
        assert!(fake.send_event(1, 0x9001, 42), "fake camera sent an event");

        assert_eq!(
            recorder.warmups.load(Ordering::Relaxed),
            1,
            "warmup fired once on the worker thread"
        );

        let log = recorder.log.lock().unwrap();
        assert!(
            log.commands
                .iter()
                .any(|&(c, retry)| c == telegenic::gige::proto::gvcp::WRITE_REGISTER_CMD && !retry),
            "the write register command reached the sink (saw {:?})",
            log.commands
        );
        assert!(
            log.commands
                .iter()
                .any(|&(c, _)| c == telegenic::gige::proto::gvcp::READ_MEMORY_CMD),
            "connect's identity reads reached the sink"
        );
        assert!(
            log.acks
                .iter()
                .any(|&(answer, _)| answer == telegenic::gige::proto::gvcp::WRITE_REGISTER_ACK),
            "the matching acknowledge reached the sink"
        );
        assert_eq!(log.events, vec![telegenic::gige::proto::gvcp::EVENT_CMD]);
        assert_eq!(log.acks_sent, 1, "we acked the event that asked for one");

        // Every acknowledge the sink saw pairs with a command id we sent, so a
        // recording is enough to rebuild the transaction sequence.
        assert!(!log.acks.is_empty());
        let now = SystemTime::now();
        for stamp in &log.stamps {
            assert!(
                *stamp >= started && *stamp <= now,
                "stamped between the test's start and now"
            );
        }
    });
}

#[derive(Default)]
struct StreamLog {
    leaders: usize,
    payload_bytes: usize,
    payload_packets: usize,
    trailers: usize,
    frame_ids: Vec<u64>,
    resend_requests: usize,
}

#[derive(Default)]
struct StreamRecorder {
    warmups: AtomicUsize,
    log: Mutex<StreamLog>,
}

struct StreamObserver(Arc<StreamRecorder>);

impl TelemetrySink<GvcpCmd, GvspPacket> for StreamObserver {
    fn warmup(&self) {
        self.0.warmups.fetch_add(1, Ordering::Relaxed);
    }

    fn sent(&self, _tx: &GvcpCmd, _timestamp: SystemTime) {
        self.0.log.lock().unwrap().resend_requests += 1;
    }

    fn received(&self, rx: &GvspPacket, _timestamp: SystemTime) {
        let mut log = self.0.log.lock().unwrap();
        if !log.frame_ids.contains(&rx.frame_id) {
            log.frame_ids.push(rx.frame_id);
        }
        match rx.content_type {
            ContentType::Leader => log.leaders += 1,
            ContentType::Trailer => log.trailers += 1,
            ContentType::Payload => {
                log.payload_packets += 1;
                log.payload_bytes += rx.data.len();
            }
            _ => {}
        }
    }
}

#[test]
fn stream_sink_sees_every_gvsp_datagram() {
    sim(2).run(|| {
        let fake = FakeCamera::start();
        let cam = connect(&fake, None);

        let mut cfg = StreamConfig::new();
        cfg.payload_size = Some(2000);
        cfg.packet_size = PacketSize::Fixed((BLOCK + 36) as u16);

        let recorder = Arc::new(StreamRecorder::default());
        let stream = cam
            .open_stream_with_telemetry(cfg, StreamObserver(recorder.clone()))
            .expect("open stream");
        let frames = stream.subscribe(4);

        let payload: Vec<u8> = (0..2000).map(|i| (i * 31 % 251) as u8).collect();
        fake.send_gvsp_frame(1, &payload, &FrameOpts::new(BLOCK));
        let frame = frames.wait_for(Duration::from_secs(1)).expect("frame");
        assert_eq!(frame.data(), &payload[..]);

        assert_eq!(recorder.warmups.load(Ordering::Relaxed), 1);

        let log = recorder.log.lock().unwrap();
        assert_eq!(log.leaders, 1, "the frame's leader reached the sink");
        assert_eq!(log.trailers, 1, "the frame's trailer reached the sink");
        assert_eq!(log.frame_ids, vec![1]);
        assert_eq!(
            log.payload_bytes,
            payload.len(),
            "the sink's payload bodies concatenate to the whole image"
        );
        assert_eq!(log.payload_packets, payload.len().div_ceil(BLOCK));
        assert_eq!(log.resend_requests, 0, "a clean frame needs no resends");
    });
}
