//! The GVSP stream receiver inside a snare simulation: packet loss and
//! resends, reordering and duplication, block-id wraparound, packet-size
//! negotiation over a narrow path, kernel receive timestamps, receive-buffer
//! sizing and a camera vanishing mid-frame, against an emulated camera.

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

mod common;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use common::{
    ACQ_REG, Camera, CameraSpec, EXPOSURE, HOST_IP, SIM_OVERHEAD, assert_costs, config, sim,
};
use telegenic::emulator::ResendRequest;
use telegenic::gige::GigECamera;
use telegenic::gige::proto::gvsp::ContentType;
use telegenic::wire::{ControlRx, ControlTx, GvcpCmd, GvspPacket, TelemetrySink};
use telegenic::{
    CameraError, Frame, FrameChannel, FrameStatus, PacketSize, ResendPolicy, SocketOption,
    StreamChannel, StreamConfig,
};

/// 576-byte packets carry 540 data bytes: a 64x48 frame is a leader, six
/// payload packets (ids 1..=6) and a trailer (id 7).
const PACKET: u16 = 576;
const BLOCK: usize = 540;
const PACKET_TIMEOUT: Duration = Duration::from_millis(20);
const FRAME_RETENTION: Duration = Duration::from_millis(100);
const STREAM_ADDR: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(HOST_IP), 50_010);

fn stream_config(spec: &CameraSpec) -> StreamConfig {
    let mut s = StreamConfig::new();
    s.payload_size = Some((spec.width * spec.height) as usize);
    s.packet_size = PacketSize::Fixed(PACKET);
    s
}

struct Rig {
    device: Camera,
    cam: GigECamera,
    stream: StreamChannel,
    frames: FrameChannel,
}

impl Rig {
    fn new(spec: CameraSpec, cfg: StreamConfig) -> Self {
        let device = Camera::with(spec);
        let mut cam = GigECamera::with_config(config(2));
        cam.connect().expect("connect");
        let stream = cam.open_stream(cfg).expect("open stream");
        let frames = stream.subscribe(8);
        Self {
            device,
            cam,
            stream,
            frames,
        }
    }

    fn start(spec: CameraSpec) -> Self {
        let cfg = stream_config(&spec);
        Self::new(spec, cfg)
    }

    /// Triggers one acquisition and waits for the frame it closes.
    fn snap(&self) -> (Arc<Frame>, Duration) {
        let t0 = Instant::now();
        self.cam.write_register(ACQ_REG, 1).unwrap().wait().unwrap();
        let frame = self
            .frames
            .wait_for(Duration::from_secs(2))
            .expect("a frame closes");
        (frame, t0.elapsed())
    }

    fn stop(self) {
        drop(self.frames);
        drop(self.stream);
        drop(self.cam);
        self.device.stop();
    }
}

/// One payload packet lost on the wire: the hole is requested once, a
/// packet timeout after the burst, and the resent packet completes the
/// frame.
#[test]
fn a_lost_packet_is_recovered_by_one_resend_request() {
    sim(40).run(|| {
        let rig = Rig::start(CameraSpec {
            packet_resend: true,
            ..CameraSpec::default()
        });
        rig.device.faults(|f| f.gvsp_lost = vec![3]);
        let (frame, took) = rig.snap();
        assert_costs(
            took,
            EXPOSURE + PACKET_TIMEOUT,
            "exposure, then one packet timeout",
        );
        assert_eq!(frame.status, FrameStatus::Complete);
        assert_eq!(frame.data(), rig.device.image());
        assert_eq!(
            rig.device.log().resends,
            [ResendRequest {
                frame_id: 1,
                first_packet: 3,
                last_packet: 3
            }]
        );
        let s = rig.stream.stats();
        assert_eq!(
            (
                s.completed_frames,
                s.resend_requests,
                s.resent_packets,
                s.missing_packets
            ),
            (1, 1, 1, 0)
        );
        assert_eq!(rig.cam.link_stats().frames_incomplete, 0);
        rig.stop();
    });
}

/// A device without packet resend gets no resend requests; a frame with a
/// hole closes as timed out once the retention window passes, and the next
/// frame is unaffected.
#[test]
fn a_hole_the_device_cannot_resend_times_the_frame_out() {
    sim(41).run(|| {
        let rig = Rig::start(CameraSpec::default());
        rig.device.faults(|f| f.gvsp_lost = vec![3]);
        let (frame, took) = rig.snap();
        assert_costs(
            took,
            EXPOSURE + FRAME_RETENTION,
            "exposure, then the retention window",
        );
        assert_eq!(frame.status, FrameStatus::Timeout);
        assert_eq!(frame.received_size, 64 * 48 - BLOCK);
        assert!(rig.device.log().resends.is_empty());
        let s = rig.stream.stats();
        assert_eq!(
            (
                s.timed_out_frames,
                s.failed_frames,
                s.resend_requests,
                s.missing_packets
            ),
            (1, 1, 0, 5)
        );
        assert_eq!(rig.cam.link_stats().frames_timed_out, 1);
        drop(frame);

        let (frame, took) = rig.snap();
        assert_costs(took, EXPOSURE, "a clean frame after the failed one");
        assert_eq!((frame.frame_id, frame.status), (2, FrameStatus::Complete));
        rig.stop();
    });
}

/// Resend requests the device never answers repeat every packet timeout
/// until the per-frame request budget (a quarter of its 8 packets) is
/// spent; then the frame waits out its retention window.
#[test]
fn unanswered_resends_stop_at_the_request_budget() {
    sim(42).run(|| {
        let rig = Rig::start(CameraSpec {
            packet_resend: true,
            ..CameraSpec::default()
        });
        rig.device.faults(|f| {
            f.gvsp_lost = vec![3];
            f.ignore_resends = true;
        });
        let (frame, took) = rig.snap();
        assert_costs(took, EXPOSURE + FRAME_RETENTION, "the retention window");
        assert_eq!(frame.status, FrameStatus::Timeout);
        let request = ResendRequest {
            frame_id: 1,
            first_packet: 3,
            last_packet: 3,
        };
        assert_eq!(rig.device.log().resends, [request, request]);
        let s = rig.stream.stats();
        assert_eq!(
            (s.resend_requests, s.resend_ratio_reached, s.resent_packets),
            (2, 1, 0)
        );
        rig.stop();
    });
}

/// Packets out of order and repeated within one burst reassemble with no
/// resend request; repeats are counted, and copies arriving after the frame
/// closed are dropped as late.
#[test]
fn reordered_and_duplicated_packets_reassemble_without_resends() {
    sim(43).run(|| {
        let rig = Rig::start(CameraSpec {
            packet_resend: true,
            ..CameraSpec::default()
        });
        rig.device
            .faults(|f| f.gvsp_order = Some(vec![0, 3, 1, 2, 2, 6, 5, 4, 7, 7, 1]));
        let (frame, took) = rig.snap();
        assert_costs(took, EXPOSURE, "reordering inside a burst costs nothing");
        assert_eq!(frame.status, FrameStatus::Complete);
        assert_eq!(frame.data(), rig.device.image());
        std::thread::sleep(Duration::from_millis(1));
        let s = rig.stream.stats();
        assert_eq!(
            (
                s.packets,
                s.duplicated_packets,
                s.ignored_packets,
                s.resend_requests
            ),
            (11, 1, 2, 0)
        );
        rig.stop();
    });
}

fn jittery_run(seed: u64) -> (Vec<(u64, FrameStatus)>, u64, u64) {
    sim(seed).run(|| {
        let rig = Rig::start(CameraSpec {
            packet_resend: true,
            ..CameraSpec::default()
        });
        snare::set_udp_policy(rig.stream.local_addr(), |p| {
            p.jitter = Duration::from_millis(3);
            p.duplicate_rate = 0.2;
        });
        let mut out = Vec::new();
        for _ in 0..5 {
            let (frame, _) = rig.snap();
            assert_eq!(frame.data(), rig.device.image(), "frame {}", frame.frame_id);
            out.push((frame.frame_id, frame.status));
        }
        let s = rig.stream.stats();
        rig.stop();
        (out, s.duplicated_packets, s.resend_requests)
    })
}

/// A link that jitters by up to 3 ms and duplicates a fifth of the packets:
/// every frame still completes intact, the duplicates are absorbed, and the
/// run replays exactly under its seed.
#[test]
fn a_jittery_duplicating_link_still_delivers_every_frame() {
    let first = jittery_run(44);
    let ids: Vec<_> = first.0.iter().map(|f| f.0).collect();
    assert_eq!(ids, [1, 2, 3, 4, 5]);
    assert!(
        first.0.iter().all(|f| f.1 == FrameStatus::Complete),
        "{first:?}"
    );
    assert!(first.1 > 0, "no duplicate reached the receiver: {first:?}");
    assert_eq!(jittery_run(44), first);
}

/// Block ids wrap from 0xffff to 1 without counting a missing frame; a
/// block id the device skips is counted once.
#[test]
fn block_ids_wrap_and_a_skipped_id_counts_one_missing_frame() {
    sim(45).run(|| {
        let rig = Rig::start(CameraSpec {
            first_frame_id: 0xfffe,
            ..CameraSpec::default()
        });
        let mut ids = Vec::new();
        for skip in [0, 0, 0, 1] {
            rig.device.faults(|f| f.skip_frame_ids = skip);
            let (frame, _) = rig.snap();
            assert_eq!(frame.status, FrameStatus::Complete);
            ids.push(frame.frame_id);
        }
        assert_eq!(ids, [0xfffe, 0xffff, 1, 3]);
        let s = rig.stream.stats();
        assert_eq!((s.completed_frames, s.missing_frames), (4, 1));
        rig.stop();
    });
}

/// Fire-test negotiation over a path that drops datagrams above a 1500-byte
/// MTU: the jumbo probe and every bisection step above 1500 go unanswered
/// (75 ms each), 1488 is the largest 16-aligned size that gets through, and
/// frames then stream at it.
#[test]
fn packet_size_negotiation_settles_below_a_1500_byte_path() {
    sim(46).run(|| {
        let spec = CameraSpec::default();
        let device = Camera::with(spec.clone());
        let mut cam = GigECamera::with_config(config(2));
        cam.connect().expect("connect");
        snare::set_udp_policy(STREAM_ADDR, |p| p.mtu = Some(1500 - 28));
        let mut cfg = stream_config(&spec);
        cfg.packet_size = PacketSize::Auto;
        cfg.local_addr = Some(STREAM_ADDR);
        let t0 = Instant::now();
        let stream = cam.open_stream(cfg).expect("open stream");
        assert_costs(
            t0.elapsed(),
            9 * Duration::from_millis(75) + Duration::from_millis(5),
            "nine unanswered probes and the final drain",
        );
        assert_eq!(stream.packet_size(), 1488);
        assert_eq!(
            device.device(
                |d| d.read_reg(telegenic::gige::proto::bootstrap::STREAM_CHANNEL_PACKET_SIZE)
            ),
            1488
        );
        let frames = stream.subscribe(1);
        cam.write_register(ACQ_REG, 1).unwrap().wait().unwrap();
        let frame = frames.wait_for(Duration::from_secs(1)).expect("frame");
        assert_eq!(frame.status, FrameStatus::Complete);
        assert_eq!(frame.data(), device.image());
        assert_eq!(
            stream.stats().packets,
            2 + (64 * 48usize).div_ceil(1488 - 36) as u64
        );
        drop(frames);
        drop(stream);
        drop(cam);
        device.stop();
    });
}

/// A device whose packet buffer tops out below the jumbo probe answers it
/// with a smaller test packet; the size it delivered is the one the
/// channel must report and stream at.
#[test]
fn a_device_capping_the_jumbo_probe_negotiates_its_capped_size() {
    sim(47).run(|| {
        let spec = CameraSpec {
            max_packet_size: Some(8000),
            ..CameraSpec::default()
        };
        let mut cfg = stream_config(&spec);
        cfg.packet_size = PacketSize::Auto;
        let rig = Rig::new(spec, cfg);
        let (frame, _) = rig.snap();
        assert_eq!(frame.status, FrameStatus::Complete);
        assert_eq!(
            rig.stream.packet_size(),
            8000,
            "reported size differs from the device's"
        );
        rig.stop();
    });
}

#[derive(Default)]
struct Stamps {
    control: Vec<(u16, SystemTime)>,
    stream: Vec<(ContentType, u32, SystemTime)>,
}

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Stamps>>);

impl TelemetrySink<ControlTx, ControlRx> for Recorder {
    fn sent(&self, _: &ControlTx, _: SystemTime) {}
    fn received(&self, rx: &ControlRx, at: SystemTime) {
        if let ControlRx::Ack(ack) = rx {
            self.0.lock().unwrap().control.push((ack.ack_id, at));
        }
    }
}

impl TelemetrySink<GvcpCmd, GvspPacket> for Recorder {
    fn sent(&self, _: &GvcpCmd, _: SystemTime) {}
    fn received(&self, rx: &GvspPacket, at: SystemTime) {
        self.0
            .lock()
            .unwrap()
            .stream
            .push((rx.content_type, rx.packet_id, at));
    }
}

/// The resolution the host's receive timestamps carry: `SO_TIMESTAMP`'s
/// microseconds off Linux.
const STAMP_RESOLUTION: Duration = if cfg!(target_os = "linux") {
    Duration::from_nanos(1)
} else {
    Duration::from_micros(1)
};

#[track_caller]
fn assert_stamp(stamp: SystemTime, sent: SystemTime, latency: Duration, what: &str) {
    let flight = stamp
        .duration_since(sent)
        .expect("stamped before it was sent");
    assert!(
        flight + STAMP_RESOLUTION > latency && flight < latency + SIM_OVERHEAD,
        "{what}: stamped {flight:?} after sending, link latency {latency:?}"
    );
}

/// With a fixed link latency each way, the kernel receive stamps the sinks
/// see are the device's send instant plus that latency, for acknowledges
/// and GVSP packets alike, and a frame's leader timestamp sits one exposure
/// before its burst.
#[test]
fn kernel_rx_stamps_are_send_time_plus_link_latency() {
    const CONTROL_LATENCY: Duration = Duration::from_millis(3);
    const STREAM_LATENCY: Duration = Duration::from_millis(2);
    const CONTROL_ADDR: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(HOST_IP), 40_000);
    sim(48).run(|| {
        let spec = CameraSpec::default();
        let device = Camera::with(spec.clone());
        let recorder = Recorder::default();
        snare::set_udp_policy(CONTROL_ADDR, |p| p.latency = CONTROL_LATENCY);
        snare::set_udp_policy(STREAM_ADDR, |p| p.latency = STREAM_LATENCY);
        let mut cfg = config(2);
        cfg.local_addr = Some(CONTROL_ADDR);
        let mut cam = GigECamera::with_config(cfg);
        cam.set_telemetry(recorder.clone());
        cam.connect().expect("connect");
        let mut s = stream_config(&spec);
        s.local_addr = Some(STREAM_ADDR);
        let stream = cam
            .open_stream_with_telemetry(s, recorder.clone())
            .expect("open stream");
        let frames = stream.subscribe(1);
        let t0 = Instant::now();
        cam.write_register(ACQ_REG, 1).unwrap().wait().unwrap();
        assert_costs(t0.elapsed(), CONTROL_LATENCY, "one ack in flight");
        let frame = frames.wait_for(Duration::from_secs(1)).expect("frame");
        assert_eq!(frame.status, FrameStatus::Complete);

        let log = device.log();
        let stamps = recorder.0.lock().unwrap();
        assert!(stamps.control.len() > 10);
        for (ack_id, at) in &stamps.control {
            let cmd = log
                .commands
                .iter()
                .rev()
                .find(|c| c.req_id == *ack_id)
                .expect("an ack answers a logged command");
            assert_stamp(*at, cmd.sys, CONTROL_LATENCY, "ack");
        }
        let burst = *log.bursts.last().unwrap();
        assert_eq!(stamps.stream.len(), 8);
        for (kind, id, at) in &stamps.stream {
            assert_stamp(*at, burst, STREAM_LATENCY, &format!("{kind:?} {id}"));
        }
        let leader_at = stamps.stream[0].2;
        assert_eq!(stamps.stream[0].0, ContentType::Leader);
        let acquired = SystemTime::UNIX_EPOCH + Duration::from_nanos(frame.timestamp_ns);
        assert_stamp(
            leader_at,
            acquired,
            EXPOSURE + STREAM_LATENCY,
            "leader stamp after the acquisition instant",
        );
        let decoded = SystemTime::UNIX_EPOCH + Duration::from_nanos(frame.system_timestamp_ns);
        assert!(
            decoded + STAMP_RESOLUTION > leader_at,
            "decoded before the kernel stamp"
        );
        drop(stamps);
        drop(frames);
        drop(stream);
        drop(cam);
        device.stop();
    });
}

const FULL_WIDTH: u32 = 1280;
const FULL_HEIGHT: u32 = 1024;

fn full_frame(stream_socket: Vec<SocketOption>) -> (FrameStatus, u64, u64) {
    sim(49).run(|| {
        let spec = CameraSpec {
            width: FULL_WIDTH,
            height: FULL_HEIGHT,
            ..CameraSpec::default()
        };
        let mut cfg = stream_config(&spec);
        cfg.packet_size = PacketSize::Fixed(1500);
        cfg.resend = ResendPolicy::Never;
        cfg.stream_socket = stream_socket;
        let rig = Rig::new(spec, cfg);
        let (frame, _) = rig.snap();
        let status = frame.status;
        let overflowed = snare::sockets_bound(rig.stream.local_addr())[0].overflowed;
        let packets = rig.stream.stats().packets;
        drop(frame);
        rig.stop();
        (status, overflowed, packets)
    })
}

/// A full-resolution frame arrives as one 900-packet burst before the
/// worker can read any of it: the default 8 MiB receive buffer holds it
/// all, even alongside other socket options, and a 64 KiB one overflows and
/// loses the frame.
#[test]
fn the_default_receive_buffer_holds_a_full_frame_burst() {
    let packets = 2 + (FULL_WIDTH * FULL_HEIGHT).div_ceil(1500 - 36) as u64;
    for stream_socket in [
        StreamConfig::new().stream_socket,
        vec![SocketOption::LinuxBusyPoll(50)],
    ] {
        let what = format!("{stream_socket:?}");
        let (status, overflowed, received) = full_frame(stream_socket);
        assert_eq!(
            (status, overflowed, received),
            (FrameStatus::Complete, 0, packets),
            "{what}"
        );
    }

    let (status, overflowed, received) = full_frame(vec![SocketOption::RecvBuffer(64 * 1024)]);
    assert_ne!(status, FrameStatus::Complete);
    assert!(overflowed > 0);
    assert_eq!(received + overflowed, packets);
}

/// The camera dies three packets into a frame: the frame closes as timed
/// out after the retention window, the heartbeat then declares control
/// lost, and closing the stream does not wait on the dead control channel.
#[test]
fn a_camera_vanishing_mid_frame_times_the_frame_out_and_loses_control() {
    sim(50).run(|| {
        let rig = Rig::start(CameraSpec::default());
        let (frame, _) = rig.snap();
        assert_eq!(frame.status, FrameStatus::Complete);
        rig.device.faults(|f| f.vanish_after_packets = Some(3));
        let (frame, took) = rig.snap();
        assert_costs(took, EXPOSURE + FRAME_RETENTION, "the retention window");
        assert_eq!((frame.frame_id, frame.status), (2, FrameStatus::Timeout));
        assert_eq!(frame.received_size, 2 * BLOCK);
        assert_eq!(rig.cam.link_stats().frames_timed_out, 1);

        let end = Instant::now() + Duration::from_secs(5);
        while rig.cam.is_connected() {
            assert!(Instant::now() < end, "control never dropped");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(matches!(
            rig.cam.write_register(ACQ_REG, 1),
            Err(CameraError::ControlLost)
        ));
        assert!(rig.stream.is_running());
        let t0 = Instant::now();
        let Rig {
            device,
            cam,
            stream,
            frames,
        } = rig;
        drop(frames);
        drop(stream);
        assert_costs(t0.elapsed(), Duration::ZERO, "closing the stream");
        drop(cam);
        device.stop();
    });
}
