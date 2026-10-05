//! Integration tests for the GVSP stream receiver against the fake camera,
//! each inside a deterministic snare simulation.
//!
//! The fake sends synthetic Mono8 frames; tests use a fixed SCPS of
//! `block + 36` (standard ids) / `block + 48` (extended) so the receiver's
//! block-size math matches the generator's.

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

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use fake_camera::{FAKE_TIMESTAMP_TICKS, FakeCamera, FrameOpts, sim};
use telegenic::gige::{GigECamera, GigeConfig};
use telegenic::{FrameStatus, PacketSize, PayloadKind, PixelFormat, StreamConfig};

const BLOCK: usize = 500;
/// Long enough for the stream worker to drain whatever the fake just sent.
const SETTLE: Duration = Duration::from_millis(1);

fn connect(fake: &FakeCamera) -> GigECamera {
    let mut cfg = GigeConfig::new(std::net::Ipv4Addr::LOCALHOST);
    cfg.addr = fake.addr();
    cfg.local_addr = Some(fake_camera::LOOPBACK_ANY_PORT);
    cfg.gvcp_timeout = Duration::from_millis(500);
    cfg.retries = 2;
    let mut cam = GigECamera::with_config(cfg);
    cam.connect().expect("connect to fake camera");
    cam
}

fn stream_config(payload_size: usize, extended: bool) -> StreamConfig {
    let mut cfg = StreamConfig::new();
    cfg.payload_size = Some(payload_size);
    cfg.packet_size = PacketSize::Fixed((BLOCK + if extended { 48 } else { 36 }) as u16);
    cfg
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 % 251) as u8).collect()
}

#[test]
fn clean_frame_reassembles() {
    sim(1).run(|| {
        let fake = FakeCamera::start();
        let cam = connect(&fake);
        let stream = cam
            .open_stream(stream_config(2000, false))
            .expect("open stream");
        let frames = stream.subscribe(4);

        let payload = pattern(2000);
        fake.send_gvsp_frame(1, &payload, &FrameOpts::new(BLOCK));

        let frame = frames.wait_for(Duration::from_secs(1)).expect("frame");
        assert_eq!(frame.status, FrameStatus::Complete);
        assert_eq!(frame.frame_id, 1);
        assert_eq!(frame.payload, PayloadKind::Image { has_chunks: false });
        assert_eq!(frame.pixel_format, PixelFormat::MONO8);
        assert_eq!(frame.width, 2000);
        assert_eq!(frame.height, 1);
        assert_eq!(frame.received_size, 2000);
        assert_eq!(frame.data(), &payload[..]);
        assert_eq!(frame.timestamp_ticks, FAKE_TIMESTAMP_TICKS);

        let stats = stream.stats();
        assert_eq!(stats.completed_frames, 1);
        assert_eq!(stats.resend_requests, 0);
    });
}

#[test]
fn extended_ids_reassemble() {
    sim(2).run(|| {
        let fake = FakeCamera::start();
        let cam = connect(&fake);
        let stream = cam
            .open_stream(stream_config(1500, true))
            .expect("open stream");
        let frames = stream.subscribe(4);

        let payload = pattern(1500);
        let mut opts = FrameOpts::new(BLOCK);
        opts.extended_ids = true;
        fake.send_gvsp_frame(0x1_0000_0001, &payload, &opts);

        let frame = frames.wait_for(Duration::from_secs(1)).expect("frame");
        assert_eq!(frame.status, FrameStatus::Complete);
        assert_eq!(frame.frame_id, 0x1_0000_0001);
        assert_eq!(frame.data(), &payload[..]);
    });
}

/// The whole burst reversed, so the trailer opens the frame and the leader
/// arrives last, with two packets repeated.
#[test]
fn out_of_order_and_duplicates() {
    sim(3).run(|| {
        let fake = FakeCamera::start();
        let cam = connect(&fake);
        let stream = cam
            .open_stream(stream_config(2000, false))
            .expect("open stream");
        let frames = stream.subscribe(4);

        let payload = pattern(2000);
        let mut opts = FrameOpts::new(BLOCK);
        opts.reverse = true;
        opts.duplicate = vec![2, 3];
        fake.send_gvsp_frame(7, &payload, &opts);

        let frame = frames.wait_for(Duration::from_secs(1)).expect("frame");
        assert_eq!(frame.status, FrameStatus::Complete);
        assert_eq!(frame.data(), &payload[..]);
        let stats = stream.stats();
        assert_eq!(stats.duplicated_packets, 2);
        assert_eq!(stats.resend_requests, 0);
    });
}

/// Three packets lost in two runs: each run is requested once, as one
/// ranged resend, a packet timeout after the burst; the replayed packets
/// complete the frame.
#[test]
fn missing_packets_are_resent() {
    sim(4).run(|| {
        let fake = FakeCamera::start();
        let cam = connect(&fake);
        let mut cfg = stream_config(5000, false);
        cfg.packet_request_ratio = 0.5;
        let packet_timeout = cfg.packet_timeout;
        let stream = cam.open_stream(cfg).expect("open stream");
        let frames = stream.subscribe(4);

        let payload = pattern(5000); // 10 payload packets + leader + trailer
        let mut opts = FrameOpts::new(BLOCK);
        opts.drop = vec![3, 4, 8];
        let t0 = Instant::now();
        fake.send_gvsp_frame(3, &payload, &opts);

        let frame = frames.wait_for(Duration::from_secs(2)).expect("frame");
        let took = t0.elapsed();
        assert_eq!(frame.status, FrameStatus::Complete);
        assert_eq!(frame.data(), &payload[..]);
        assert!(
            took >= packet_timeout && took < packet_timeout + SETTLE,
            "one packet timeout to the resend, took {took:?}"
        );

        let stats = stream.stats();
        assert_eq!((stats.resend_requests, stats.resent_packets), (3, 3));
        assert_eq!(
            fake.counters.resend_requests.load(Ordering::Relaxed),
            2,
            "one request per run of missing packets"
        );
    });
}

#[test]
fn unanswered_holes_time_out() {
    sim(5).run(|| {
        let fake = FakeCamera::start();
        let cam = connect(&fake);
        let mut cfg = stream_config(2000, false);
        cfg.frame_retention = Duration::from_millis(80);
        let stream = cam.open_stream(cfg).expect("open stream");
        let frames = stream.subscribe(4);

        fake.knobs().lock().resend_replay = false;
        let mut opts = FrameOpts::new(BLOCK);
        opts.drop = vec![2];
        let t0 = Instant::now();
        fake.send_gvsp_frame(5, &pattern(2000), &opts);

        let frame = frames.wait_for(Duration::from_secs(1)).expect("frame");
        let took = t0.elapsed();
        assert_eq!(frame.status, FrameStatus::Timeout);
        assert!(
            took >= Duration::from_millis(80) && took < Duration::from_millis(80) + SETTLE,
            "closed when the retention window ran out, took {took:?}"
        );
        // The data extends to the last received packet — the hole does not cut
        // off the tail — while received_size reflects the missing block.
        assert_eq!(frame.data().len(), 2000);
        assert_eq!(frame.received_size, 1500);
        let stats = stream.stats();
        assert_eq!(stats.timed_out_frames, 1);
        assert!(stats.missing_packets >= 1);
    });
}

#[test]
fn early_trailer_shrinks_the_frame() {
    sim(6).run(|| {
        let fake = FakeCamera::start();
        let cam = connect(&fake);
        // Buffer sized for 4000 bytes, actual payload only 1000.
        let stream = cam
            .open_stream(stream_config(4000, false))
            .expect("open stream");
        let frames = stream.subscribe(4);

        let payload = pattern(1000);
        fake.send_gvsp_frame(9, &payload, &FrameOpts::new(BLOCK));

        let frame = frames.wait_for(Duration::from_secs(1)).expect("frame");
        assert_eq!(frame.status, FrameStatus::Complete);
        assert_eq!(frame.received_size, 1000);
        assert_eq!(frame.data(), &payload[..]);
    });
}

#[test]
fn pool_exhaustion_counts_underruns() {
    sim(7).run(|| {
        let fake = FakeCamera::start();
        let cam = connect(&fake);
        let mut cfg = stream_config(1000, false);
        cfg.n_buffers = 1;
        let stream = cam.open_stream(cfg).expect("open stream");
        let frames = stream.subscribe(8);

        // First frame completes and sits undelivered in the channel, holding the
        // only buffer; the second frame finds the pool empty.
        fake.send_gvsp_frame(1, &pattern(1000), &FrameOpts::new(BLOCK));
        std::thread::sleep(SETTLE);
        assert_eq!(stream.stats().completed_frames, 1);
        fake.send_gvsp_frame(2, &pattern(1000), &FrameOpts::new(BLOCK));
        std::thread::sleep(SETTLE);
        let stats = stream.stats();
        assert_eq!((stats.completed_frames, stats.underruns), (1, 1));

        // Consuming (and dropping) the frame returns the buffer; streaming
        // recovers.
        drop(frames.recv_all());
        fake.send_gvsp_frame(3, &pattern(1000), &FrameOpts::new(BLOCK));
        let frame = frames
            .wait_for(Duration::from_secs(1))
            .expect("frame after recovery");
        assert_eq!(frame.frame_id, 3);
    });
}
