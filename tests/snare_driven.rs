//! The driver inside a deterministic snare simulation against an emulated
//! camera: every wait the driver makes is seen by the simulation and every
//! timeout runs on its virtual clock. A device that answers at once costs
//! next to no virtual time; what passes is the time the device itself takes
//! and the driver's own deliberate waits.

#![cfg(unix)]

mod common;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use common::{
    Camera, DEVICE_IP, DEVICE_MAC, EXPOSURE, HOST_IP, assert_costs, config, image, sim, unix_ns,
};
use snare::{NicSpec, Sim};
use telegenic::gige::GigECamera;
use telegenic::gige::discovery::{self, DiscoveryConfig};
use telegenic::gige::proto::gvcp::GVCP_PORT;
use telegenic::{CameraError, FrameStatus, GenICamera, PacketSize, StreamConfig};

const WIDTH: u32 = common::WIDTH;
const HEIGHT: u32 = common::HEIGHT;
const BLACKHOLE_REG: u32 = 0x2020;
const NEGOTIATION_DRAIN: Duration = Duration::from_millis(5);

/// Runs `body` against a fresh device on a deterministic sim seeded `seed`.
/// The device never answers a write to [`BLACKHOLE_REG`].
fn driven<T>(seed: u64, body: impl FnOnce() -> T) -> T {
    sim(seed).run(|| {
        let device = Camera::spawn();
        device.faults(|f| {
            f.ignore.insert(BLACKHOLE_REG, usize::MAX);
        });
        let out = body();
        device.stop();
        out
    })
}

#[derive(Debug, PartialEq, Eq)]
struct SnapRun {
    connect_and_open: Duration,
    snap: Duration,
    disconnect: Duration,
    frame_id: u64,
    frame_bytes: Vec<u8>,
    timestamp_after_start_ns: u64,
    received_after_acquisition_ns: u64,
    retransmits: u64,
    control_timeouts: u64,
    frames_incomplete: u64,
    frames_timed_out: u64,
    still_connected: bool,
}

fn snap_run() -> SnapRun {
    let start_ns = unix_ns();
    let t0 = Instant::now();
    let mut cam = GenICamera::from_transport(GigECamera::with_config(config(4)));
    cam.connect().expect("connect");
    let mut stream = StreamConfig::new();
    stream.packet_size = PacketSize::Auto;
    let mut session = cam.snapshot_session(stream).expect("snapshot session");
    let t1 = Instant::now();
    let frame = session.snap(Duration::from_secs(15)).expect("snap");
    let t2 = Instant::now();
    drop(session);
    let control_timeouts = cam.transport().stats().map_or(0, |s| s.timeouts);
    let link = cam.link_stats();
    let t3 = Instant::now();
    cam.disconnect(Duration::from_secs(1));
    let t4 = Instant::now();
    assert_eq!(frame.status, FrameStatus::Complete);
    SnapRun {
        connect_and_open: t1 - t0,
        snap: t2 - t1,
        disconnect: t4 - t3,
        frame_id: frame.frame_id,
        frame_bytes: frame.data().to_vec(),
        timestamp_after_start_ns: frame.timestamp_ns - start_ns,
        received_after_acquisition_ns: frame.system_timestamp_ns - frame.timestamp_ns,
        retransmits: link.gvcp_retransmits,
        control_timeouts,
        frames_incomplete: link.frames_incomplete,
        frames_timed_out: link.frames_timed_out,
        still_connected: cam.is_connected(),
    }
}

#[test]
fn a_snap_costs_only_the_exposure_and_no_timeout() {
    let run = driven(11, snap_run);
    assert_costs(
        run.snap,
        EXPOSURE,
        "snap must take the device's exposure only",
    );
    assert_eq!(run.frame_id, 1);
    assert_eq!(run.frame_bytes, image(WIDTH, HEIGHT));
    assert_costs(
        Duration::from_nanos(run.received_after_acquisition_ns),
        EXPOSURE,
        "the leader arrives one exposure after the acquisition it carries",
    );
    assert_costs(
        run.connect_and_open,
        NEGOTIATION_DRAIN,
        "connecting costs only the packet-size negotiation's final drain",
    );
    assert_costs(
        Duration::from_nanos(run.timestamp_after_start_ns),
        NEGOTIATION_DRAIN,
        "acquisition starts right after the negotiation",
    );
    assert_eq!(run.retransmits, 0);
    assert_eq!(run.control_timeouts, 0);
    assert_eq!(run.frames_incomplete, 0);
    assert_eq!(run.frames_timed_out, 0);
    assert_eq!(run.disconnect, Duration::ZERO);
    assert!(!run.still_connected);
}

#[test]
fn an_unanswered_write_times_out_on_virtual_time() {
    let wall = snare::real(Instant::now);
    let (first, second, retries, retransmits) = driven(12, || {
        let mut cam = GigECamera::with_config(config(2));
        cam.connect().expect("connect");
        let handle = cam.write_register(BLACKHOLE_REG, 1).unwrap();
        let t0 = Instant::now();
        let err = handle.wait_timeout(Duration::from_millis(500)).unwrap_err();
        assert!(matches!(*err, CameraError::Timeout));
        let first = Instant::now() - t0;
        let err = handle.wait().unwrap_err();
        assert!(matches!(*err, CameraError::Timeout));
        let second = Instant::now() - t0;
        let retries = cam.stats().unwrap().retries;
        let retransmits = cam.link_stats().gvcp_retransmits;
        cam.disconnect(Duration::from_secs(1));
        (first, second, retries, retransmits)
    });
    let wall = wall.elapsed();
    assert_costs(
        first,
        Duration::from_millis(500),
        "wait_timeout gives up at its own timeout",
    );
    assert_costs(
        second,
        Duration::from_millis(1500),
        "the transaction fails after its tries, 500 ms each",
    );
    assert_eq!(retries, 2);
    assert_eq!(retransmits, 2);
    assert!(
        wall < Duration::from_secs(5),
        "virtual timeouts must not cost wall time ({wall:?})"
    );
}

#[test]
fn repeated_runs_with_one_seed_are_identical() {
    let runs: Vec<SnapRun> = (0..3).map(|_| driven(21, snap_run)).collect();
    assert_eq!(runs[0], runs[1]);
    assert_eq!(runs[1], runs[2]);
}

/// Discovery with no adapters given: the interface comes from the OS
/// enumeration, the beacon goes out as a real subnet broadcast, and the
/// camera on that segment answers within the receive window.
#[test]
fn discovery_finds_a_camera_by_broadcast_on_an_enumerated_adapter() {
    let sim = Sim::builder()
        .nic(
            NicSpec::new("eth0")
                .address(snare::IpNet::new(HOST_IP.into(), 24))
                .station(DEVICE_IP),
        )
        .strict_sockopts()
        .stuck_after(Duration::from_secs(30))
        .build();
    let (devices, window) = sim.run(|| {
        let device = Camera::spawn();
        let t0 = Instant::now();
        let devices = discovery::discover(&DiscoveryConfig::default()).expect("discover");
        let window = t0.elapsed();
        device.stop();
        (devices, window)
    });
    assert_eq!(devices.len(), 1, "{devices:?}");
    let d = &devices[0];
    assert_eq!(d.info.mac, DEVICE_MAC);
    assert_eq!(d.info.ip, DEVICE_IP);
    assert_eq!(d.from, SocketAddr::new(IpAddr::V4(DEVICE_IP), GVCP_PORT));
    assert_eq!(d.adapter.name, "eth0");
    assert_eq!(d.adapter.ip, HOST_IP);
    assert_eq!(d.adapter.broadcast, Ipv4Addr::new(10, 0, 0, 255));
    assert!(discovery::is_reachable(d));
    assert!(window >= DiscoveryConfig::default().recv_window);
}
