//! The GVCP control channel inside a snare simulation: option handling,
//! retransmission, PENDING_ACK, heartbeat and control loss, malformed
//! acknowledges, concurrent callers, reconnects and discovery, against an
//! emulated camera with fault knobs.

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

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use common::{
    ACQ_REG, Camera, CameraSpec, DEVICE_IP, HOST_IP, Pending, assert_costs, assert_within, config,
    sim,
};
use snare::{IpNet, NicSpec, SocketEntry, SocketKind};
use telegenic::emulator;
use telegenic::fast_talker::rt::QosClass;
use telegenic::gige::discovery::{self, DiscoveryConfig};
use telegenic::gige::proto::bootstrap;
use telegenic::gige::proto::gvcp::{self, GvcpStatus};
use telegenic::gige::{GigECamera, GigeConfig};
use telegenic::{
    CameraError, FrameStatus, GenICamera, PacketSize, ReportSummary, SocketOption, StreamConfig,
    ThreadOption,
};

const PROBE_REG: u32 = 0x2020;
const TIMEOUT: Duration = Duration::from_millis(500);
const CONTROL_PORT: u16 = 40_000;
/// The worker's poll period: deadlines are noticed at its next wake.
const POLL: Duration = Duration::from_millis(10);

fn control_addr() -> SocketAddr {
    SocketAddr::new(HOST_IP.into(), CONTROL_PORT)
}

fn bound_config(retries: u8) -> GigeConfig {
    let mut cfg = config(retries);
    cfg.local_addr = Some(control_addr());
    cfg
}

fn connected(cfg: GigeConfig) -> GigECamera {
    let mut cam = GigECamera::with_config(cfg);
    cam.connect().expect("connect");
    cam
}

/// Open UDP sockets other than the emulated camera's.
fn host_udp_sockets() -> Vec<SocketEntry> {
    snare::socket_table()
        .into_iter()
        .filter(|s| s.kind == SocketKind::Udp)
        .filter(|s| s.local.is_none_or(|a| a.ip() != IpAddr::V4(DEVICE_IP)))
        .collect()
}

fn stream_config() -> StreamConfig {
    let mut s = StreamConfig::new();
    s.payload_size = Some((common::WIDTH * common::HEIGHT) as usize);
    s.packet_size = PacketSize::Fixed(576);
    s
}

#[track_caller]
fn expect_invalid(result: telegenic::Result<impl std::fmt::Debug>, driver: &str) {
    match result {
        Err(CameraError::InvalidOption { driver: d, .. }) if d == driver => {}
        other => panic!("expected InvalidOption from {driver}, got {other:?}"),
    }
}

/// A refused option fails `connect` (or `open_stream`) before the driver
/// puts a single datagram on the wire, and leaves no socket behind.
#[test]
fn refused_options_fail_before_any_traffic() {
    sim(1).run(|| {
        let device = Camera::spawn();
        let refused_control: [GigeConfig; 3] = [
            GigeConfig {
                thread: vec![ThreadOption::RtPriority(80)],
                ..config(2)
            },
            GigeConfig {
                control_socket: vec![SocketOption::SendBuffer(1 << 20)],
                ..config(2)
            },
            GigeConfig {
                control_socket: vec![SocketOption::LinuxBusyPoll(50)],
                ..config(2)
            },
        ];
        for cfg in refused_control {
            let what = format!("{:?}{:?}", cfg.thread, cfg.control_socket);
            let mut cam = GigECamera::with_config(cfg);
            expect_invalid(cam.connect(), "gvcp");
            assert!(!cam.is_connected());
            assert_eq!(device.log().datagrams, 0, "{what} reached the device");
            assert!(host_udp_sockets().is_empty(), "{what} left a socket open");
        }

        let cam = connected(config(2));
        let before = device.log().datagrams;
        let sockets = host_udp_sockets().len();
        let refused_stream = [
            StreamConfig {
                stream_socket: vec![SocketOption::Dscp(46)],
                ..stream_config()
            },
            StreamConfig {
                stream_socket: vec![SocketOption::SendBuffer(1 << 20)],
                ..stream_config()
            },
            StreamConfig {
                thread: vec![ThreadOption::MacOsTimeConstraint {
                    period_us: 1000,
                    computation_us: 100,
                    constraint_us: 500,
                }],
                ..stream_config()
            },
        ];
        for cfg in refused_stream {
            let what = format!("{:?}{:?}", cfg.thread, cfg.stream_socket);
            expect_invalid(cam.open_stream(cfg), "gvsp");
            assert_eq!(device.log().datagrams, before, "{what} reached the device");
            assert_eq!(
                host_udp_sockets().len(),
                sockets,
                "{what} left a socket open"
            );
        }
        assert!(cam.is_connected());
        drop(cam);
        device.stop();
    });
}

fn foreign_options() -> (Vec<ThreadOption>, Vec<SocketOption>, Vec<SocketOption>) {
    if cfg!(target_os = "linux") {
        (
            vec![
                ThreadOption::MacOsQos(QosClass::UserInteractive),
                ThreadOption::WinDisablePowerThrottling,
            ],
            vec![],
            vec![SocketOption::WinCpuAffinity(1)],
        )
    } else {
        (
            vec![
                ThreadOption::LinuxNice(5),
                ThreadOption::WinDisablePowerThrottling,
            ],
            vec![SocketOption::LinuxPriority(4)],
            vec![
                SocketOption::LinuxBusyPoll(50),
                SocketOption::WinCpuAffinity(1),
            ],
        )
    }
}

fn skipped<O: Clone>(report: &ReportSummary<O>) -> Vec<O> {
    report.skipped.iter().map(|s| s.option.clone()).collect()
}

/// Options meant for another platform are skipped: the link comes up and
/// streams as if they were absent.
#[test]
fn options_for_another_platform_are_skipped() {
    sim(2).run(|| {
        let device = Camera::spawn();
        let (thread, control_socket, stream_socket) = foreign_options();
        let cam = connected(GigeConfig {
            thread: thread.clone(),
            control_socket: control_socket.clone(),
            ..config(2)
        });
        let mut s = stream_config();
        s.thread = thread.clone();
        s.stream_socket.extend(stream_socket.clone());
        let stream = cam.open_stream(s).expect("open stream");
        let control = cam.tuning_report().expect("connected");
        assert_eq!(skipped(&control.thread), thread);
        assert_eq!(skipped(&control.socket), control_socket);
        assert_eq!(skipped(&stream.tuning_report().thread), thread);
        assert_eq!(skipped(&stream.tuning_report().socket), stream_socket);
        assert_eq!(
            stream.tuning_report().socket.applied,
            [SocketOption::RecvBuffer(
                telegenic::gige::stream::DEFAULT_STREAM_RECV_BUFFER
            )]
        );
        let frames = stream.subscribe(1);
        cam.write_register(ACQ_REG, 1).unwrap().wait().unwrap();
        let frame = frames.wait_for(Duration::from_secs(1)).expect("frame");
        assert_eq!(frame.status, FrameStatus::Complete);
        assert_eq!(frame.data(), device.image());
        drop(stream);
        drop(cam);
        device.stop();
    });
}

/// With the control socket on a wildcard bind, the stream is advertised
/// at and bound to the host's address on the camera's subnet.
#[test]
fn an_unbound_control_socket_streams_to_the_host_address() {
    sim(4).run(|| {
        let _device = Camera::spawn();
        let cam = connected(config(2));
        let stream = cam.open_stream(stream_config()).expect("open stream");
        assert_eq!(stream.local_addr().ip(), IpAddr::V4(HOST_IP));
    });
}

/// Accepted socket options land on the driver's sockets: the receive
/// buffers and the bound interface read back from the kernel's view.
#[test]
fn accepted_socket_options_land_on_the_sockets() {
    sim(3).run(|| {
        let device = Camera::spawn();
        let cam = connected(GigeConfig {
            control_socket: vec![
                SocketOption::RecvBuffer(256 * 1024),
                SocketOption::BindDevice("eth0".into()),
                SocketOption::Dscp(46),
            ],
            ..bound_config(2)
        });
        let mut s = stream_config();
        s.stream_socket
            .push(SocketOption::BindDevice("eth0".into()));
        let stream = cam.open_stream(s).expect("open stream");

        let [control] = &snare::sockets_bound(control_addr())[..] else {
            panic!("one control socket at {}", control_addr());
        };
        let [gvsp] = &snare::sockets_bound(stream.local_addr())[..] else {
            panic!("one stream socket at {}", stream.local_addr());
        };
        assert_eq!(control.bound_device.as_deref(), Some("eth0"));
        assert_eq!(gvsp.bound_device.as_deref(), Some("eth0"));
        assert!(
            control.rcvbuf as usize >= 256 * 1024,
            "control rcvbuf {}",
            control.rcvbuf
        );
        assert!(
            gvsp.rcvbuf as usize >= telegenic::gige::stream::DEFAULT_STREAM_RECV_BUFFER,
            "stream rcvbuf {}",
            gvsp.rcvbuf
        );
        assert!(control.unmodelled_options.is_empty());
        assert!(gvsp.unmodelled_options.is_empty());
        drop(stream);
        drop(cam);
        device.stop();
    });
}

/// Binding to an interface the host does not have fails `connect` with the
/// OS error before any traffic, and closes the socket before binding it.
#[test]
fn an_unknown_bind_device_fails_connect_cleanly() {
    sim(4).run(|| {
        let device = Camera::spawn();
        let mut cam = GigECamera::with_config(GigeConfig {
            control_socket: vec![SocketOption::BindDevice("nope0".into())],
            ..bound_config(2)
        });
        let err = cam.connect().unwrap_err();
        assert!(matches!(err, CameraError::Io(_)), "{err:?}");
        assert!(!cam.is_connected());
        assert_eq!(device.log().datagrams, 0);
        assert!(snare::sockets_bound(control_addr()).is_empty());
        assert!(
            snare::closed_sockets()
                .iter()
                .any(|s| s.kind == SocketKind::Udp && s.local.is_none())
        );
        assert!(host_udp_sockets().is_empty());
        cam.config_mut().control_socket.clear();
        cam.connect().expect("connect without the bad option");
        drop(cam);
        device.stop();
    });
}

/// A stream thread option an unprivileged process cannot apply. macOS
/// applies, clamps or reports every thread option, so it has none.
#[cfg(not(target_os = "macos"))]
fn refusing_stream_thread() -> Vec<ThreadOption> {
    vec![ThreadOption::RtPriority(50)]
}

/// A stream thread option the OS refuses fails `open_stream` with the OS
/// error instead of starting a worker without it.
#[cfg(not(target_os = "macos"))]
#[test]
fn a_stream_thread_option_the_os_refuses_fails_open() {
    let sim = common::sim_with(5, |b| b.privileges(snare::Privileges::none()));
    sim.run(|| {
        let device = Camera::spawn();
        let cam = connected(config(2));
        let sockets = host_udp_sockets().len();
        let mut s = stream_config();
        s.thread = refusing_stream_thread();
        match cam.open_stream(s) {
            Err(CameraError::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "{e}")
            }
            other => panic!("expected a permission error, got {other:?}"),
        }
        assert_eq!(host_udp_sockets().len(), sockets, "stream socket leaked");
        assert!(cam.is_connected());
        let stream = cam.open_stream(stream_config()).expect("open without it");
        drop(stream);
        drop(cam);
        device.stop();
    });
}

/// The device must not be left streaming at a socket that is gone: an open
/// that fails after pointing SCDA/SCP at its socket closes the channel again.
#[cfg(not(target_os = "macos"))]
#[test]
fn a_failed_stream_open_closes_the_channel_on_the_device() {
    let sim = common::sim_with(6, |b| b.privileges(snare::Privileges::none()));
    sim.run(|| {
        let device = Camera::spawn();
        let cam = connected(config(2));
        let mut s = stream_config();
        s.thread = refusing_stream_thread();
        assert!(cam.open_stream(s).is_err());
        let scp = device.device(|d| d.read_reg(bootstrap::STREAM_CHANNEL_PORT));
        assert_eq!(scp, 0, "SCP still points at the failed stream socket");
        drop(cam);
        device.stop();
    });
}

/// Calls that wait on a transaction themselves return its error with the
/// variant the worker recorded.
#[test]
fn blocking_calls_return_the_transaction_error_unchanged() {
    sim(8).run(|| {
        let device = Camera::spawn();
        let cam = connected(config(2));
        device.faults(|f| {
            f.ack_filter = Some(Box::new(|cmd, _, ack| match common::first_addr(cmd) {
                Some(bootstrap::STREAM_CHANNEL_DEST_ADDRESS) => vec![ack_bytes(
                    GvcpStatus::ACCESS_DENIED,
                    gvcp::WRITE_REGISTER_ACK,
                    cmd.req_id,
                    &[],
                )],
                Some(bootstrap::MESSAGE_CHANNEL_PORT) => vec![ack_bytes(
                    GvcpStatus::SUCCESS,
                    gvcp::READ_REGISTER_ACK,
                    cmd.req_id,
                    &[0, 0, 0, 1],
                )],
                _ => vec![ack],
            }))
        });
        match cam.open_stream(stream_config()) {
            Err(CameraError::Nak {
                command: gvcp::WRITE_REGISTER_CMD,
                status: GvcpStatus::ACCESS_DENIED,
            }) => {}
            other => panic!("expected the SCDA NAK, got {other:?}"),
        }
        match cam.disable_events() {
            Err(CameraError::Protocol(m)) if m.contains("expected ack 0x0083") => {}
            other => panic!("expected a mismatched-ack error, got {other:?}"),
        }

        device.faults(|f| {
            f.ack_filter = None;
            f.ignore.insert(bootstrap::MESSAGE_CHANNEL_PORT, usize::MAX);
        });
        match cam.disable_events() {
            Err(CameraError::Timeout) => {}
            other => panic!("expected Timeout, got {other:?}"),
        }

        device.faults(|f| f.silent = true);
        let beats = heartbeats(&device).len();
        while heartbeats(&device).len() == beats {
            std::thread::sleep(Duration::from_millis(1));
        }
        match cam.disable_events() {
            Err(CameraError::ControlLost) => {}
            other => panic!("expected ControlLost, got {other:?}"),
        }
        drop(cam);
        device.stop();
    });
}

#[cfg(target_os = "linux")]
/// A control thread option the OS refuses fails `connect` before any
/// traffic and leaves no socket behind.
#[test]
fn a_control_thread_option_the_os_refuses_fails_connect() {
    let sim = common::sim_with(7, |b| b.privileges(snare::Privileges::none()));
    sim.run(|| {
        let device = Camera::spawn();
        let mut cam = GigECamera::with_config(GigeConfig {
            thread: vec![ThreadOption::LinuxNice(-10)],
            ..config(2)
        });
        match cam.connect() {
            Err(CameraError::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "{e}")
            }
            other => panic!("expected a permission error, got {other:?}"),
        }
        assert_eq!(device.log().datagrams, 0);
        assert!(host_udp_sockets().is_empty());
        device.stop();
    });
}

/// Commands the link loses are sent again after each acknowledge timeout,
/// under the same request id, until one gets through.
#[test]
fn lost_commands_are_retransmitted_under_their_request_id() {
    sim(10).run(|| {
        let device = Camera::spawn();
        device.faults(|f| {
            f.ignore.insert(PROBE_REG, 2);
        });
        let cam = connected(config(4));
        let t0 = Instant::now();
        cam.write_register(PROBE_REG, 0xabcd)
            .unwrap()
            .wait()
            .unwrap();
        assert_costs(t0.elapsed(), 2 * TIMEOUT, "two lost tries, 500 ms each");

        let tries = device.log().commands_to(PROBE_REG);
        assert_eq!(tries.len(), 3);
        assert!(
            tries.iter().all(|c| c.req_id == tries[0].req_id),
            "{tries:?}"
        );
        assert_costs(tries[1].at - tries[0].at, TIMEOUT, "first retransmit");
        assert_costs(tries[2].at - tries[1].at, TIMEOUT, "second retransmit");
        assert_eq!(device.device(|d| d.read_reg(PROBE_REG)), 0xabcd);
        let stats = cam.stats().unwrap();
        assert_eq!((stats.retries, stats.timeouts), (2, 0));
        assert_eq!(cam.link_stats().gvcp_retransmits, 2);
        drop(cam);
        device.stop();
    });
}

/// A lost acknowledge makes the driver retransmit a command the device
/// already executed; the device answers from its cache, so AcquisitionStart
/// runs (and streams) exactly once.
#[test]
fn a_lost_ack_is_replayed_without_executing_the_command_twice() {
    sim(11).run(|| {
        let device = Camera::spawn();
        let mut dropped = false;
        device.faults(|f| {
            f.ack_filter = Some(Box::new(move |cmd, _, ack| {
                if common::first_addr(cmd) == Some(ACQ_REG) && !dropped {
                    dropped = true;
                    return vec![];
                }
                vec![ack]
            }));
        });
        let cam = connected(config(4));
        let stream = cam.open_stream(stream_config()).expect("open stream");
        let frames = stream.subscribe(4);
        let t0 = Instant::now();
        cam.write_register(ACQ_REG, 1).unwrap().wait().unwrap();
        assert_costs(t0.elapsed(), TIMEOUT, "one retransmit after the lost ack");
        let frame = frames.wait_for(Duration::from_secs(1)).expect("frame");
        assert_eq!(frame.status, FrameStatus::Complete);
        assert!(frames.wait_for(Duration::from_millis(500)).is_none());

        let log = device.log();
        assert_eq!(log.commands_to(ACQ_REG).len(), 2);
        assert_eq!(log.acquisitions, 1);
        assert_eq!(log.frames, [1]);
        assert_eq!(device.device(|d| d.duplicates()), 1);
        assert_eq!(cam.link_stats().gvcp_retransmits, 1);
        drop(stream);
        drop(cam);
        device.stop();
    });
}

#[derive(Debug, PartialEq, Eq)]
struct LossyRun {
    frames: Vec<(u64, FrameStatus)>,
    retransmits: u64,
    retries: u64,
    timeouts: u64,
    elapsed: Duration,
}

fn lossy_run(seed: u64) -> LossyRun {
    sim(seed).run(|| {
        let device = Camera::spawn();
        snare::set_udp_policy(device.addr(), |p| p.loss_rate = 0.2);
        snare::set_udp_policy(control_addr(), |p| p.loss_rate = 0.2);
        let t0 = Instant::now();
        let mut cam = GenICamera::from_transport(GigECamera::with_config(bound_config(8)));
        cam.connect().expect("connect over a lossy link");
        let mut s = StreamConfig::new();
        s.packet_size = PacketSize::Fixed(1500);
        let mut session = cam.snapshot_session(s).expect("session");
        let mut frames = Vec::new();
        for _ in 0..3 {
            let f = session.snap(Duration::from_secs(10)).expect("snap");
            assert_eq!(f.data(), device.image());
            frames.push((f.frame_id, f.status));
        }
        drop(session);
        let stats = cam.transport().stats().unwrap();
        let run = LossyRun {
            frames,
            retransmits: cam.link_stats().gvcp_retransmits,
            retries: stats.retries,
            timeouts: stats.timeouts,
            elapsed: t0.elapsed(),
        };
        cam.disconnect(Duration::from_secs(1));
        device.stop();
        run
    })
}

/// A fifth of the datagrams lost each way: every transaction still lands
/// through retransmissions, each counted once, and the run replays exactly
/// under its seed.
#[test]
fn control_survives_loss_in_both_directions_and_replays() {
    let first = lossy_run(12);
    assert_eq!(
        first.frames,
        [
            (1, FrameStatus::Complete),
            (2, FrameStatus::Complete),
            (3, FrameStatus::Complete)
        ]
    );
    assert!(first.retransmits > 0, "{first:?}");
    assert_eq!(first.retransmits, first.retries);
    assert_eq!(first.timeouts, 0);
    assert_eq!(lossy_run(12), first);
}

/// PENDING_ACK replaces the acknowledge timeout: the transaction waits for
/// the device's late answer instead of retransmitting at 500 ms.
#[test]
fn pending_ack_holds_the_transaction_past_its_timeout() {
    sim(13).run(|| {
        let device = Camera::spawn();
        device.faults(|f| {
            f.pending.insert(
                PROBE_REG,
                Pending {
                    timeout_ms: 2000,
                    answer_after: Some(Duration::from_millis(1500)),
                },
            );
        });
        let cam = connected(config(2));
        let t0 = Instant::now();
        cam.write_register(PROBE_REG, 7).unwrap().wait().unwrap();
        assert_costs(t0.elapsed(), Duration::from_millis(1500), "the late answer");
        let stats = cam.stats().unwrap();
        assert_eq!(
            (stats.pending_acks, stats.retries, stats.timeouts),
            (1, 0, 0)
        );
        assert_eq!(device.log().commands_to(PROBE_REG).len(), 1);
        assert_eq!(device.device(|d| d.read_reg(PROBE_REG)), 7);
        drop(cam);
        device.stop();
    });
}

/// A device that answers every try with PENDING_ACK and never completes
/// fails the transaction after one pending window per try.
#[test]
fn a_device_that_stays_pending_times_out_after_every_window() {
    sim(14).run(|| {
        let device = Camera::spawn();
        device.faults(|f| {
            f.pending.insert(
                PROBE_REG,
                Pending {
                    timeout_ms: 800,
                    answer_after: None,
                },
            );
        });
        let cam = connected(config(2));
        let t0 = Instant::now();
        let err = cam
            .write_register(PROBE_REG, 7)
            .unwrap()
            .wait()
            .unwrap_err();
        assert!(matches!(*err, CameraError::Timeout), "{err:?}");
        assert_within(
            t0.elapsed(),
            Duration::from_millis(3 * 800),
            POLL,
            "three 800 ms pending windows",
        );
        let stats = cam.stats().unwrap();
        assert_eq!(
            (stats.pending_acks, stats.retries, stats.timeouts),
            (3, 2, 1)
        );
        assert!(cam.is_connected());
        drop(cam);
        device.stop();
    });
}

fn heartbeats(device: &Camera) -> Vec<common::Command> {
    device
        .log()
        .commands_to(bootstrap::CONTROL_CHANNEL_PRIVILEGE)
        .into_iter()
        .filter(|c| c.command == gvcp::READ_REGISTER_CMD)
        .collect()
}

/// An idle connection reads CCP every third of the heartbeat timeout, so
/// the device never drops control.
#[test]
fn heartbeat_keeps_control_while_idle() {
    sim(20).run(|| {
        let device = Camera::spawn();
        let cam = connected(config(2));
        assert_eq!(
            device.device(|d| d.read_reg(bootstrap::HEARTBEAT_TIMEOUT)),
            3000
        );
        std::thread::sleep(Duration::from_millis(10_500));
        let beats = heartbeats(&device);
        assert_eq!(beats.len(), 10, "{beats:?}");
        for pair in beats.windows(2) {
            assert_within(
                pair[1].at - pair[0].at,
                Duration::from_millis(1000),
                POLL + common::SIM_OVERHEAD,
                "heartbeat period",
            );
        }
        let stats = cam.stats().unwrap();
        assert_eq!(
            (stats.heartbeats, stats.retries, stats.timeouts),
            (10, 0, 0)
        );
        assert!(cam.is_connected());
        drop(cam);
        device.stop();
    });
}

/// Polls `is_connected` every millisecond until it drops; the instant it did.
fn wait_disconnected(cam: &GigECamera, give_up: Duration) -> Instant {
    let end = Instant::now() + give_up;
    while cam.is_connected() {
        assert!(Instant::now() < end, "control never dropped");
        std::thread::sleep(Duration::from_millis(1));
    }
    Instant::now()
}

/// A camera that stops answering is declared lost by the first heartbeat
/// that exhausts its tries; calls then fail with `ControlLost`, and
/// `connect` redials once the camera is back, without leaking the old socket.
#[test]
fn a_silent_camera_is_declared_lost_and_reconnects_when_back() {
    sim(21).run(|| {
        let device = Camera::spawn();
        let mut cam = connected(config(2));
        std::thread::sleep(Duration::from_millis(2500));
        device.faults(|f| f.silent = true);
        let lost_at = wait_disconnected(&cam, Duration::from_secs(10));

        let beats = heartbeats(&device);
        let failing = &beats[beats.len() - 3..];
        assert!(failing.iter().all(|c| c.req_id == failing[0].req_id));
        assert_within(
            lost_at - failing[0].at,
            3 * TIMEOUT,
            POLL + Duration::from_millis(2),
            "three unanswered heartbeat tries",
        );
        assert_eq!(cam.stats().unwrap().timeouts, 1);
        match cam.read_register(PROBE_REG) {
            Err(CameraError::ControlLost) => {}
            other => panic!("expected ControlLost, got {other:?}"),
        }

        device.faults(|f| f.silent = false);
        cam.connect().expect("reconnect");
        assert!(cam.is_connected());
        assert_eq!(cam.stats().unwrap().heartbeats, 0, "fresh connection stats");
        assert_eq!(host_udp_sockets().len(), 1, "{:?}", host_udp_sockets());
        cam.read_register(PROBE_REG).unwrap().wait().unwrap();
        drop(cam);
        device.stop();
    });
}

/// Carrier loss on the host's interface: the heartbeat goes unanswered and
/// control is lost; redialing fails with `ConnectTimeout` while the link is
/// down and succeeds once it is back.
#[test]
fn link_down_loses_control_and_link_up_lets_it_reconnect() {
    sim(22).run(|| {
        let device = Camera::spawn();
        let mut cam = connected(config(2));
        std::thread::sleep(Duration::from_millis(2500));
        snare::set_link("eth0", false).unwrap();
        wait_disconnected(&cam, Duration::from_secs(10));
        assert!(snare::nic_counters("eth0").unwrap().tx_carrier_errors >= 3);

        let t0 = Instant::now();
        match cam.connect() {
            Err(CameraError::ConnectTimeout) => {}
            other => panic!("expected ConnectTimeout, got {other:?}"),
        }
        assert_within(
            t0.elapsed(),
            3 * TIMEOUT,
            POLL + Duration::from_millis(2),
            "CCP write tries",
        );
        assert!(!cam.is_connected());

        snare::set_link("eth0", true).unwrap();
        cam.connect().expect("reconnect after link up");
        cam.write_register(PROBE_REG, 3).unwrap().wait().unwrap();
        assert_eq!(device.device(|d| d.read_reg(PROBE_REG)), 3);
        drop(cam);
        device.stop();
    });
}

fn ack_bytes(status: GvcpStatus, answer: u16, id: u16, payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&status.0.to_be_bytes());
    b.extend_from_slice(&answer.to_be_bytes());
    b.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    b.extend_from_slice(&id.to_be_bytes());
    b.extend_from_slice(payload);
    b
}

/// Garbage around the real acknowledge — a truncated header, a length past
/// the datagram, a stale id, an unexpected command, a datagram from another
/// host — is ignored; the transaction completes on the real ack at once.
#[test]
fn malformed_and_stale_acks_are_ignored() {
    let stranger_ip = Ipv4Addr::new(10, 0, 0, 66);
    common::bare_sim_with(30, |b| b.nic(common::eth0().station(stranger_ip))).run(|| {
        let device = Camera::spawn();
        let cam = connected(config(2));
        cam.write_register(PROBE_REG, 0x1234)
            .unwrap()
            .wait()
            .unwrap();
        let unsolicited = cam.stats().unwrap().unsolicited;
        let stranger = UdpSocket::bind((stranger_ip, 0)).unwrap();
        device.faults(|f| {
            f.ack_filter = Some(Box::new(move |cmd, src, ack| {
                if common::first_addr(cmd) != Some(PROBE_REG) {
                    return vec![ack];
                }
                let mut stale = ack.clone();
                stale[6..8].copy_from_slice(&cmd.req_id.wrapping_sub(1).to_be_bytes());
                let mut overlong = ack.clone();
                overlong[4..6].copy_from_slice(&100u16.to_be_bytes());
                stranger.send_to(&ack, src).unwrap();
                vec![
                    ack[..6].to_vec(),
                    overlong,
                    stale,
                    gvcp::encode_read_reg(&[PROBE_REG], 9),
                    ack,
                ]
            }));
        });
        let t0 = Instant::now();
        let value = cam.read_register(PROBE_REG).unwrap().wait().unwrap();
        assert_costs(t0.elapsed(), Duration::ZERO, "the real ack completes it");
        assert_eq!(value, 0x1234);
        let stats = cam.stats().unwrap();
        assert_eq!(
            stats.unsolicited - unsolicited,
            1,
            "only the stale id counts"
        );
        assert_eq!((stats.retries, stats.timeouts, stats.events), (0, 0, 0));
        drop(cam);
        device.stop();
    });
}

/// An acknowledge with the right id but the wrong shape fails that one
/// transaction with an error naming the mismatch; the link stays up.
#[test]
fn a_mismatched_ack_fails_only_its_transaction() {
    sim(31).run(|| {
        let device = Camera::spawn();
        let cam = connected(config(2));
        let replace = |make: fn(u16) -> Vec<u8>| {
            device.faults(|f| {
                f.ack_filter = Some(Box::new(move |cmd, _, ack| {
                    if common::first_addr(cmd) == Some(PROBE_REG) {
                        vec![make(cmd.req_id)]
                    } else {
                        vec![ack]
                    }
                }))
            })
        };

        replace(|id| {
            ack_bytes(
                GvcpStatus::SUCCESS,
                gvcp::WRITE_REGISTER_ACK,
                id,
                &[0, 0, 0, 1],
            )
        });
        let err = cam.read_register(PROBE_REG).unwrap().wait().unwrap_err();
        assert!(
            matches!(&*err, CameraError::Protocol(m) if m.contains("expected ack 0x0081")),
            "{err:?}"
        );

        replace(|id| {
            ack_bytes(
                GvcpStatus::SUCCESS,
                gvcp::READ_REGISTER_ACK,
                id,
                &[0, 0, 0, 1],
            )
        });
        let err = cam
            .read_registers(vec![PROBE_REG, PROBE_REG + 4])
            .unwrap()
            .wait()
            .unwrap_err();
        assert!(
            matches!(&*err, CameraError::Protocol(m) if m.contains("carried 1 values, expected 2")),
            "{err:?}"
        );

        replace(|id| ack_bytes(GvcpStatus::ACCESS_DENIED, gvcp::WRITE_REGISTER_ACK, id, &[]));
        let err = cam
            .write_register(PROBE_REG, 1)
            .unwrap()
            .wait()
            .unwrap_err();
        assert!(
            matches!(
                &*err,
                CameraError::Nak {
                    command: gvcp::WRITE_REGISTER_CMD,
                    status: GvcpStatus::ACCESS_DENIED
                }
            ),
            "{err:?}"
        );

        device.faults(|f| f.ack_filter = None);
        cam.write_register(PROBE_REG, 2).unwrap().wait().unwrap();
        assert_eq!(cam.read_register(PROBE_REG).unwrap().wait().unwrap(), 2);
        let stats = cam.stats().unwrap();
        assert_eq!((stats.naks, stats.retries, stats.timeouts), (1, 0, 0));
        assert!(cam.is_connected());
        drop(cam);
        device.stop();
    });
}

const CALLERS: u32 = 4;
const CALLS: u32 = 10;
const LATENCY: Duration = Duration::from_millis(1);

fn concurrent_run(seed: u64) -> (Duration, Vec<u16>) {
    sim(seed).run(|| {
        let device = Camera::spawn();
        let cam = connected(config(2));
        snare::set_udp_policy(device.addr(), |p| p.latency = LATENCY);
        let before = device.log().commands.len();
        let t0 = Instant::now();
        std::thread::scope(|s| {
            for caller in 0..CALLERS {
                let cam = &cam;
                s.spawn(move || {
                    let reg = 0x3000 + 4 * caller;
                    for i in 0..CALLS {
                        let value = caller << 16 | i;
                        cam.write_register(reg, value).unwrap().wait().unwrap();
                        assert_eq!(cam.read_register(reg).unwrap().wait().unwrap(), value);
                    }
                });
            }
        });
        let elapsed = t0.elapsed();
        let commands = device.log().commands[before..].to_vec();
        assert_eq!(commands.len() as u32, 2 * CALLERS * CALLS);
        for pair in commands.windows(2) {
            assert!(
                pair[1].at - pair[0].at >= LATENCY,
                "a command arrived while another was in flight: {pair:?}"
            );
        }
        drop(cam);
        device.stop();
        (elapsed, commands.iter().map(|c| c.req_id).collect())
    })
}

/// Callers on several threads share one control channel: their
/// transactions go out one at a time, each after the previous one's ack,
/// and every caller reads back its own writes.
#[test]
fn concurrent_callers_share_one_transaction_in_flight() {
    for seed in 1..=3 {
        let (elapsed, ids) = concurrent_run(seed);
        let n = 2 * CALLERS * CALLS;
        assert_within(
            elapsed,
            LATENCY * n,
            Duration::from_millis(1),
            "one latency per transaction",
        );
        let mut sorted = ids.clone();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len(), "request ids repeat: {ids:?}");
        assert!(
            ids.windows(2).all(|w| w[1] == gvcp::next_id(w[0])),
            "{ids:?}"
        );
    }
}

/// Repeated connect/stream/disconnect cycles: each disconnect releases
/// control and the stream channel on the device and closes every driver
/// socket; each reconnect starts with fresh per-connection state while the
/// cumulative link counters carry over.
#[test]
fn reconnect_cycles_start_fresh_and_leak_no_sockets() {
    sim(32).run(|| {
        let device = Camera::spawn();
        device.faults(|f| {
            f.ignore.insert(PROBE_REG, 1);
        });
        let mut cam = GigECamera::with_config(config(2));
        for cycle in 1..=3u64 {
            cam.connect().expect("connect");
            assert_eq!(
                device.device(|d| d.read_reg(bootstrap::CONTROL_CHANNEL_PRIVILEGE)),
                bootstrap::CCP_CONTROL
            );
            let commands = cam.stats().unwrap().commands;
            assert!(
                commands < 20,
                "cycle {cycle}: stats carried over ({commands})"
            );
            cam.write_register(PROBE_REG, 1).unwrap().wait().unwrap();
            let stream = cam.open_stream(stream_config()).expect("open stream");
            let frames = stream.subscribe(1);
            cam.write_register(ACQ_REG, 1).unwrap().wait().unwrap();
            let frame = frames.wait_for(Duration::from_secs(1)).expect("frame");
            assert_eq!(
                (frame.frame_id, frame.status),
                (cycle, FrameStatus::Complete)
            );
            drop(frame);
            drop(stream);
            assert_eq!(
                device.device(|d| d.read_reg(bootstrap::STREAM_CHANNEL_PORT)),
                0
            );
            cam.disconnect(Duration::from_secs(1));
            assert!(!cam.is_connected());
            assert!(
                matches!(cam.read_register(PROBE_REG), Err(CameraError::Disconnected)),
                "cycle {cycle}"
            );
            assert_eq!(
                device.device(|d| d.read_reg(bootstrap::CONTROL_CHANNEL_PRIVILEGE)),
                0,
                "cycle {cycle}: control not released"
            );
            assert!(
                host_udp_sockets().is_empty(),
                "cycle {cycle}: {:?}",
                host_udp_sockets()
            );
            assert_eq!(
                cam.link_stats().gvcp_retransmits,
                1,
                "cumulative across cycles"
            );
        }
        device.stop();
    });
}

const SECOND_HOST: Ipv4Addr = Ipv4Addr::new(192, 168, 1, 1);
const SECOND_CAMERA: Ipv4Addr = Ipv4Addr::new(192, 168, 1, 7);
const STRAY_CAMERA: Ipv4Addr = Ipv4Addr::new(169, 254, 3, 3);

/// Two adapters, a camera on each subnet and one with a link-local address
/// on the first segment: each camera is reported once, with the adapter
/// that heard it, and only the limited broadcast finds the stray one —
/// which is then flagged as unreachable.
#[test]
fn discovery_across_adapters_finds_each_camera_once() {
    let sim = common::bare_sim_with(33, |b| {
        b.nic(common::eth0().station(STRAY_CAMERA)).nic(
            NicSpec::new("eth1")
                .address(IpNet::new(SECOND_HOST.into(), 24))
                .station(SECOND_CAMERA),
        )
    });
    let (all, subnet_only) = sim.run(|| {
        let cameras = [(DEVICE_IP, 5), (SECOND_CAMERA, 7), (STRAY_CAMERA, 3)].map(|(ip, n)| {
            Camera::with(CameraSpec {
                ip,
                mac: [0x00, 0x11, 0x1c, 0, 0, n],
                ..CameraSpec::default()
            })
        });
        let all = discovery::discover(&DiscoveryConfig::default()).expect("discover");
        let subnet_only = discovery::discover(&DiscoveryConfig {
            limited_broadcast: false,
            ..DiscoveryConfig::default()
        })
        .expect("discover");
        for c in cameras {
            c.stop();
        }
        (all, subnet_only)
    });
    let found = |devices: &[discovery::DiscoveredDevice]| {
        let mut v: Vec<_> = devices
            .iter()
            .map(|d| {
                (
                    d.info.ip,
                    d.info.mac[5],
                    d.adapter.name.clone(),
                    discovery::is_reachable(d),
                )
            })
            .collect();
        v.sort();
        v
    };
    assert_eq!(
        found(&all),
        [
            (DEVICE_IP, 5, "eth0".to_string(), true),
            (STRAY_CAMERA, 3, "eth0".to_string(), false),
            (SECOND_CAMERA, 7, "eth1".to_string(), true),
        ]
    );
    assert_eq!(
        found(&subnet_only),
        [
            (DEVICE_IP, 5, "eth0".to_string(), true),
            (SECOND_CAMERA, 7, "eth1".to_string(), true),
        ]
    );
}

/// Device events on the message channel reach every subscriber in order;
/// one that asks for an acknowledge gets it at its own source socket, one
/// that does not gets none, and a subscriber whose buffer is full loses the
/// overflow while one that keeps up still gets everything.
#[test]
fn device_events_reach_subscribers_and_are_acknowledged() {
    sim(34).run(|| {
        let device = Camera::with(CameraSpec {
            events: true,
            ..CameraSpec::default()
        });
        let cam = connected(GigeConfig {
            event_capacity: 2,
            ..bound_config(2)
        });
        let keeps_up = cam.events().unwrap();
        cam.enable_events().expect("enable events");
        let dest = device
            .device(|d| d.message_dest())
            .expect("message channel open");
        assert_eq!(dest, control_addr());
        let falls_behind = cam.events().unwrap();

        let source = UdpSocket::bind((DEVICE_IP, 0)).unwrap();
        source
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let mut buf = [0u8; 64];
        source
            .send_to(&emulator::event_cmd(0x9001, 1, 111, 77, true), dest)
            .unwrap();
        let (n, from) = source.recv_from(&mut buf).expect("event ack");
        let ack = gvcp::Ack::parse(&buf[..n]).unwrap();
        assert_eq!((ack.answer, ack.ack_id), (gvcp::EVENT_ACK, 77));
        assert_eq!(from.port(), CONTROL_PORT);
        let first = keeps_up.wait_for(Duration::from_secs(1)).expect("event");
        assert_eq!(
            (first.event_id, first.block_id, first.timestamp),
            (0x9001, 1, 111)
        );

        for (block, ts) in [(2, 222), (3, 333)] {
            source
                .send_to(&emulator::event_cmd(0x9002, block, ts, 78, false), dest)
                .unwrap();
        }
        let t0 = Instant::now();
        assert!(source.recv_from(&mut buf).is_err(), "unrequested ack");
        assert_costs(t0.elapsed(), Duration::from_millis(200), "no ack arrives");

        let seen = |ch: &telegenic::gige::EventChannel| {
            std::iter::from_fn(|| ch.try_recv())
                .map(|e| (e.event_id, e.block_id, e.timestamp))
                .collect::<Vec<_>>()
        };
        assert_eq!(seen(&keeps_up), [(0x9002, 2, 222), (0x9002, 3, 333)]);
        assert_eq!(
            seen(&falls_behind),
            [(0x9001, 1, 111), (0x9002, 2, 222)],
            "a full buffer of 2 drops the third"
        );
        assert_eq!(cam.stats().unwrap().events, 3);
        cam.disable_events().unwrap();
        assert_eq!(device.device(|d| d.message_dest()), None);
        drop(cam);
        device.stop();
    });
}
