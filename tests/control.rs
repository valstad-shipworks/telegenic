//! Integration tests for the GVCP control driver against the fake camera,
//! each inside a deterministic snare simulation.

#![cfg(all(
    snare,
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

use fake_camera::{FakeCamera, sim};
use telegenic::CameraError;
use telegenic::gige::proto::bootstrap;
use telegenic::gige::{GigECamera, GigeConfig};

/// The worker's poll period: deadlines are noticed at its next wake.
const POLL: Duration = Duration::from_millis(10);

fn config_for(fake: &FakeCamera) -> GigeConfig {
    let mut cfg = GigeConfig::new(std::net::Ipv4Addr::LOCALHOST);
    cfg.addr = fake.addr();
    cfg.gvcp_timeout = Duration::from_millis(500);
    cfg.retries = 2;
    cfg
}

fn connect(fake: &FakeCamera) -> GigECamera {
    let mut cam = GigECamera::with_config(config_for(fake));
    cam.connect().expect("connect to fake camera");
    cam
}

#[test]
fn connect_reads_identity_and_takes_control() {
    sim(1).run(|| {
        let fake = FakeCamera::start();
        let cam = connect(&fake);

        let info = cam.device_info().expect("device info");
        assert_eq!(info.manufacturer, "FakeWorks");
        assert_eq!(info.model, "Fake2000");
        assert_eq!(info.serial, "FK-0001");
        assert_eq!(info.spec_version, (2, 0));
        assert_eq!(info.mac, [0xaa, 0xbb, 0xcc, 0x00, 0x00, 0x01]);

        let capabilities = cam.capabilities().expect("capabilities");
        assert!(capabilities & bootstrap::CAP_PACKET_RESEND != 0);
        assert!(capabilities & bootstrap::CAP_PENDING_ACK != 0);

        assert_eq!(
            fake.read_reg(bootstrap::CONTROL_CHANNEL_PRIVILEGE),
            bootstrap::CCP_CONTROL,
            "connect should take (non-exclusive) control"
        );
        assert_eq!(fake.read_reg(bootstrap::HEARTBEAT_TIMEOUT), 3000);
        assert!(cam.is_connected());
    });
}

#[test]
fn construction_is_free_and_disconnected() {
    let cam = GigECamera::new(std::net::Ipv4Addr::LOCALHOST);
    assert!(!cam.is_connected());
    assert!(matches!(cam.device_info(), Err(CameraError::Disconnected)));
    assert!(matches!(
        cam.read_register(0),
        Err(CameraError::Disconnected)
    ));
    assert!(cam.stats().is_none());
}

#[test]
fn connect_is_idempotent() {
    sim(2).run(|| {
        let fake = FakeCamera::start();
        let mut cam = connect(&fake);
        let before = cam.stats().expect("stats").commands;
        let datagrams = fake.counters.datagrams.load(Ordering::Relaxed);
        cam.connect().expect("second connect is a no-op");
        assert_eq!(cam.stats().expect("stats").commands, before);
        assert_eq!(fake.counters.datagrams.load(Ordering::Relaxed), datagrams);
    });
}

#[test]
fn register_roundtrip() {
    sim(3).run(|| {
        let fake = FakeCamera::start();
        let cam = connect(&fake);

        cam.write_register(0x2000, 0xdead_beef)
            .expect("submit")
            .wait()
            .expect("write register");
        let value = cam
            .read_register(0x2000)
            .expect("submit")
            .wait()
            .expect("read register");
        assert_eq!(value, 0xdead_beef);

        let values = cam
            .read_registers(vec![0x2000, bootstrap::VERSION])
            .expect("submit")
            .wait()
            .expect("read registers");
        assert_eq!(values, vec![0xdead_beef, 0x0002_0000]);
    });
}

#[test]
fn memory_io_chunks_across_transactions() {
    sim(4).run(|| {
        let fake = FakeCamera::start();
        let cam = connect(&fake);
        let acks = cam.stats().expect("stats").acks;

        let pattern: Vec<u8> = (0..1500u32).map(|i| (i * 7) as u8).collect();
        fake.write_mem(0x4000, &pattern);
        let read = cam
            .read_memory(0x4000, 1500)
            .expect("submit")
            .wait()
            .expect("read memory");
        assert_eq!(read, pattern);

        let data: Vec<u8> = (0..1024u32).map(|i| (i * 3) as u8).collect();
        cam.write_memory(0x6000, data.clone())
            .expect("submit")
            .wait()
            .expect("write memory");
        assert_eq!(fake.read_mem(0x6000, 1024), data);

        // 1500 B read = 3 chunks, 1024 B write = 2 chunks.
        assert_eq!(cam.stats().expect("stats").acks - acks, 5);
    });
}

#[test]
fn unaligned_memory_access_fails_fast() {
    sim(5).run(|| {
        let fake = FakeCamera::start();
        let cam = connect(&fake);
        let commands = cam.stats().expect("stats").commands;

        let err = cam
            .read_memory(0x4001, 8)
            .expect("submit")
            .wait()
            .unwrap_err();
        assert!(matches!(*err, CameraError::Protocol(_)));
        let err = cam
            .write_memory(0x4000, vec![1, 2, 3])
            .expect("submit")
            .wait()
            .unwrap_err();
        assert!(matches!(*err, CameraError::Protocol(_)));
        assert_eq!(
            cam.stats().expect("stats").commands,
            commands,
            "rejected before reaching the wire"
        );
    });
}

#[test]
fn persistent_loss_times_out() {
    sim(6).run(|| {
        let fake = FakeCamera::start();
        let mut cfg = config_for(&fake);
        cfg.gvcp_timeout = Duration::from_millis(100);
        let mut cam = GigECamera::with_config(cfg);
        cam.connect().expect("connect to fake camera");

        fake.knobs().lock().drop_next = 3;
        let t0 = Instant::now();
        let err = cam
            .read_register(bootstrap::VERSION)
            .expect("submit")
            .wait()
            .unwrap_err();
        let took = t0.elapsed();
        assert!(matches!(*err, CameraError::Timeout), "got {err}");
        assert!(
            took >= Duration::from_millis(300) && took < Duration::from_millis(300) + 3 * POLL,
            "three 100 ms tries, took {took:?}"
        );
        let stats = cam.stats().expect("stats");
        assert_eq!((stats.retries, stats.timeouts), (2, 1));
        // The link stays up; the next transaction succeeds.
        assert!(cam.is_connected());
        cam.read_register(bootstrap::VERSION)
            .expect("submit")
            .wait()
            .expect("read after timeout");
    });
}

#[test]
fn control_denied_fails_connect() {
    sim(7).run(|| {
        let fake = FakeCamera::start();
        fake.knobs().lock().deny_control = true;
        let mut cam = GigECamera::with_config(config_for(&fake));
        let err = cam.connect().unwrap_err();
        assert!(matches!(err, CameraError::ControlDenied), "got {err}");
        assert!(!cam.is_connected());

        // The camera value stays usable: clear the knob and redial.
        fake.knobs().lock().deny_control = false;
        cam.connect().expect("connect after denial cleared");
        assert!(cam.is_connected());
    });
}

#[test]
fn heartbeat_keeps_control_and_detects_loss() {
    sim(8).run(|| {
        let fake = FakeCamera::start();
        let mut cfg = config_for(&fake);
        cfg.heartbeat_timeout_ms = 90; // worker heartbeats every 30ms
        let mut cam = GigECamera::with_config(cfg);
        cam.connect().expect("connect");

        let period = Duration::from_millis(30);
        std::thread::sleep(3 * period + POLL);
        let beats = fake.counters.ccp_reads.load(Ordering::Relaxed);
        assert!(beats >= 3, "expected a heartbeat per period, saw {beats}");
        assert!(cam.is_connected());

        // Another application takes the device: the next heartbeat reads a
        // cleared CCP and the worker stops.
        fake.clear_ccp();
        std::thread::sleep(period + POLL);
        assert!(!cam.is_connected(), "control loss should stop the worker");
        let err = cam.read_register(bootstrap::VERSION).unwrap_err();
        assert!(matches!(err, CameraError::ControlLost), "got {err}");

        // connect() doubles as the recovery path from a lost link.
        cam.connect().expect("reconnect after control loss");
        assert!(cam.is_connected());
    });
}
