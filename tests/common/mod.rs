//! An emulated GigE Vision camera on the simulated network, with knobs for
//! the faults a real link and a real device produce, and the sim and config
//! builders the snare-driven suites share.

#![cfg(snare)]
#![allow(dead_code)]

use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use snare::prelude::*;
use telegenic::emulator::{self, DeviceConfig, GigeDevice, ResendRequest};
use telegenic::gige::GigeConfig;
use telegenic::gige::proto::gvcp::{self, GVCP_PORT};

pub const DEVICE_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 5);
pub const HOST_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 1);
pub const WIDTH: u32 = 64;
pub const HEIGHT: u32 = 48;
pub const EXPOSURE: Duration = Duration::from_millis(30);
pub const DEVICE_MAC: [u8; 6] = [0x00, 0x11, 0x1c, 0x00, 0x00, 0x05];
/// The AcquisitionStart command register of the emulated device.
pub const ACQ_REG: u32 = 0x2008;
const UDP_OVERHEAD: usize = 28;
const QUIT: &[u8] = b"quit";

/// What the simulation adds on top of the waits themselves: a timer fires
/// 1 ns past its deadline and every call that returns without blocking costs
/// 1 µs. Far below the driver's shortest timeout (the 5 ms drain), so a span
/// inside this band of its expected value contains no hidden wait.
pub const SIM_OVERHEAD: Duration = Duration::from_micros(200);

pub fn sim(seed: u64) -> Sim {
    sim_with(seed, |b| b)
}

/// The host's adapter on the camera's segment: the host at `HOST_IP`, the
/// camera a separate station on the same /24, as on a real link.
pub fn eth0() -> NicSpec {
    NicSpec::new("eth0")
        .address(IpNet::new(HOST_IP.into(), 24))
        .station(DEVICE_IP)
}

pub fn sim_with(seed: u64, extra: impl FnOnce(SimBuilder) -> SimBuilder) -> Sim {
    bare_sim_with(seed, |b| extra(b.nic(eth0())))
}

/// A sim with no adapter, for tests that lay out their own.
pub fn bare_sim_with(seed: u64, extra: impl FnOnce(SimBuilder) -> SimBuilder) -> Sim {
    extra(
        Sim::builder()
            .deterministic()
            .seed(seed)
            .strict_sockopts()
            .stuck_after(Duration::from_secs(30)),
    )
    .build()
}

#[track_caller]
pub fn assert_costs(actual: Duration, expected: Duration, what: &str) {
    assert_within(actual, expected, SIM_OVERHEAD, what);
}

#[track_caller]
pub fn assert_within(actual: Duration, expected: Duration, slack: Duration, what: &str) {
    assert!(
        actual >= expected && actual - expected < slack,
        "{what}: took {actual:?}, expected {expected:?} (+ < {slack:?})"
    );
}

pub fn unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}

pub fn image(width: u32, height: u32) -> Vec<u8> {
    (0..width * height).map(|i| (i * 7 % 251) as u8).collect()
}

pub fn config(retries: u8) -> GigeConfig {
    let mut cfg = GigeConfig::new(DEVICE_IP);
    cfg.gvcp_timeout = Duration::from_millis(500);
    cfg.retries = retries;
    cfg
}

/// The register (or memory) address a READREG/WRITEREG/READMEM/WRITEMEM
/// command addresses first.
pub fn first_addr(cmd: &gvcp::Cmd<'_>) -> Option<u32> {
    match cmd.command {
        gvcp::READ_REGISTER_CMD
        | gvcp::WRITE_REGISTER_CMD
        | gvcp::READ_MEMORY_CMD
        | gvcp::WRITE_MEMORY_CMD => {
            let a = cmd.payload.get(..4)?;
            Some(u32::from_be_bytes([a[0], a[1], a[2], a[3]]))
        }
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct CameraSpec {
    pub ip: Ipv4Addr,
    pub mac: [u8; 6],
    pub serial: String,
    pub width: u32,
    pub height: u32,
    pub exposure: Duration,
    pub packet_resend: bool,
    pub events: bool,
    pub max_packet_size: Option<u16>,
    /// The block id of the first frame; ids wrap 0xffff -> 1.
    pub first_frame_id: u64,
}

impl Default for CameraSpec {
    fn default() -> Self {
        Self {
            ip: DEVICE_IP,
            mac: DEVICE_MAC,
            serial: "THEATER-CAM-0001".into(),
            width: WIDTH,
            height: HEIGHT,
            exposure: EXPOSURE,
            packet_resend: false,
            events: false,
            max_packet_size: None,
            first_frame_id: 1,
        }
    }
}

/// Rewrites the device's acknowledge for a command into the datagrams
/// actually sent back to `src` (none drops it).
pub type AckFilter = Box<dyn FnMut(&gvcp::Cmd<'_>, SocketAddr, Vec<u8>) -> Vec<Vec<u8>> + Send>;

/// A PENDING_ACK answer to commands addressing one register.
#[derive(Debug, Clone, Copy)]
pub struct Pending {
    pub timeout_ms: u16,
    /// When the real acknowledge follows; `None` never, and every
    /// retransmission is answered with another PENDING_ACK.
    pub answer_after: Option<Duration>,
}

#[derive(Default)]
pub struct Faults {
    /// Ignores every datagram, as a camera that lost power does.
    pub silent: bool,
    /// Drops this many of the next commands addressing the register before
    /// the device sees them (`usize::MAX`: all of them).
    pub ignore: HashMap<u32, usize>,
    pub pending: HashMap<u32, Pending>,
    pub ack_filter: Option<AckFilter>,
    /// GVSP packet ids lost on the first transmission of the next frame.
    pub gvsp_lost: Vec<u32>,
    /// The burst indices (= packet ids) the next frame sends, in this order;
    /// repeating one duplicates it.
    pub gvsp_order: Option<Vec<usize>>,
    /// Goes silent after sending this many packets of the next frame.
    pub vanish_after_packets: Option<usize>,
    /// Leaves resend requests unanswered.
    pub ignore_resends: bool,
    /// Frame ids the device consumes without sending, before the next frame.
    pub skip_frame_ids: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Command {
    pub at: Instant,
    pub sys: SystemTime,
    pub command: u16,
    pub req_id: u16,
    pub addr: Option<u32>,
}

#[derive(Debug, Clone, Default)]
pub struct Log {
    /// Every datagram that reached the GVCP port, answered or not.
    pub datagrams: u64,
    pub commands: Vec<Command>,
    pub resends: Vec<ResendRequest>,
    pub acquisitions: u64,
    /// Frame ids streamed, in order.
    pub frames: Vec<u64>,
    /// When each frame's first packet went out.
    pub bursts: Vec<SystemTime>,
}

impl Log {
    pub fn commands_to(&self, addr: u32) -> Vec<Command> {
        self.commands
            .iter()
            .filter(|c| c.addr == Some(addr))
            .copied()
            .collect()
    }
}

struct State {
    device: GigeDevice,
    faults: Faults,
    log: Log,
    next_frame_id: u64,
    sent_frames: HashMap<u16, Vec<Vec<u8>>>,
    delayed: Vec<JoinHandle<()>>,
}

struct Acquisition {
    frame_id: u64,
    dest: SocketAddr,
    at_ns: u64,
}

/// A camera at `spec.ip`: a control plane answering GVCP at once, and a data
/// plane that exposes for `spec.exposure` before streaming the frame, its
/// leader stamped with the acquisition instant. Stopped by [`Camera::stop`].
pub struct Camera {
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
    threads: Vec<JoinHandle<()>>,
    width: u32,
    height: u32,
}

impl Camera {
    pub fn spawn() -> Self {
        Self::with(CameraSpec::default())
    }

    pub fn with(spec: CameraSpec) -> Self {
        let ip = IpAddr::V4(spec.ip);
        let addr = SocketAddr::new(ip, GVCP_PORT);
        let control = Arc::new(UdpSocket::bind(addr).expect("bind gvcp"));
        let gvsp = Arc::new(UdpSocket::bind(SocketAddr::new(ip, 0)).expect("bind gvsp"));
        let cfg = DeviceConfig {
            width: spec.width,
            height: spec.height,
            mac: spec.mac,
            serial: spec.serial.clone(),
            ..Default::default()
        };
        let mut device = GigeDevice::new(spec.ip, &cfg);
        device.set_packet_resend(spec.packet_resend);
        device.set_event_support(spec.events);
        device.set_max_packet_size(spec.max_packet_size);
        let state = Arc::new(Mutex::new(State {
            device,
            faults: Faults::default(),
            log: Log::default(),
            next_frame_id: spec.first_frame_id,
            sent_frames: HashMap::new(),
            delayed: Vec::new(),
        }));
        let (jobs, queue) = mpsc::channel::<Acquisition>();

        let data_plane = {
            let state = Arc::clone(&state);
            let gvsp = Arc::clone(&gvsp);
            let (exposure, pixels) = (spec.exposure, image(spec.width, spec.height));
            std::thread::Builder::new()
                .name("fake-gvsp".into())
                .spawn(move || {
                    while let Ok(acq) = queue.recv() {
                        std::thread::sleep(exposure);
                        stream_frame(&state, &gvsp, &acq, &pixels);
                    }
                })
                .unwrap()
        };

        let control_plane = {
            let state = Arc::clone(&state);
            std::thread::Builder::new()
                .name("fake-gvcp".into())
                .spawn(move || {
                    let mut buf = [0u8; 0xffff];
                    loop {
                        // Windows reports an ICMP port unreachable for an
                        // earlier ack on the next receive; a device ignores it.
                        let (n, src) = match control.recv_from(&mut buf) {
                            Ok(r) => r,
                            Err(e) if e.kind() == ErrorKind::ConnectionReset => continue,
                            Err(e) => panic!("gvcp recv: {e}"),
                        };
                        if &buf[..n] == QUIT {
                            return;
                        }
                        on_datagram(&state, &control, &gvsp, &jobs, &buf[..n], src);
                    }
                })
                .unwrap()
        };
        Self {
            addr,
            state,
            threads: vec![control_plane, data_plane],
            width: spec.width,
            height: spec.height,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn image(&self) -> Vec<u8> {
        image(self.width, self.height)
    }

    pub fn faults(&self, change: impl FnOnce(&mut Faults)) {
        change(&mut self.lock().faults);
    }

    pub fn log(&self) -> Log {
        self.lock().log.clone()
    }

    pub fn device<R>(&self, read: impl FnOnce(&GigeDevice) -> R) -> R {
        read(&self.lock().device)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    /// Stops the device threads. The stop request comes from the device's
    /// own address, so no host route or link state stands in its way, and
    /// any link policy on the GVCP address is lifted first.
    pub fn stop(self) {
        snare::set_udp_policy(self.addr, |p| *p = UdpPolicy::default());
        let s = UdpSocket::bind(SocketAddr::new(self.addr.ip(), 0)).unwrap();
        s.send_to(QUIT, self.addr).unwrap();
        for t in self.threads {
            t.join().expect("device thread");
        }
        let delayed = std::mem::take(&mut self.state.lock().unwrap().delayed);
        for t in delayed {
            t.join().expect("delayed ack thread");
        }
    }
}

fn on_datagram(
    state: &Arc<Mutex<State>>,
    control: &Arc<UdpSocket>,
    gvsp: &UdpSocket,
    jobs: &mpsc::Sender<Acquisition>,
    datagram: &[u8],
    src: SocketAddr,
) {
    let mut st = state.lock().unwrap();
    st.log.datagrams += 1;
    let cmd = gvcp::Cmd::parse(datagram);
    let addr = cmd.as_ref().and_then(first_addr);
    if let Some(cmd) = &cmd {
        st.log.commands.push(Command {
            at: Instant::now(),
            sys: SystemTime::now(),
            command: cmd.command,
            req_id: cmd.req_id,
            addr,
        });
    }
    if st.faults.silent {
        return;
    }
    if let Some(a) = addr
        && let Some(left) = st.faults.ignore.get_mut(&a)
        && *left > 0
    {
        if *left != usize::MAX {
            *left -= 1;
        }
        return;
    }
    if let (Some(cmd), Some(a)) = (&cmd, addr)
        && let Some(pending) = st.faults.pending.get(&a).copied()
    {
        control
            .send_to(&emulator::pending_ack(cmd.req_id, pending.timeout_ms), src)
            .unwrap();
        let Some(after) = pending.answer_after else {
            return;
        };
        st.faults.pending.remove(&a);
        let (state2, control2, datagram) =
            (Arc::clone(state), Arc::clone(control), datagram.to_vec());
        let late = std::thread::Builder::new()
            .name("fake-gvcp-late".into())
            .spawn(move || {
                std::thread::sleep(after);
                let reply = state2
                    .lock()
                    .unwrap()
                    .device
                    .handle_datagram(&datagram, src)
                    .reply;
                if let Some(reply) = reply {
                    control2.send_to(&reply, src).unwrap();
                }
            })
            .unwrap();
        st.delayed.push(late);
        return;
    }

    let reaction = st.device.handle_datagram(datagram, src);
    if let Some(reply) = reaction.reply {
        let out = match (&cmd, st.faults.ack_filter.as_mut()) {
            (Some(cmd), Some(filter)) => filter(cmd, src, reply),
            _ => vec![reply],
        };
        for d in out {
            control.send_to(&d, src).unwrap();
        }
    }
    if let Some(req) = reaction.resend {
        st.log.resends.push(req);
        if !st.faults.ignore_resends
            && let Some(dest) = st.device.stream_dest()
            && let Some(packets) = st.sent_frames.get(&(req.frame_id as u16))
        {
            for id in req.first_packet..=req.last_packet {
                if let Some(p) = packets.get(id as usize) {
                    let mut p = p.clone();
                    emulator::mark_resent(&mut p);
                    gvsp.send_to(&p, dest).unwrap();
                }
            }
        }
    }
    if let Some(size) = reaction.fire_test
        && let Some(dest) = st.device.stream_dest()
    {
        let len = usize::from(size).saturating_sub(UDP_OVERHEAD);
        let _ = gvsp.send_to(&vec![0u8; len], dest);
    }
    if reaction.acquisition_started {
        st.log.acquisitions += 1;
        if let Some(dest) = st.device.stream_dest() {
            for _ in 0..std::mem::take(&mut st.faults.skip_frame_ids) {
                st.next_frame_id = next_block_id(st.next_frame_id);
            }
            let frame_id = st.next_frame_id;
            st.next_frame_id = next_block_id(frame_id);
            jobs.send(Acquisition {
                frame_id,
                dest,
                at_ns: unix_ns(),
            })
            .unwrap();
        }
        st.device.clear_acquisition();
    }
}

fn next_block_id(id: u64) -> u64 {
    if id >= 0xffff { 1 } else { id + 1 }
}

fn stream_frame(state: &Mutex<State>, gvsp: &UdpSocket, acq: &Acquisition, pixels: &[u8]) {
    let (packets, order, lost, vanish_after) = {
        let mut st = state.lock().unwrap();
        if st.faults.silent {
            return;
        }
        st.device.set_leader_timestamp_ns(acq.at_ns);
        let packets = st.device.frame_packets(acq.frame_id, pixels);
        st.sent_frames.insert(acq.frame_id as u16, packets.clone());
        st.log.frames.push(acq.frame_id);
        st.log.bursts.push(SystemTime::now());
        let order = st
            .faults
            .gvsp_order
            .take()
            .unwrap_or_else(|| (0..packets.len()).collect());
        let lost = std::mem::take(&mut st.faults.gvsp_lost);
        (packets, order, lost, st.faults.vanish_after_packets.take())
    };
    for (sent, index) in order.into_iter().enumerate() {
        if vanish_after == Some(sent) {
            state.lock().unwrap().faults.silent = true;
            return;
        }
        if lost.contains(&(index as u32)) {
            continue;
        }
        gvsp.send_to(&packets[index], acq.dest).unwrap();
    }
}
