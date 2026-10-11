//! Live video in a window, leaving the camera's configuration as it found it:
//! `cargo run --release --example live <camera-ip> [max-packet-size]`
//!
//! No GenICam feature is written: exposure, gain, ROI, pixel format,
//! `AcquisitionMode`, triggering and `TLParamsLocked` are all left alone, so
//! the camera streams however it is currently configured. The only writes are
//! the ones GigE Vision cannot stream without: control privilege, the
//! heartbeat timeout and `SCPS` packet size (both written back with the values
//! read from the device beforehand, `SCPS` flag bits included), the stream
//! destination address and port, and
//! `AcquisitionStart` / `AcquisitionStop`. The bootstrap registers are read
//! before and after the session and any difference is printed.
//!
//! When the camera's packet size is larger than the host's NIC can receive
//! (e.g. jumbo packets on a 1500-MTU adapter), pass `max-packet-size`: `SCPS`
//! is lowered to it for the session and written back to its original value
//! before disconnecting.
//!
//! Close the window or press Esc to stop. Killing the process instead leaves
//! the camera streaming until its heartbeat expires.

use std::net::{IpAddr, UdpSocket};
use std::time::{Duration, Instant};

use minifb::{Key, ScaleMode, Window, WindowOptions};
use telegenic::gige::proto::{bootstrap, gvcp};
use telegenic::gige::{GigECamera, GigeConfig};
use telegenic::{
    Frame, FrameChannel, FrameStatus, GenICamera, PacketSize, PixelFormat, StreamChannel,
    StreamConfig,
};

const WATCHED: [(&str, u32); 5] = [
    ("heartbeat timeout", bootstrap::HEARTBEAT_TIMEOUT),
    ("SCPS (packet size)", bootstrap::STREAM_CHANNEL_PACKET_SIZE),
    (
        "SCPD (packet delay)",
        bootstrap::STREAM_CHANNEL_PACKET_DELAY,
    ),
    (
        "SCDA (stream dest ip)",
        bootstrap::STREAM_CHANNEL_DEST_ADDRESS,
    ),
    ("SCP (stream dest port)", bootstrap::STREAM_CHANNEL_PORT),
];

fn main() {
    tracing_subscriber::fmt::init();
    let mut args = std::env::args().skip(1);
    let ip: IpAddr = args
        .next()
        .expect("usage: live <camera-ip> [max-packet-size]")
        .parse()
        .expect("camera ip");
    let max_packet_size: Option<u32> = args.next().map(|s| s.parse().expect("max packet size"));

    let before = peek_registers(ip).expect("read bootstrap registers");
    let [heartbeat_ms, scps, ..] = before;
    let packet_size = scps & bootstrap::SCPS_PACKET_SIZE_MASK;
    if packet_size == 0 {
        eprintln!("SCPS is {scps:#010x}: no packet size configured, so not streaming");
        std::process::exit(1);
    }
    let session_size = max_packet_size.map_or(packet_size, |max| packet_size.min(max));
    if session_size != packet_size {
        println!("packet size {packet_size} lowered to {session_size} for this session");
    }
    let scps_flags = scps & !bootstrap::SCPS_PACKET_SIZE_MASK & !bootstrap::SCPS_FIRE_TEST_PACKET;

    let mut cam = GenICamera::from_transport(GigECamera::with_config(GigeConfig {
        heartbeat_timeout_ms: heartbeat_ms,
        ..GigeConfig::new(ip)
    }));
    cam.connect().expect("connect");
    let info = cam.transport().device_info().expect("device info").clone();
    println!(
        "{} {} (serial {})",
        info.manufacturer, info.model, info.serial
    );
    for feature in ["PixelFormat", "AcquisitionMode", "TriggerMode"] {
        match cam.get_enum(feature) {
            Ok(v) => println!("  {feature} = {v}"),
            Err(e) => println!("  {feature}: {e}"),
        }
    }
    if cam.get_enum("TriggerMode").is_ok_and(|m| m == "On") {
        println!("  trigger mode is on: frames only arrive when the camera is triggered");
    }
    let mut window = open_window(
        cam.get_integer("Width").map_or(640, |v| v as usize),
        cam.get_integer("Height").map_or(480, |v| v as usize),
    );

    let cfg = StreamConfig {
        payload_size: Some(
            usize::try_from(cam.get_integer("PayloadSize").expect("PayloadSize"))
                .expect("PayloadSize"),
        ),
        packet_size: PacketSize::Fixed(session_size as u16),
        ..StreamConfig::new()
    };
    let capacity = cfg.n_buffers;
    let stream = cam.transport().open_stream(cfg).expect("open stream");
    // open_stream writes the bare size; put back the flag bits it cleared.
    cam.transport()
        .write_register(
            bootstrap::STREAM_CHANNEL_PACKET_SIZE,
            session_size | scps_flags,
        )
        .expect("submit")
        .wait()
        .expect("restore SCPS flags");
    let frames = stream.subscribe(capacity);
    cam.execute("AcquisitionStart").expect("AcquisitionStart");
    println!(
        "streaming to {} with packet size {}",
        stream.local_addr(),
        stream.packet_size()
    );

    show(&mut window, &frames, &stream);
    drop(window);

    if let Err(e) = cam.execute("AcquisitionStop") {
        eprintln!("AcquisitionStop failed: {e}");
    }
    println!("stream stats: {:?}", stream.stats());
    drop(frames);
    drop(stream);
    if session_size != packet_size
        && let Err(e) = cam
            .transport()
            .write_register(
                bootstrap::STREAM_CHANNEL_PACKET_SIZE,
                packet_size | scps_flags,
            )
            .expect("submit")
            .wait()
    {
        eprintln!("SCPS not restored to {scps:#010x}: {e}");
    }
    cam.disconnect(Duration::from_millis(500));

    match peek_registers(ip) {
        Ok(after) => {
            println!("bootstrap registers, before -> after:");
            for ((name, _), (b, a)) in WATCHED.iter().zip(before.iter().zip(after)) {
                let mark = if *b == a { "" } else { "  (changed)" };
                println!("  {name:<24} {b:#010x} -> {a:#010x}{mark}");
            }
        }
        Err(e) => eprintln!("could not re-read bootstrap registers: {e}"),
    }
}

fn show(window: &mut Window, frames: &FrameChannel, stream: &StreamChannel) {
    let mut rgb = Vec::new();
    let mut shown = 0u32;
    let mut skipped = 0u64;
    let mut window_start = Instant::now();

    while window.is_open() && !window.is_key_down(Key::Escape) {
        let Some(mut frame) = frames.wait_for(Duration::from_millis(30)) else {
            window.update();
            continue;
        };
        if let Some(latest) = frames.recv_all().pop() {
            frame = latest;
        }
        if frame.status != FrameStatus::Complete {
            skipped += 1;
            window.update();
            continue;
        }

        let (w, h) = (frame.width as usize, frame.height as usize);
        let converted = to_rgb(&frame, &mut rgb);
        if converted {
            window.update_with_buffer(&rgb, w, h).expect("draw frame");
            shown += 1;
        } else {
            window.update();
        }

        let elapsed = window_start.elapsed();
        if elapsed >= Duration::from_secs(1) {
            let stats = stream.stats();
            window.set_title(&format!(
                "{w}x{h} {}{}  {:.1} fps  {} incomplete  {} missing packets",
                frame.pixel_format,
                if converted {
                    ""
                } else {
                    " (no viewer for this format)"
                },
                f64::from(shown) / elapsed.as_secs_f64(),
                skipped,
                stats.missing_packets,
            ));
            shown = 0;
            window_start = Instant::now();
        }
    }
}

fn open_window(width: usize, height: usize) -> Window {
    let fit = (1280.0 / width as f64).min(960.0 / height as f64).min(1.0);
    Window::new(
        "telegenic live",
        ((width as f64 * fit) as usize).max(1),
        ((height as f64 * fit) as usize).max(1),
        WindowOptions {
            resize: true,
            scale_mode: ScaleMode::AspectRatioStretch,
            ..WindowOptions::default()
        },
    )
    .expect("open window")
}

/// Read the watched bootstrap registers over a throwaway GVCP socket, without
/// taking control of the device. One register per request: some devices
/// answer a multi-register read with `DATA_OVERRUN`.
fn peek_registers(ip: IpAddr) -> std::io::Result<[u32; WATCHED.len()]> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.set_read_timeout(Some(Duration::from_millis(500)))?;
    let mut values = [0u32; WATCHED.len()];
    let mut id = 0u16;
    for (value, (name, addr)) in values.iter_mut().zip(WATCHED) {
        *value = read_register(&socket, ip, addr, &mut id)
            .map_err(|e| std::io::Error::new(e.kind(), format!("{name}: {e}")))?;
    }
    Ok(values)
}

fn read_register(socket: &UdpSocket, ip: IpAddr, addr: u32, id: &mut u16) -> std::io::Result<u32> {
    let mut buf = [0u8; 1024];
    for _ in 0..4 {
        *id = gvcp::next_id(*id);
        socket.send_to(&gvcp::encode_read_reg(&[addr], *id), (ip, gvcp::GVCP_PORT))?;
        let Ok(n) = socket.recv(&mut buf) else {
            continue;
        };
        let Some(ack) = gvcp::Ack::parse(&buf[..n]) else {
            continue;
        };
        if ack.ack_id != *id || ack.answer != gvcp::READ_REGISTER_ACK {
            continue;
        }
        if ack.status.is_error() {
            return Err(std::io::Error::other(format!(
                "device refused register read: {:?}",
                ack.status
            )));
        }
        return ack
            .register_values()
            .next()
            .ok_or_else(|| std::io::Error::other("empty read acknowledge"));
    }
    Err(std::io::ErrorKind::TimedOut.into())
}

/// Convert a frame to minifb's 0RGB pixels; `false` for formats it can't show.
fn to_rgb(frame: &Frame, out: &mut Vec<u32>) -> bool {
    let (w, h) = (frame.width as usize, frame.height as usize);
    let format = frame.pixel_format;
    let bytes_pp = format.bits_per_pixel() as usize / 8;
    let stride = w * bytes_pp + usize::from(frame.x_padding);
    let data = frame.data();
    if w == 0 || h == 0 || bytes_pp == 0 || data.len() < stride * (h - 1) + w * bytes_pp {
        return false;
    }
    let at = |x: usize, y: usize, byte: usize| data[y * stride + x * bytes_pp + byte];
    let rgb = |r: u8, g: u8, b: u8| u32::from(r) << 16 | u32::from(g) << 8 | u32::from(b);

    out.clear();
    out.reserve(w * h);
    let mut fill = |pixel: &dyn Fn(usize, usize) -> u32| {
        for y in 0..h {
            out.extend((0..w).map(|x| pixel(x, y)));
        }
    };
    match format {
        PixelFormat::MONO8 => fill(&|x, y| {
            let v = at(x, y, 0);
            rgb(v, v, v)
        }),
        PixelFormat::MONO10 | PixelFormat::MONO12 | PixelFormat::MONO16 => {
            let shift = match format {
                PixelFormat::MONO10 => 2,
                PixelFormat::MONO12 => 4,
                _ => 8,
            };
            fill(&|x, y| {
                let raw = u16::from_le_bytes([at(x, y, 0), at(x, y, 1)]);
                let v = (raw >> shift).min(255) as u8;
                rgb(v, v, v)
            })
        }
        PixelFormat::RGB8 => fill(&|x, y| rgb(at(x, y, 0), at(x, y, 1), at(x, y, 2))),
        PixelFormat::BGR8 => fill(&|x, y| rgb(at(x, y, 2), at(x, y, 1), at(x, y, 0))),
        PixelFormat::BAYER_RG8 | PixelFormat::BAYER_GB8 => {
            if w < 2 || h < 2 {
                return false;
            }
            // Nearest-neighbour over each 2x2 cell: [[a, b], [c, d]].
            let rg = format == PixelFormat::BAYER_RG8;
            fill(&|x, y| {
                let (x0, y0) = ((x & !1).min(w - 2), (y & !1).min(h - 2));
                let a = at(x0, y0, 0);
                let b = at(x0 + 1, y0, 0);
                let c = at(x0, y0 + 1, 0);
                let d = at(x0 + 1, y0 + 1, 0);
                if rg {
                    rgb(a, ((u16::from(b) + u16::from(c)) / 2) as u8, d)
                } else {
                    rgb(c, ((u16::from(a) + u16::from(d)) / 2) as u8, b)
                }
            })
        }
        PixelFormat::YUV422_8 => fill(&|x, y| {
            // YUYV: each pixel pair shares one U and one V sample.
            let pair = x & !1;
            let luma = f32::from(at(x, y, 0));
            let u = f32::from(at(pair, y, 1)) - 128.0;
            let v = f32::from(at((pair + 1).min(w - 1), y, 1)) - 128.0;
            let clamp = |c: f32| c.clamp(0.0, 255.0) as u8;
            rgb(
                clamp(luma + 1.402 * v),
                clamp(luma - 0.344 * u - 0.714 * v),
                clamp(luma + 1.772 * u),
            )
        }),
        _ => return false,
    }
    true
}
