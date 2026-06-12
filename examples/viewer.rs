//! Live egui viewer: `cargo run --release --example viewer <camera-ip>`
//!
//! Streams at whatever rate the camera and link sustain: the frame channel
//! is drained to the newest frame every repaint, so a slow UI drops stale
//! frames instead of building latency.

use std::time::{Duration, Instant};

use eframe::egui;
use telegenic::{FrameChannel, FrameStatus, GenICamera, PixelFormat, StreamChannel, StreamConfig};

const EXPOSURE_US: f64 = 10_000.0;
const GAIN: f64 = 30.0;

fn main() -> eframe::Result {
    tracing_subscriber::fmt::init();
    let ip: std::net::IpAddr = std::env::args()
        .nth(1)
        .expect("usage: viewer <camera-ip>")
        .parse()
        .expect("camera ip");

    let mut cam = GenICamera::new(ip);
    cam.connect().expect("connect");
    {
        let info = cam.transport().device_info().expect("device info");
        println!(
            "{} {} (serial {})",
            info.manufacturer, info.model, info.serial
        );
    }

    set_number(&mut cam, "ExposureTime", EXPOSURE_US);
    set_number(&mut cam, "Gain", GAIN);
    if let Err(e) = cam.set_enum("PixelFormat", "Mono8") {
        println!("PixelFormat not set to Mono8: {e}");
    }
    if let Err(e) = cam.set_enum("TriggerMode", "Off") {
        println!("TriggerMode not set to Off: {e}");
    }
    // Free-run as fast as the sensor allows.
    if let Ok((_, max)) = cam.float_bounds("AcquisitionFrameRate") {
        let _ = cam.set_boolean("AcquisitionFrameRateEnable", true);
        match cam.set_float("AcquisitionFrameRate", max) {
            Ok(()) => println!("AcquisitionFrameRate = {max:.1}"),
            Err(e) => println!("AcquisitionFrameRate not raised: {e}"),
        }
    }

    let mut cfg = StreamConfig::new(0);
    cfg.n_buffers = 16;
    let stream = cam.start_acquisition(cfg).expect("start acquisition");
    println!(
        "streaming to {} with packet size {}",
        stream.local_addr(),
        stream.packet_size()
    );
    let frames = stream.subscribe(4);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1280.0, 1080.0]),
        ..Default::default()
    };
    eframe::run_native(
        "telegenic viewer",
        options,
        Box::new(move |_cc| Ok(Box::new(Viewer::new(cam, stream, frames)))),
    )
}

/// Camera features come as Float or Integer depending on the vendor.
fn set_number(cam: &mut GenICamera, name: &str, value: f64) {
    let result = cam
        .set_float(name, value)
        .or_else(|_| cam.set_integer(name, value as i64));
    match result {
        Ok(()) => println!("{name} = {value}"),
        Err(e) => println!("{name} not set: {e}"),
    }
}

struct Viewer {
    cam: GenICamera,
    stream: StreamChannel,
    frames: FrameChannel,
    texture: Option<egui::TextureHandle>,
    frame_info: String,
    fps: f32,
    fps_window_start: Instant,
    fps_window_frames: u32,
    last_log: Instant,
}

impl Viewer {
    fn new(cam: GenICamera, stream: StreamChannel, frames: FrameChannel) -> Self {
        Self {
            cam,
            stream,
            frames,
            texture: None,
            frame_info: String::new(),
            fps: 0.0,
            fps_window_start: Instant::now(),
            fps_window_frames: 0,
            last_log: Instant::now(),
        }
    }

    fn poll_frames(&mut self, ctx: &egui::Context) {
        // Newest frame wins; everything older was already stale.
        let mut latest = None;
        while let Some(frame) = self.frames.try_recv() {
            latest = Some(frame);
        }
        let Some(frame) = latest else { return };
        if frame.status != FrameStatus::Complete || frame.pixel_format != PixelFormat::MONO8 {
            self.frame_info = format!(
                "frame {} skipped: {:?} {}",
                frame.frame_id, frame.status, frame.pixel_format
            );
            return;
        }

        let (w, h) = (frame.width as usize, frame.height as usize);
        let pixels = w * h;
        let data = frame.data();
        if data.len() < pixels {
            self.frame_info = format!("frame {} short: {} < {pixels}", frame.frame_id, data.len());
            return;
        }
        let image = egui::ColorImage::from_gray([w, h], &data[..pixels]);
        match &mut self.texture {
            Some(texture) => texture.set(image, egui::TextureOptions::LINEAR),
            None => {
                self.texture =
                    Some(ctx.load_texture("camera", image, egui::TextureOptions::LINEAR));
            }
        }

        self.fps_window_frames += 1;
        let elapsed = self.fps_window_start.elapsed();
        if elapsed >= Duration::from_secs(1) {
            self.fps = self.fps_window_frames as f32 / elapsed.as_secs_f32();
            self.fps_window_start = Instant::now();
            self.fps_window_frames = 0;
        }
        self.frame_info = format!(
            "frame {}  {w}x{h} {}  ts {} ns",
            frame.frame_id, frame.pixel_format, frame.timestamp_ns
        );
    }
}

impl eframe::App for Viewer {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        self.poll_frames(&ctx);

        egui::Frame::central_panel(ui.style()).show(ui, |ui| {
            let stats = self.stream.stats();
            ui.horizontal(|ui| {
                ui.label(format!("{:.1} fps", self.fps));
                ui.separator();
                ui.label(&self.frame_info);
                ui.separator();
                ui.label(format!(
                    "complete {}  dropped {}  underruns {}  resends {}  missing {}",
                    stats.completed_frames,
                    stats.frames_dropped,
                    stats.underruns,
                    stats.resend_requests,
                    stats.missing_packets,
                ));
            });
            ui.separator();
            if let Some(texture) = &self.texture {
                ui.centered_and_justified(|ui| {
                    ui.add(egui::Image::new(texture).shrink_to_fit());
                });
            } else {
                ui.centered_and_justified(|ui| {
                    ui.label("waiting for first frame...");
                });
            }
        });

        if self.last_log.elapsed() >= Duration::from_secs(2) {
            self.last_log = Instant::now();
            let stats = self.stream.stats();
            println!(
                "{:.1} fps  complete {}  dropped {}  underruns {}",
                self.fps, stats.completed_frames, stats.frames_dropped, stats.underruns
            );
        }

        // Poll continuously for new frames.
        ctx.request_repaint();
    }

    fn on_exit(&mut self) {
        if let Err(e) = self.cam.stop_acquisition() {
            println!("stop acquisition: {e}");
        }
    }
}
