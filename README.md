# telegenic

Pure-Rust GenICam camera library.

Transports are pluggable backends behind the feature layer. The only backend
today is GigE Vision (`telegenic::gige`): GVCP (control) and GVSP (stream)
spoken directly over UDP — no vendor SDK, no GenTL producer. USB3 Vision can
slot in later without touching the feature API.

```rust no_run
use telegenic::{GenICamera, StreamConfig};

let mut cam = GenICamera::new([10, 0, 0, 210]); // no I/O, infallible
cam.connect()?; // dial, take control, load the feature model
cam.set_float("ExposureTime", 5000.0)?;
cam.set_enum("PixelFormat", "Mono8")?;

// One frame on demand — zero stream bandwidth between captures:
let frame = cam.snap(StreamConfig::new(), std::time::Duration::from_secs(5))?;
println!("{}x{} {}", frame.width, frame.height, frame.pixel_format);

// Or continuous acquisition. The guard subscribes before the camera
// starts (so even the first frame is delivered) and stops it on drop:
let acq = cam.start_acquisition(StreamConfig::new())?;
while let Some(frame) = acq.wait_for(std::time::Duration::from_secs(1)) {
    println!("{}x{} {}", frame.width, frame.height, frame.pixel_format);
}
acq.stop()?; // or just drop it
cam.disconnect(std::time::Duration::from_millis(500)); // or just drop it
# Ok::<(), telegenic::GenicamError>(())
```

Cameras are plain owned values with an explicit lifecycle: construction is
free, `connect(&mut self)` is where I/O happens (and doubles as the
reconnect path after a dropped link), `disconnect`/drop releases device
control, and per-connection state — identity, capabilities, the GenICam
model, stats — lives exactly as long as the connection. Worker-facing
methods return `Disconnected` while no link is up.

## Layers

- **`GenICamera`**: the GenICam feature layer and the type most users hold.
  String-keyed typed feature access over the node graph from the device's
  XML (zipped or plain), Converter/SwissKnife formula evaluation, register
  caching with pInvalidator handling, and acquisition tied together for you:
  `start_acquisition` returns an RAII guard for continuous streaming,
  `snap`/`snapshot_session` capture single frames on demand with the camera
  idle between shots. Both guards borrow the camera mutably, so stopping is
  automatic and disconnecting mid-acquisition cannot compile.
- **`gige`**: the GigE Vision backend.
  - `gige::discovery`: broadcast device discovery per network adapter, plus
    Force IP for repointing a camera whose address doesn't match the local
    subnet (`examples/discover.rs`, `examples/force_ip.rs`).
  - `gige::GigECamera`: the GVCP transport: register/memory IO as
    [`ResponseHandle`]s (sync `wait_timeout` or `await`), automatic
    heartbeat, pending-ack handling, retries, message-channel events, and
    `open_stream` for raw GVSP channels.
  - `gige::stream`: per-channel GVSP receiver on its own thread:
    preallocated buffer pool (zero allocation per packet/frame at steady
    state), out-of-order reassembly, packet-resend requests, automatic
    packet size negotiation, frames fanned out as `Arc<Frame>` over bounded
    channels with drop-on-full.

## Python

The same library ships as a Python package (PyO3/maturin, `py` feature;
`pip install telegenicam` once published, or `maturin develop` from a
checkout. The import name stays `telegenic`). The GenICam surface maps one-to-one, every blocking call
releases the GIL, and frames expose their pixels as `bytes` for
`numpy.frombuffer`:

```python
import telegenic

cam = telegenic.Camera("10.0.0.210")
cam.connect()
cam.set_float("ExposureTime", 5000.0)

with cam.snapshot_session() as session:   # camera idle between snaps
    frame = session.snap(timeout=5.0)
    print(frame.width, frame.height, frame.pixel_format)

with cam.start_acquisition() as acq:   # stops the camera again on exit
    for _ in range(100):
        frame = acq.wait_for(timeout=1.0)
        if frame is not None:
            print(frame)
```

`telegenic.discover()` finds cameras on the local subnets. Type stubs and
docstrings ship in the package (`telegenic/__init__.pyi`).

## Examples

```sh
cargo run --example discover [interface-ip]   # find cameras
cargo run --example force_ip <mac> <ip> <mask> <gw>
cargo run --example info <camera-ip>          # identity + GenICam URL
cargo run --example snap <camera-ip> [n]      # n on-demand single frames
cargo run --example grab <camera-ip> [n]      # stream n frames
python examples/grab.py <camera-ip> [n]       # the same, via the bindings
```

## Testing

`cargo test` runs everything against an in-process fake camera over loopback
UDP (`tests/fake_camera/`): GVCP semantics (retries, pending-ack, control
loss), discovery/force-IP, GVSP reassembly under loss/reordering/duplication
with resend replay, GenICam evaluation against the real Hikrobot and Imperx
vendor XMLs in `tests/data/`, and the full GenICamera path including
message-channel events and single-frame snapshots.
