# telegenic

Pure-Rust GenICam camera library.

Transports are pluggable, but the only backend right now is GigE Vision
(`telegenic::gige`), which speaks GVCP (control) and GVSP (streaming)
directly over UDP.

```rust no_run
use telegenic::{GenICamera, StreamConfig};

let mut cam = GenICamera::new([10, 0, 0, 210]);
cam.connect()?; // takes control and loads the feature model
cam.set_float("ExposureTime", 5000.0)?;
cam.set_enum("PixelFormat", "Mono8")?;

// Grab a single frame. The camera stays idle between captures,
// so this uses no stream bandwidth outside the snap itself.
let frame = cam.snap(StreamConfig::new(), std::time::Duration::from_secs(5))?;
println!("{}x{} {}", frame.width, frame.height, frame.pixel_format);

// Or stream continuously. The guard subscribes before the camera
// starts (so the first frame isn't lost) and stops it on drop.
let acq = cam.start_acquisition(StreamConfig::new())?;
while let Some(frame) = acq.wait_for(std::time::Duration::from_secs(1)) {
    println!("{}x{} {}", frame.width, frame.height, frame.pixel_format);
}
acq.stop()?; // or just drop it
cam.disconnect(std::time::Duration::from_millis(500)); // or just drop it
# Ok::<(), telegenic::GenicamError>(())
```

A camera is a plain owned value. Constructing one does no I/O and can't
fail; `connect(&mut self)` is where the work happens, and it doubles as the
reconnect path after a dropped link. Everything tied to a connection
(identity, capabilities, the parsed GenICam model, stats) lives only as long
as the connection, and methods return `Disconnected` when no link is up.

## Layers

`GenICamera` is the top-level type and the one most code uses. It parses the
device's GenICam XML (zipped or plain) and gives string-keyed typed access
to the feature node graph, including Converter/SwissKnife formula
evaluation, register caching, and pInvalidator handling. Acquisition is
built in: `start_acquisition` returns an RAII guard for continuous
streaming, and `snap`/`snapshot_session` grab single frames with the camera
idle in between. Both guards borrow the camera mutably, so stopping happens
automatically and disconnecting mid-acquisition won't compile.

Underneath sits the GigE Vision backend:

- `gige::discovery` does broadcast device discovery per network adapter, and
  Force IP for repointing a camera whose address doesn't match the local
  subnet (see `examples/discover.rs` and `examples/force_ip.rs`).
- `gige::GigECamera` is the GVCP transport. Register and memory IO return
  [`ResponseHandle`]s that can be waited on synchronously (`wait_timeout`)
  or `.await`ed. It handles the heartbeat, pending-acks, retries, and
  message-channel events, and `open_stream` opens raw GVSP channels.
- `gige::stream` is the GVSP receiver, one thread per channel. Buffers come
  from a preallocated pool (no allocation per packet or frame at steady
  state), packets are reassembled out of order with resend requests, packet
  size is negotiated automatically, and frames fan out as `Arc<Frame>` over
  bounded channels that drop when full.

## Real-time tuning

`GigeConfig` and `StreamConfig` take [fast-talker](https://docs.rs/fast-talker)
option lists, re-exported as `telegenic::{ThreadOption, SocketOption}`, all
empty by default. The GVSP socket always gets an 8 MiB receive buffer, so a
full frame's burst fits between two worker wakeups, unless
`StreamConfig::stream_socket` sets its own `RecvBuffer`; its other options
are applied on top of that default rather than replacing it.

```rust no_run
use telegenic::gige::GigeConfig;
use telegenic::{GenICamera, SocketOption, StreamConfig, ThreadOption};

let mut cfg = GigeConfig::new([10, 0, 0, 210]);
cfg.thread = vec![ThreadOption::CpuAffinity(vec![2])];
cfg.control_socket = vec![SocketOption::Dscp(46)];
let mut cam = GenICamera::with_config(cfg);
cam.connect()?;

let mut stream = StreamConfig::new();
stream.thread = vec![ThreadOption::CpuAffinity(vec![3]), ThreadOption::RtPriority(80)];
stream.stream_socket = vec![SocketOption::RecvBuffer(32 << 20)];
let acq = cam.start_acquisition(stream)?;
# drop(acq);
# Ok::<(), telegenic::GenicamError>(())
```

| Field | Applied to | Accepted | Refused, and why |
|---|---|---|---|
| `GigeConfig::thread` | the GVCP control worker, by itself during `connect` | `CpuAffinity`, `PrefaultStack`, `LinuxNice`, `UnixScheduler(Other \| Batch \| Idle)`, `WinPriority` below `TimeCritical`, `WinDisablePowerThrottling`, `MacOsQos` | `RtPriority`, `UnixScheduler(Fifo \| RoundRobin)`, `WinPriority(TimeCritical)`, `WinMmcss`, `MacOsTimeConstraint`: a real-time class on a thread that blocks on slow request/response round-trips only risks starving the rest of the system |
| `GigeConfig::control_socket` | the GVCP socket, right after bind | `RecvBuffer`, `BindDevice`, `Dscp`, `LinuxPriority` | the busy-poll options burn a core on a slow loop; `SendBuffer`, `DontFragment` and `WinCpuAffinity` don't help a request/response socket |
| `StreamConfig::thread` | the GVSP stream worker, by itself before the stream opens | every `ThreadOption` except the one refused | `MacOsTimeConstraint`: it reserves a fixed computation slice per period, and the worker is a receive loop with no host-owned period |
| `StreamConfig::stream_socket` | the GVSP socket, right after bind | `RecvBuffer`, `BindDevice`, `LinuxBusyPoll`, `LinuxPreferBusyPoll`, `LinuxBusyPollBudget` | `SendBuffer`, `DontFragment`, `Dscp`, `LinuxPriority` only shape traffic this receive-only socket doesn't send; Windows only takes `WinCpuAffinity` before bind |

A refused option, or one that fails to apply (e.g. `RtPriority` without
`CAP_SYS_NICE`), makes `connect` (or opening the stream) fail. Options for
another platform, or that this platform cannot do, are skipped with a
`tracing` warning, so one config works on Linux, macOS and Windows. Discovery
and Force IP take no options.

Process-wide settings (memory locking, `cpu_dma_latency`, Windows priority
class, timer resolution and working set) are the application's job: call
`telegenic::fast_talker::options::ProcessOption::apply_all` once at startup.

From Python the same lists are keyword arguments, in any shape fast-talker
accepts:

```python
cam = telegenic.Camera("10.0.0.210", thread={"cpu_affinity": [2]})
acq = cam.start_acquisition(
    thread=[("cpu_affinity", [3]), ("rt_priority", 80)],
    stream_socket={"recv_buffer": 32 << 20},
)
```

## Python

The same library ships as a Python package via PyO3/maturin (the `py`
feature). Install with `pip install telegenicam` once it's published, or run
`maturin develop` from a checkout; the import name is `telegenic` either
way. The GenICam surface maps one-to-one, blocking calls release the GIL,
and frames expose their pixels as `bytes` for `numpy.frombuffer`:

```python
import telegenic

cam = telegenic.Camera("10.0.0.210")
cam.connect()
cam.set_float("ExposureTime", 5000.0)

with cam.snapshot_session() as session:  # camera idle between snaps
    frame = session.snap(timeout=5.0)
    print(frame.width, frame.height, frame.pixel_format)

with cam.start_acquisition() as acq:  # stops the camera again on exit
    for _ in range(100):
        frame = acq.wait_for(timeout=1.0)
        if frame is not None:
            print(frame)
```

`telegenic.discover()` finds cameras on the local subnets. The package
includes type stubs and docstrings (`telegenic/__init__.pyi`).

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
UDP (`tests/fake_camera/`). It covers GVCP semantics (retries, pending-ack,
control loss), discovery and Force IP, GVSP reassembly under packet loss,
reordering, and duplication with resend replay, GenICam evaluation against
the real Hikrobot and Imperx vendor XMLs in `tests/data/`, and the full
`GenICamera` path including message-channel events and single-frame
snapshots.
