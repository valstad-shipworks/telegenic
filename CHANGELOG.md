# Changelog

## 2.0.0 — 2026-10-05

Real-time tuning moves to fast-talker 0.3 option lists, snare is no longer a
runtime dependency, and decoder/reassembly bugs found by the new snare 2,
proptest, cargo-fuzz and pytest suites are fixed. Requires `fast-talker` 0.3.

### Breaking changes

- `ThreadConfig` is removed. `GigeConfig::thread_cfg` becomes
  `thread: Vec<ThreadOption>` plus `control_socket: Vec<SocketOption>`;
  `StreamConfig::thread_cfg` and `socket_buffer` become `thread` and
  `stream_socket`. Each worker and socket accepts a fixed set of options (see
  the README table); a refused option, or one that fails to apply, fails
  `connect`/`open_stream` with the new `CameraError::InvalidOption`
  (`gvsp does not accept option dscp (Dscp(46))`). Options for another
  platform are skipped with a warning. Thread options are no longer
  Linux-only and are no longer downgraded to a warning on failure.
- `PixelFormat::image_size` returns `Option<usize>` (`None` on overflow).
- New public fields: `StreamStats::socket_drops`; emulator
  `Reaction::resend`.
- `snare` (1.4) is no longer a dependency; production code uses std/mio.
  `if-addrs` and `atomic-waker` are dropped.
- The `async` feature is an empty alias: `flume`'s `async` is always on, so
  `FrameChannel::recv_async`/`EventChannel::recv_async` always exist.

### Added

- Re-exports `fast_talker`, `ThreadOption`, `SocketOption`, `ReportSummary`.
- `TuningReport` (applied/adjusted/skipped per thread and socket):
  `GigECamera::tuning_report()`, `StreamChannel::tuning_report()`.
- Packet telemetry (`telegenic::wire`): `TelemetrySink`, `GvcpCmd`,
  `GvcpAck`, `ControlTx`/`ControlRx`, `GvspPacket`;
  `GigECamera::set_telemetry` and `open_stream_with_telemetry`. Receive
  stamps are kernel stamps where the platform provides them.
- `LinkStats { gvcp_retransmits, frames_incomplete, frames_timed_out }` via
  `GigECamera::link_stats()`/`GenICamera::link_stats()`, cumulative across
  connections and streams.
- `StreamStats::socket_drops` (Linux `SO_RXQ_OVFL`).
- `DEFAULT_STREAM_RECV_BUFFER` (8 MiB).
- Python: `thread`/`control_socket` on `Camera(...)`, `thread`/`stream_socket`
  on `start_acquisition`, `snap`, `snapshot_session`; `tuning_report()` on
  `Camera`, `Acquisition`, `SnapshotSession`; `apply_process_options` and
  `ProcessGuard`; option type stubs in `telegenic/_options.pyi`.
- Emulator: retransmitted GVCP commands replay the cached acknowledge
  (`GigeDevice::duplicates()`); `set_leader_timestamp_ns`,
  `set_packet_resend`, `set_event_support`, `message_dest`,
  `set_max_packet_size`; helpers `pending_ack`, `event_cmd`, `mark_resent`.
  `CAP_PACKET_RESEND` is no longer advertised by default.

### Fixed

- GVSP: block-id distance across 16-bit and extended-id wraps; a device that
  restarts block ids at 1 is followed instead of discarded as late; packets
  arriving before payload packet 1 are held until the block size is known
  instead of placed at guessed offsets; a trailer contradicting received
  packets, or an out-of-range packet id, can no longer size the frame; a frame
  with data holes reports `MissingPackets` instead of `Complete`.
- Packet-size negotiation writes the size the device acknowledged, not the
  probed maximum.
- A failed `open_stream` clears the channel's SCP so the device stops sending.
- GVCP: `PENDING_ACK` reads its 16-bit timeout from the right offset, and
  extensions are capped at 120 s per command; a `READ_MEMORY` length no
  longer drives an unbounded preallocation.
- GenICam: device-supplied register lengths (16 MiB) and XML sizes (64 MiB)
  are bounded; zipped XML is checked against its size and CRC-32; cyclic
  value/string links no longer recurse forever; a big-endian bit range past
  the register no longer underflows.
- `timestamp_to_ns` no longer overflows for large tick counts.
- `ResponseHandle` wakes every task awaiting any clone, and an error read
  through a shared handle keeps its variant (was flattened to `Spawn`).
- Python: `telegenic.CameraError`/`GenicamError` report those names
  (their class names were `CameraException`/`GenicamException`).
- Python: negative or non-finite timeouts raise `ValueError` instead of
  panicking; an oversized `heartbeat_timeout` is rejected.

### Changed

- Sockets are created through fast-talker with options applied before bind,
  so `BindDevice` and `WinCpuAffinity` (stream socket) take effect; adjusted
  values (e.g. a capped `RecvBuffer`) are logged.
- The GVSP socket's default receive buffer is 8 MiB (was sized from the
  payload, 256 KiB-8 MiB); a `RecvBuffer` in `stream_socket` replaces it.
- Adapters are enumerated with `fast_talker::nic::interfaces`; the advertised
  stream/message host address comes from `nic::source_for`, which also finds
  it where a connected probe socket leaves it unspecified.
- `GigECamera::disconnect` waits on the worker's exit notification (sent even
  if the worker unwinds) instead of polling every 5 ms.
- GenICam feature queries on a disconnected camera return the connection
  error instead of "feature model not loaded".
