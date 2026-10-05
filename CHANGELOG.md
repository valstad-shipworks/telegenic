# Changelog

## Unreleased

Depends on snare 1.5.0, which is not on crates.io yet: the manifest points at
a local `../snare` checkout, so this version cannot be published until snare
1.5.0 is.

### Changed (with or without a simulation)

- `ResponseHandle` wakes every task awaiting any clone of it (a `WakerSet`
  replaces the single `AtomicWaker`, which only woke the last registered
  task).
- Every clock the workers read (heartbeat, acknowledge deadlines, packet
  timeouts, frame retention, telemetry and frame timestamps) is snare's. Without
  the `shim` feature on snare this is the `std` clock.
- `GigECamera::disconnect` waits for the worker's exit notification instead of
  polling its liveness every 5 ms.
- `flume` is built with its `async` feature unconditionally, so
  `FrameChannel::recv_async` and `EventChannel::recv_async` always exist. The
  `async` feature remains as an empty alias.
- Emulator: a retransmitted GVCP command (same source, command and request id
  as the previous one) replays the cached acknowledge instead of executing
  again, so a retried `AcquisitionStart` never starts a second acquisition.
  `GigeDevice::duplicates()` counts the replays.
- Emulator: `CAP_PACKET_RESEND` is no longer advertised.

### Added

- `LinkStats { gvcp_retransmits, frames_incomplete, frames_timed_out }` and
  `GigECamera::link_stats()` / `GenICamera::link_stats()`: link-health
  counters cumulative across connections and stream channels.
- Emulator: `GigeDevice::set_leader_timestamp_ns`, the timestamp the next GVSP
  leaders carry; `Frame::timestamp_ns` reads it back.
- `snare-shim` feature (enables snare's `shim`), for the scheduler tests in
  `tests/snare_driven.rs`.

### Under a snare scheduler

When `snare::sched::is_driven()` holds on the calling thread:

- `ResponseHandle::wait`/`wait_timeout`, `FrameChannel::wait_for` and
  `EventChannel::wait_for` block through snare, visible to the scheduler and
  with the timeout on the virtual clock.
- The GVCP and GVSP workers sleep until their next deadline (heartbeat,
  acknowledge, packet timeout, retention) or input, instead of waking every
  10-100 ms.
- A dropped worker handle waits visibly (at most 5 s of virtual time) for the
  worker's exit before joining it.
