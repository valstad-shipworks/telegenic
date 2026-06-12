"""Pure-Rust GenICam camera library (GigE Vision backend, no vendor SDK).

A camera's features are exposed by name through the GenICam node graph
parsed from the device's own description XML. Typical use::

    import telegenic

    cam = telegenic.Camera("10.0.0.210")
    cam.connect()
    cam.set_float("ExposureTime", 5000.0)
    cam.set_enum("PixelFormat", "Mono8")

    # One frame on demand — zero stream bandwidth between captures:
    with cam.snapshot_session() as session:
        frame = session.snap(timeout=5.0)
        print(frame.width, frame.height, frame.pixel_format)

    # Or continuous acquisition:
    stream = cam.start_acquisition()
    for frame in stream.subscribe(16):
        print(frame)
    cam.stop_acquisition()
"""

from __future__ import annotations

import enum
from typing import Iterator, final

__version__: str

__all__ = [
    "AccessMode",
    "Camera",
    "CameraError",
    "DeviceInfo",
    "Frame",
    "FrameChannel",
    "FrameStatus",
    "GenicamError",
    "LinkStats",
    "SnapshotSession",
    "StreamChannel",
    "StreamStats",
    "discover",
]


class CameraError(RuntimeError):
    """Raised when the transport link fails: connect/acknowledge timeout,
    I/O error, device NAK, lost control, or a malformed packet."""


class GenicamError(RuntimeError):
    """Raised by the GenICam feature layer: unknown feature, wrong type or
    access mode, value out of range, or a broken device description."""


@final
class AccessMode(enum.Enum):
    """Current accessibility of a feature (:meth:`Camera.access_mode`)."""

    RO = ...
    WO = ...
    RW = ...


@final
class FrameStatus(enum.Enum):
    """How a frame ended; only :attr:`Complete` frames carry trustworthy
    pixel data end to end."""

    Complete = ...
    """All packets received."""
    MissingPackets = ...
    """Closed with holes left (resend disabled or overtaken by a newer
    frame)."""
    Timeout = ...
    """No packet arrived for the retention window."""
    Aborted = ...
    """The stream was stopped while the frame was filling."""
    WrongPacketId = ...
    """A packet id outside the expected range was seen."""
    PayloadUnsupported = ...
    """The payload type cannot be reassembled by this receiver."""


@final
class Camera:
    """A GenICam camera over GigE Vision.

    Construction is free and does no I/O; :meth:`connect` dials the device,
    takes control, and loads the feature model. Feature access is by GenICam
    name (``"Width"``, ``"ExposureTime"``, ...) with one getter/setter pair
    per feature type. The handle is thread-safe; calls are serialized on an
    internal lock.
    """

    def __new__(
        cls,
        ip: str,
        *,
        gvcp_timeout: float = 0.5,
        retries: int = 4,
        heartbeat_timeout: float = 3.0,
        exclusive: bool = False,
        local_ip: str | None = None,
    ) -> Camera:
        """Create a disconnected camera targeting ``ip:3956``.

        :param ip: Device IP address, e.g. ``"10.0.0.210"``.
        :param gvcp_timeout: Seconds allowed per control-command acknowledge
            try.
        :param retries: Retries after the first try before a transaction
            fails.
        :param heartbeat_timeout: Seconds written to the device's heartbeat
            timeout register; control is kept alive automatically.
        :param exclusive: Take exclusive control (no other application may
            even read).
        :param local_ip: Bind the control socket to this local address, for
            multi-homed hosts.
        :raises ValueError: if an IP string does not parse.
        """

    def connect(self) -> None:
        """Dial the device, take control, and load its GenICam feature
        model. No-op when already connected; doubles as the reconnect path.

        :raises CameraError: if the device is unreachable or denies control.
        :raises GenicamError: if the device description cannot be loaded.
        """

    def disconnect(self, deadline: float = 0.5) -> None:
        """Release device control and stop the I/O worker. The feature
        model is dropped with the connection; :meth:`connect` reloads it."""

    def is_connected(self) -> bool: ...

    def device_info(self) -> DeviceInfo:
        """Identity read from the bootstrap registers at connect."""

    def link_stats(self) -> LinkStats | None:
        """Control-channel counters, or ``None`` while disconnected."""

    def feature_names(self) -> list[str]:
        """Every node name in the device description (features plus the
        register/computation nodes behind them)."""

    def has_feature(self, name: str) -> bool: ...

    def get_integer(self, name: str) -> int: ...

    def set_integer(self, name: str, value: int) -> None: ...

    def integer_bounds(self, name: str) -> tuple[int, int]:
        """``(min, max)`` of an integer feature."""

    def integer_increment(self, name: str) -> int: ...

    def get_float(self, name: str) -> float: ...

    def set_float(self, name: str, value: float) -> None: ...

    def float_bounds(self, name: str) -> tuple[float, float]: ...

    def get_boolean(self, name: str) -> bool: ...

    def set_boolean(self, name: str, value: bool) -> None: ...

    def get_string(self, name: str) -> str: ...

    def set_string(self, name: str, value: str) -> None: ...

    def get_enum(self, name: str) -> str:
        """Current entry name of an enumeration feature."""

    def set_enum(self, name: str, entry: str) -> None: ...

    def enum_entries(self, name: str) -> list[str]: ...

    def execute(self, name: str) -> None:
        """Execute a command feature (e.g. ``"AcquisitionStart"``)."""

    def access_mode(self, name: str) -> AccessMode: ...

    def invalidate_caches(self) -> None:
        """Drop every cached register value, forcing fresh reads."""

    def start_acquisition(
        self,
        *,
        channel: int = 0,
        n_buffers: int = 8,
        packet_size: int | None = None,
        packet_delay: int | None = None,
        resend: bool = True,
    ) -> StreamChannel:
        """Open the stream channel and start continuous acquisition.

        :param channel: Stream channel index; almost always 0.
        :param n_buffers: Frame buffers in the receiver pool.
        :param packet_size: Fixed GVSP packet size in bytes, or ``None`` to
            negotiate the largest the link carries.
        :param packet_delay: Inter-packet delay in device timestamp ticks.
        :param resend: Request resends for missing packets.
        """

    def stop_acquisition(self) -> None:
        """Stop acquisition and unlock transport parameters. The stream
        channel closes when the :class:`StreamChannel` is garbage-collected
        (or its last reference dropped)."""

    def snap(
        self,
        timeout: float = 5.0,
        *,
        channel: int = 0,
        n_buffers: int = 8,
        packet_size: int | None = None,
        packet_delay: int | None = None,
        resend: bool = True,
    ) -> Frame:
        """Capture exactly one frame, opening and closing the stream around
        it. For repeated captures use :meth:`snapshot_session`, which pays
        the channel setup and packet-size negotiation once.

        :param timeout: Seconds to wait for the frame; must cover exposure
            plus transfer (and the trigger wait, when a trigger is
            configured).
        :raises CameraError: on timeout or a transport failure.
        """

    def snapshot_session(
        self,
        *,
        channel: int = 0,
        n_buffers: int = 8,
        packet_size: int | None = None,
        packet_delay: int | None = None,
        resend: bool = True,
    ) -> SnapshotSession:
        """Open a stream channel for on-demand single-frame capture.

        Unlike :meth:`start_acquisition` the camera is left idle: it
        transmits only while a :meth:`SnapshotSession.snap` is in flight, so
        an open session uses no link bandwidth between captures. Switches
        ``AcquisitionMode`` to ``SingleFrame`` when the device offers it
        (restored when the session closes)."""


@final
class SnapshotSession:
    """An open stream channel dedicated to single-frame capture.

    Use as a context manager (or call :meth:`close`) so the camera's
    ``AcquisitionMode`` and transport-parameter lock are restored::

        with cam.snapshot_session() as session:
            frame = session.snap()
    """

    def snap(self, timeout: float = 5.0) -> Frame:
        """Arm the camera, wait for the resulting frame, and leave the
        camera idle again. Check :attr:`Frame.status` before trusting the
        pixels.

        :raises CameraError: on timeout or a transport failure.
        :raises ValueError: if the session is closed.
        """

    def stats(self) -> StreamStats: ...

    def packet_size(self) -> int:
        """The negotiated (or configured) GVSP packet size."""

    def is_closed(self) -> bool: ...

    def close(self) -> None:
        """Restore ``AcquisitionMode``, unlock transport parameters, and
        close the stream channel. Idempotent."""

    def __enter__(self) -> SnapshotSession: ...

    def __exit__(self, *args: object) -> bool: ...


@final
class StreamChannel:
    """An open GVSP stream channel receiving on its own thread. Frames fan
    out to subscribers; the channel closes on the device when this object
    is garbage-collected."""

    def subscribe(self, capacity: int = 16) -> FrameChannel:
        """A new receiver buffering up to ``capacity`` frames. Each
        subscription is independent; when its buffer is full, new frames
        are dropped for that subscriber only and counted in
        :attr:`StreamStats.frames_dropped`."""

    def stats(self) -> StreamStats: ...

    def packet_size(self) -> int:
        """The negotiated (or configured) GVSP packet size."""

    def local_addr(self) -> str:
        """Where the device sends this stream, as ``ip:port``."""

    def is_running(self) -> bool: ...


@final
class FrameChannel:
    """A receiver for completed frames. Iterating blocks until the next
    frame and stops when the stream closes; Ctrl-C interrupts promptly."""

    def wait_for(self, timeout: float) -> Frame | None:
        """Block until a frame is buffered or ``timeout`` seconds elapse."""

    def try_recv(self) -> Frame | None: ...

    def recv_all(self) -> list[Frame]:
        """Drain and return every buffered frame."""

    def clear(self) -> None:
        """Discard buffered frames — pair with :meth:`wait_for` to grab a
        freshly acquired frame instead of a stale one."""

    def __iter__(self) -> Iterator[Frame]: ...

    def __next__(self) -> Frame: ...


@final
class Frame:
    """One reassembled frame."""

    status: FrameStatus
    is_complete: bool
    """``True`` when every packet of the frame arrived."""
    frame_id: int
    width: int
    height: int
    x_offset: int
    y_offset: int
    pixel_format: str
    """Pixel format name (e.g. ``"Mono8"``), or the raw GigE Vision code in
    hex for formats without a known name."""
    pixel_format_code: int
    """The raw 32-bit GigE Vision pixel format code."""
    bits_per_pixel: int
    timestamp_ns: int
    """Device timestamp in nanoseconds (0 when the tick frequency is
    unknown)."""
    timestamp_ticks: int
    """Device timestamp in ticks."""
    system_timestamp_ns: int
    """Host wall-clock time at frame start, nanoseconds since the epoch."""
    received_size: int
    """Payload bytes actually received."""

    def data(self) -> bytes:
        """The payload bytes, e.g. for ``numpy.frombuffer``. For an
        incomplete frame the holes read as stale buffer content — check
        :attr:`status` first."""


@final
class StreamStats:
    """Stream receiver counters, all monotonic since stream open."""

    packets: int
    bytes: int
    completed_frames: int
    failed_frames: int
    timed_out_frames: int
    aborted_frames: int
    missing_frames: int
    underruns: int
    missing_packets: int
    resend_requests: int
    resent_packets: int
    resend_ratio_reached: int
    resend_disabled: int
    duplicated_packets: int
    error_packets: int
    ignored_packets: int
    unsupported_frames: int
    size_mismatch_errors: int
    frames_dropped: int
    """Completed frames a subscriber could not take (its channel was
    full)."""


@final
class LinkStats:
    """Control-channel counters."""

    commands: int
    acks: int
    retries: int
    timeouts: int
    naks: int
    pending_acks: int
    heartbeats: int
    events: int
    unsolicited: int


@final
class DeviceInfo:
    """Device identity from the GigE Vision bootstrap registers."""

    manufacturer: str
    model: str
    serial: str
    device_version: str
    manufacturer_info: str
    user_defined_name: str
    ip: str
    subnet_mask: str
    gateway: str
    mac: str
    """Colon-separated hex, e.g. ``"00:11:1c:aa:bb:cc"``."""
    spec_version: tuple[int, int]
    """GigE Vision spec version as ``(major, minor)``."""


def discover(timeout: float = 1.0) -> list[DeviceInfo]:
    """Broadcast a GigE Vision discovery beacon on every Up IPv4 adapter
    and return the devices that answer within ``timeout`` seconds.

    Devices with a mis-configured IP still answer (the beacon also goes to
    the limited broadcast); reconfigure those from Rust via
    ``gige::discovery::force_ip`` for now."""
