"""Tests of the Python extension that need no camera: the module surface
against its stubs, every option-taking parameter's conversion, and the
errors reachable without a device.

Build and run (from the crate root)::

    uv venv .venv && uv pip install --python .venv maturin pytest mypy
    .venv/bin/maturin develop -i .venv/bin/python
    .venv/bin/python -m pytest py_tests
"""

from __future__ import annotations

import ast
import dataclasses
import enum
import inspect
import math
import re
import subprocess
import sys
import time
import types
from pathlib import Path

import pytest
import telegenic

STUBS = Path(__file__).resolve().parent.parent / "py_src" / "telegenic"

# Nothing listens on this port of the loopback address, and UDP gets no
# refusal back on an unconnected socket, so a connect attempt waits out its
# timeout without any device involved.
CLOSED = "127.0.0.1"
FAST = {"gvcp_timeout": 0.05, "retries": 0}


def stub_tree() -> ast.Module:
    return ast.parse((STUBS / "__init__.pyi").read_text())


def stub_all() -> list[str]:
    for node in stub_tree().body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets
        ):
            return list(ast.literal_eval(node.value))
    raise AssertionError("__all__ missing from the stubs")


def stub_params(fn: ast.FunctionDef) -> list[tuple[str, str, object]]:
    """``(name, kind, default)`` per parameter, ``self``/``cls`` dropped."""
    a = fn.args
    out: list[tuple[str, str, object]] = []
    positional = a.posonlyargs + a.args
    defaults = [inspect.Parameter.empty] * (len(positional) - len(a.defaults)) + [
        ast.literal_eval(d) for d in a.defaults
    ]
    for p, d in zip(positional, defaults):
        out.append((p.arg, "positional", d))
    if a.vararg:
        out.append((a.vararg.arg, "var_positional", inspect.Parameter.empty))
    for p, d in zip(a.kwonlyargs, a.kw_defaults):
        out.append(
            (
                p.arg,
                "keyword_only",
                inspect.Parameter.empty if d is None else ast.literal_eval(d),
            )
        )
    if out and out[0][0] in ("self", "cls"):
        out = out[1:]
    return out


def runtime_params(obj) -> list[tuple[str, str, object]]:
    kinds = {
        inspect.Parameter.POSITIONAL_ONLY: "positional",
        inspect.Parameter.POSITIONAL_OR_KEYWORD: "positional",
        inspect.Parameter.VAR_POSITIONAL: "var_positional",
        inspect.Parameter.KEYWORD_ONLY: "keyword_only",
    }
    out = [
        (p.name, kinds[p.kind], p.default)
        for p in inspect.signature(obj).parameters.values()
    ]
    if out and out[0][0] in ("self", "cls"):
        out = out[1:]
    return out


def comparable(params):
    """A ``*args`` parameter cannot be passed by name, so its name is not
    part of the interface."""
    return [("*", k, d) if k == "var_positional" else (n, k, d) for n, k, d in params]


def stub_callables():
    """``(qualified name, runtime object, stub def)`` for every function and
    method the stubs declare."""
    for node in stub_tree().body:
        if isinstance(node, ast.FunctionDef):
            yield node.name, getattr(telegenic, node.name), node
        elif isinstance(node, ast.ClassDef):
            cls = getattr(telegenic, node.name)
            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    target = cls if item.name == "__new__" else getattr(cls, item.name)
                    yield f"{node.name}.{item.name}", target, item


def test_every_stubbed_name_exists_at_runtime():
    names = stub_all()
    assert sorted(names) == sorted(telegenic.__all__)
    for name in names:
        assert hasattr(telegenic, name), name


@pytest.mark.parametrize(
    "qualname,target,stub",
    [pytest.param(q, t, s, id=q) for q, t, s in stub_callables()],
)
def test_signatures_match_the_stubs(qualname, target, stub):
    assert comparable(runtime_params(target)) == comparable(stub_params(stub)), qualname


def test_stubbed_attributes_and_members_exist():
    for node in stub_tree().body:
        if not isinstance(node, ast.ClassDef):
            continue
        cls = getattr(telegenic, node.name)
        for item in node.body:
            if isinstance(item, ast.AnnAssign):
                assert hasattr(cls, item.target.id), f"{node.name}.{item.target.id}"
            elif isinstance(item, ast.Assign):
                for t in item.targets:
                    assert hasattr(cls, t.id), f"{node.name}.{t.id}"


def test_stubtest_agrees_with_the_runtime():
    pytest.importorskip("mypy")
    allow = Path(__file__).resolve().parent / "stubtest_allowlist.txt"
    run = subprocess.run(
        [sys.executable, "-m", "mypy.stubtest", "telegenic", "--allowlist", str(allow)],
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parent,
        check=False,
    )
    assert run.returncode == 0, run.stdout + run.stderr


def test_enums_compare_by_value_and_name():
    assert telegenic.FrameStatus.Complete == telegenic.FrameStatus.Complete
    assert telegenic.FrameStatus.Complete != telegenic.FrameStatus.Timeout
    assert int(telegenic.FrameStatus.Complete) == 0
    assert {
        m.__repr__() for m in (telegenic.AccessMode.RO, telegenic.AccessMode.RW)
    } == {
        "AccessMode.RO",
        "AccessMode.RW",
    }


class QosEnum(enum.Enum):
    macos_qos = "utility"


class NiceEnum(enum.Enum):
    LINUX_NICE = 5


@dataclasses.dataclass
class Opt:
    kind: str
    value: object


# Every shape the option conversion takes, each spelling an option the GVCP
# worker accepts.
ACCEPTED_THREAD = [
    None,
    "win_disable_power_throttling",
    ("prefault_stack", 65536),
    ["prefault_stack", 65536],
    [("cpu_affinity", [0]), ("linux_nice", 5)],
    {"linux_nice": 5},
    {"kind": "linux_nice", "value": 5},
    {"type": "LinuxNice", "value": 5},
    Opt("macos_qos", "utility"),
    types.SimpleNamespace(kind="MacOsQos", value="utility"),
    types.SimpleNamespace(type="linux-nice", value=5),
    QosEnum.macos_qos,
    NiceEnum.LINUX_NICE,
    [NiceEnum.LINUX_NICE, ("unix_scheduler", "batch")],
    (x for x in [("cpu_affinity", 0)]),
]

ACCEPTED_SOCKET = [
    None,
    ("recv_buffer", 1 << 20),
    [("recv_buffer", 1 << 20), ("dscp", 46)],
    {"recv_buffer": 1 << 20},
    {"kind": "recv_buffer", "value": 1 << 20},
    Opt("linux_priority", 4),
    types.SimpleNamespace(kind="RECV_BUFFER", value=4096),
]

# (value, exception, text the message must name)
INVALID_THREAD = [
    ([("no_such_option", 1)], ValueError, "no_such_option"),
    ([("rt_priority", "high")], TypeError, "rt_priority"),
    ([("rt_priority", 300)], ValueError, "rt_priority"),
    ([("cpu_affinity", -1)], ValueError, "cpu_affinity"),
    (5, TypeError, "ThreadOption"),
    ({"a": 1, "b": 2}, ValueError, "'a'"),
    ({"kind": "linux_nice", "value": 5, "extra": 1}, TypeError, "extra"),
    (("rt_priority", 1, 2), TypeError, "needs a value"),
    ([("rt_priority", 1, 2)], TypeError, "2 items"),
    (Opt("no_such_option", 1), ValueError, "no_such_option"),
]

INVALID_SOCKET = [
    ([("no_such_option", 1)], ValueError, "no_such_option"),
    ([("recv_buffer", "big")], TypeError, "recv_buffer"),
    ([("recv_buffer", -1)], ValueError, "recv_buffer"),
    (5, TypeError, "SocketOption"),
]

STREAM_METHODS = ["start_acquisition", "snap", "snapshot_session"]


@pytest.mark.parametrize("value", ACCEPTED_THREAD, ids=repr)
def test_camera_thread_option_shapes_convert(value):
    telegenic.Camera(CLOSED, thread=value)


@pytest.mark.parametrize("value", ACCEPTED_SOCKET, ids=repr)
def test_camera_control_socket_option_shapes_convert(value):
    telegenic.Camera(CLOSED, control_socket=value)


@pytest.mark.parametrize("value,exc,names", INVALID_THREAD, ids=repr)
def test_invalid_camera_thread_options_are_rejected(value, exc, names):
    with pytest.raises(exc, match=names):
        telegenic.Camera(CLOSED, thread=value)


@pytest.mark.parametrize("value,exc,names", INVALID_SOCKET, ids=repr)
def test_invalid_camera_control_socket_options_are_rejected(value, exc, names):
    with pytest.raises(exc, match=names):
        telegenic.Camera(CLOSED, control_socket=value)


def stream_call(method: str, **kwargs):
    cam = telegenic.Camera(CLOSED)
    return getattr(cam, method)(**kwargs)


@pytest.mark.parametrize("method", STREAM_METHODS)
@pytest.mark.parametrize(
    "param,value",
    [("thread", v) for v in ACCEPTED_THREAD]
    + [("stream_socket", v) for v in ACCEPTED_SOCKET],
    ids=repr,
)
def test_stream_option_shapes_convert(method, param, value):
    # The options convert; the call then fails only for want of a device.
    with pytest.raises(telegenic.CameraError, match="not connected"):
        stream_call(method, **{param: value})


@pytest.mark.parametrize("method", STREAM_METHODS)
@pytest.mark.parametrize(
    "param,value,exc,names",
    [("thread", *case) for case in INVALID_THREAD]
    + [("stream_socket", *case) for case in INVALID_SOCKET],
    ids=repr,
)
def test_invalid_stream_options_are_rejected(method, param, value, exc, names):
    with pytest.raises(exc, match=names):
        stream_call(method, **{param: value})


# Options the GVCP worker refuses, in assorted shapes, with the text its
# error must carry: the converted option, so the shape demonstrably
# decoded to it.
REFUSED_CONTROL = [
    ({"thread": ("rt_priority", 80)}, "rt_priority (RtPriority(80))"),
    ({"thread": Opt("rt_priority", 7)}, "rt_priority (RtPriority(7))"),
    (
        {"thread": {"unix_scheduler": ("fifo", 10)}},
        "unix_scheduler (UnixScheduler(Fifo(10)))",
    ),
    (
        {"thread": types.SimpleNamespace(kind="win_mmcss", value="Pro Audio")},
        'win_mmcss (WinMmcss("Pro Audio"))',
    ),
    ({"control_socket": ("send_buffer", 4096)}, "send_buffer (SendBuffer(4096))"),
    (
        {"control_socket": {"linux_busy_poll": 50}},
        "linux_busy_poll (LinuxBusyPoll(50))",
    ),
    (
        {"control_socket": ("dont_fragment", True)},
        "dont_fragment (DontFragment(true))",
    ),
]


@pytest.mark.parametrize("kwargs,names", REFUSED_CONTROL, ids=repr)
def test_refused_options_fail_connect_before_any_io(kwargs, names):
    cam = telegenic.Camera(CLOSED, gvcp_timeout=5.0, retries=4, **kwargs)
    started = time.monotonic()
    with pytest.raises(
        telegenic.CameraError, match="gvcp does not accept option " + re.escape(names)
    ):
        cam.connect()
    # Well inside one acknowledge timeout: nothing was sent and waited on.
    assert time.monotonic() - started < 1.0
    assert not cam.is_connected()


def test_connect_to_a_silent_address_times_out():
    cam = telegenic.Camera(CLOSED, **FAST)
    started = time.monotonic()
    with pytest.raises(telegenic.CameraError, match="connect timed out"):
        cam.connect()
    assert time.monotonic() - started < 2.0
    assert not cam.is_connected()
    assert cam.link_stats() is None


@pytest.mark.parametrize(
    "call",
    [
        lambda c: c.device_info(),
        lambda c: c.feature_names(),
        lambda c: c.get_integer("Width"),
        lambda c: c.set_integer("Width", 64),
        lambda c: c.get_float("ExposureTime"),
        lambda c: c.get_enum("PixelFormat"),
        lambda c: c.execute("AcquisitionStart"),
        lambda c: c.invalidate_caches(),
        lambda c: c.snap(0.01),
        lambda c: c.start_acquisition(),
        lambda c: c.snapshot_session(),
    ],
    ids=[
        "device_info",
        "feature_names",
        "get_integer",
        "set_integer",
        "get_float",
        "get_enum",
        "execute",
        "invalidate_caches",
        "snap",
        "start_acquisition",
        "snapshot_session",
    ],
)
def test_calls_on_a_disconnected_camera_raise_camera_error(call):
    cam = telegenic.Camera(CLOSED)
    with pytest.raises(telegenic.CameraError, match="not connected"):
        call(cam)


def test_a_disconnected_camera_answers_what_needs_no_device():
    cam = telegenic.Camera(CLOSED)
    assert not cam.is_connected()
    assert not cam.has_feature("Width")
    assert cam.link_stats() is None
    assert repr(cam) == "Camera(127.0.0.1, connected=False)"
    cam.disconnect()


def test_errors_are_distinct_runtime_errors():
    assert issubclass(telegenic.CameraError, RuntimeError)
    assert issubclass(telegenic.GenicamError, RuntimeError)
    assert not issubclass(telegenic.CameraError, telegenic.GenicamError)
    assert not issubclass(telegenic.GenicamError, telegenic.CameraError)


@pytest.mark.parametrize(
    "kwargs,exc",
    [
        ({"ip": "not-an-ip"}, ValueError),
        ({"ip": "10.0.0.256"}, ValueError),
        ({"ip": CLOSED, "local_ip": "nope"}, ValueError),
        ({"ip": 1234}, TypeError),
        ({"ip": CLOSED, "retries": "4"}, TypeError),
        ({"ip": CLOSED, "gvcp_timeout": "fast"}, TypeError),
    ],
    ids=repr,
)
def test_invalid_camera_arguments_raise(kwargs, exc):
    with pytest.raises(exc):
        telegenic.Camera(**kwargs)


class DurationPanicked(Exception):
    """The extension panicked converting a duration."""


@pytest.mark.parametrize(
    "call",
    [
        lambda: telegenic.Camera(CLOSED, gvcp_timeout=-1.0),
        lambda: telegenic.Camera(CLOSED, gvcp_timeout=math.nan),
        lambda: telegenic.Camera(CLOSED, gvcp_timeout=math.inf),
        lambda: telegenic.Camera(CLOSED).disconnect(-1.0),
        lambda: telegenic.Camera(CLOSED).snap(timeout=math.nan),
        lambda: telegenic.discover(timeout=-1.0),
    ],
    ids=["negative", "nan", "inf", "disconnect", "snap", "discover"],
)
def test_invalid_durations_raise_value_error(call):
    try:
        call()
    except ValueError:
        return
    except BaseException as e:
        if type(e).__name__ == "PanicException":
            raise DurationPanicked(str(e)) from e
        raise
    pytest.fail("the duration was accepted")


@pytest.mark.parametrize("seconds", [-1.0, 1e12])
def test_out_of_range_heartbeat_timeouts_raise(seconds):
    with pytest.raises(ValueError):
        telegenic.Camera(CLOSED, heartbeat_timeout=seconds)


@pytest.mark.parametrize("retries", [-1, 256])
def test_out_of_range_retries_raise(retries):
    with pytest.raises((OverflowError, ValueError)):
        telegenic.Camera(CLOSED, retries=retries)
