import importlib.metadata

try:
    __version__ = importlib.metadata.version("telegenicam")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

from importlib import import_module

__all__ = [
    "AccessMode",
    "Acquisition",
    "Camera",
    "CameraError",
    "DeviceInfo",
    "Frame",
    "FrameChannel",
    "FrameStatus",
    "GenicamError",
    "LinkStats",
    "SnapshotSession",
    "StreamStats",
    "discover",
]


def __getattr__(name: str):
    core = import_module(f"{__name__}._telegenic_core")
    if hasattr(core, name):
        return getattr(core, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    core = import_module(f"{__name__}._telegenic_core")
    return sorted(set(globals().keys()) | set(dir(core)))
