from .types import ActionDef, ActionExecContext
from .registry import ActionRegistry


def build_registry(*args, **kwargs):
    from .builtin import build_registry as _build_registry

    return _build_registry(*args, **kwargs)


def build_registry_for_benchmark(*args, **kwargs):
    from .builtin import build_registry_for_benchmark as _build_registry_for_benchmark

    return _build_registry_for_benchmark(*args, **kwargs)

__all__ = [
    "ActionDef",
    "ActionExecContext",
    "ActionRegistry",
    "build_registry",
    "build_registry_for_benchmark",
]
