from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable


if TYPE_CHECKING:
    from TRACE.core.benchmarks.types import BenchmarkDef


@dataclass(frozen=True)
class ActionExecContext:
    benchmark_def: "BenchmarkDef"
    capsule: dict[str, Any]
    extracts_by_snippet: dict[str, list[dict[str, Any]]]
    cache: dict[str, Any]
    lookup_fn: Callable[[str, str, dict[str, Any], dict[str, list[dict[str, Any]]]], dict[str, Any]]


@dataclass(frozen=True)
class ActionDef:
    name: str
    arg_keys: tuple[str, ...]
    executor: Callable[[ActionExecContext, str, dict[str, Any]], Any] | None = None
