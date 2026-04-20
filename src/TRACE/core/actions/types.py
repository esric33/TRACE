from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Literal


if TYPE_CHECKING:
    from TRACE.core.benchmarks.types import BenchmarkDef


@dataclass(frozen=True)
class ActionExecContext:
    benchmark_def: "BenchmarkDef"
    capsule: dict[str, Any]
    extracts_by_snippet: dict[str, list[dict[str, Any]]]
    cache: dict[str, Any]
    lookup_fn: Callable[[str, str, dict[str, Any], dict[str, list[dict[str, Any]]]], dict[str, Any]]


ArgKind = Literal["ref", "number", "string"]


@dataclass(frozen=True)
class ArgSpec:
    name: str
    kind: ArgKind
    non_empty: bool = False

    def prompt_repr(self) -> str:
        if self.kind == "ref":
            return '"ref:<id>"'
        if self.kind == "number":
            return "number"
        if self.kind == "string":
            return "string"
        raise ValueError(f"Unsupported arg kind: {self.kind}")

    def validate(self, value: Any, *, action_name: str, node_id: str) -> None:
        if self.kind == "ref":
            if not (isinstance(value, str) and value.startswith("ref:")):
                raise ValueError(f"{action_name}.{self.name} must be ref:<id> in {node_id}")
            return

        if self.kind == "number":
            if not isinstance(value, (int, float)):
                raise ValueError(f"{action_name}.{self.name} must be a number in {node_id}")
            return

        if self.kind == "string":
            if not isinstance(value, str):
                raise ValueError(f"{action_name}.{self.name} must be a string in {node_id}")
            if self.non_empty and not value.strip():
                raise ValueError(
                    f"{action_name}.{self.name} must be a non-empty string in {node_id}"
                )
            return

        raise ValueError(f"Unsupported arg kind: {self.kind}")


@dataclass(frozen=True)
class ActionDef:
    name: str
    arg_specs: tuple[ArgSpec, ...]
    executor: Callable[[ActionExecContext, str, dict[str, Any]], Any] | None = None

    @property
    def arg_keys(self) -> tuple[str, ...]:
        return tuple(spec.name for spec in self.arg_specs)

    def prompt_doc(self) -> str:
        args = ", ".join(
            f'"{spec.name}": {spec.prompt_repr()}' for spec in self.arg_specs
        )
        return f"- {self.name}: {{{args}}}"

    def validate_args(self, args: dict[str, Any], *, node_id: str) -> None:
        expected_keys = set(self.arg_keys)
        if set(args.keys()) != expected_keys:
            raise ValueError(f"{self.name} args must be exactly {expected_keys} in {node_id}")
        for spec in self.arg_specs:
            spec.validate(args[spec.name], action_name=self.name, node_id=node_id)
