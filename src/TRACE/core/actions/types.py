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


ArgKind = Literal["ref", "refs", "number", "string", "object"]
OutputCategory = Literal["quantity", "relation_set"]


@dataclass(frozen=True)
class ArgSpec:
    name: str
    kind: ArgKind
    non_empty: bool = False

    def prompt_repr(self) -> str:
        if self.kind == "ref":
            return '"ref:<id>"'
        if self.kind == "refs":
            return '["ref:<id>", ...]'
        if self.kind == "number":
            return "number"
        if self.kind == "string":
            return "string"
        if self.kind == "object":
            return "object"
        raise ValueError(f"Unsupported arg kind: {self.kind}")

    def validate(self, value: Any, *, action_name: str, node_id: str) -> None:
        if self.kind == "ref":
            if not (isinstance(value, str) and value.startswith("ref:")):
                raise ValueError(f"{action_name}.{self.name} must be ref:<id> in {node_id}")
            return

        if self.kind == "refs":
            if not isinstance(value, list) or not value:
                raise ValueError(
                    f"{action_name}.{self.name} must be a non-empty list of ref:<id> in {node_id}"
                )
            for item in value:
                if not (isinstance(item, str) and item.startswith("ref:")):
                    raise ValueError(
                        f"{action_name}.{self.name} items must be ref:<id> in {node_id}"
                    )
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

        if self.kind == "object":
            if not isinstance(value, dict):
                raise ValueError(f"{action_name}.{self.name} must be an object in {node_id}")
            return

        raise ValueError(f"Unsupported arg kind: {self.kind}")


@dataclass(frozen=True)
class OutputSpec:
    category: OutputCategory
    summary: str
    same_as_arg: str | None = None
    same_fields: tuple[str, ...] = ()
    fixed_type: str | None = None
    fixed_unit: str | None = None
    fixed_scale: int | float | None = None

    def prompt_repr(self) -> str:
        parts = [self.summary]
        if self.same_as_arg and self.same_fields:
            same = ", ".join(self.same_fields)
            parts.append(f"matches {same} of {self.same_as_arg}")
        fixed_parts = []
        if self.fixed_type is not None:
            fixed_parts.append(f"type={self.fixed_type}")
        if self.fixed_unit is not None:
            fixed_parts.append(f"unit={self.fixed_unit}")
        if self.fixed_scale is not None:
            fixed_parts.append(f"scale={self.fixed_scale}")
        if fixed_parts:
            parts.append(", ".join(fixed_parts))
        return "; ".join(parts)

    def validate(self, value: Any, *, args: dict[str, Any], action_name: str, node_id: str) -> None:
        if self.category == "quantity":
            if not (
                isinstance(value, dict)
                and {"value", "unit", "scale", "type"} <= set(value.keys())
            ):
                raise ValueError(
                    f"{action_name} output must be Quantity-like in {node_id}"
                )
        elif self.category == "relation_set":
            if not (
                isinstance(value, dict)
                and value.get("type") == "relation_set"
                and isinstance(value.get("items"), list)
            ):
                raise ValueError(
                    f"{action_name} output must be relation_set-like in {node_id}"
                )
        else:
            raise ValueError(f"Unsupported output category: {self.category}")

        if self.same_as_arg is not None:
            source = args.get(self.same_as_arg)
            if isinstance(source, dict):
                for field in self.same_fields:
                    if value.get(field) != source.get(field):
                        raise ValueError(
                            f"{action_name} output.{field} must match {self.same_as_arg}.{field} in {node_id}"
                        )

        if self.fixed_type is not None and value.get("type") != self.fixed_type:
            raise ValueError(
                f"{action_name} output.type must be {self.fixed_type} in {node_id}"
            )
        if self.fixed_unit is not None and value.get("unit") != self.fixed_unit:
            raise ValueError(
                f"{action_name} output.unit must be {self.fixed_unit} in {node_id}"
            )
        if self.fixed_scale is not None and value.get("scale") != self.fixed_scale:
            raise ValueError(
                f"{action_name} output.scale must be {self.fixed_scale} in {node_id}"
            )


@dataclass(frozen=True)
class ActionDef:
    name: str
    arg_specs: tuple[ArgSpec, ...]
    summary: str
    output_spec: OutputSpec
    executor: Callable[[ActionExecContext, str, dict[str, Any]], Any] | None = None

    @property
    def arg_keys(self) -> tuple[str, ...]:
        return tuple(spec.name for spec in self.arg_specs)

    def prompt_doc(self) -> str:
        args = ", ".join(
            f'"{spec.name}": {spec.prompt_repr()}' for spec in self.arg_specs
        )
        return (
            f"- {self.name}: {self.summary}. "
            f"args={{{args}}}. "
            f"returns={self.output_spec.prompt_repr()}"
        )

    def validate_args(self, args: dict[str, Any], *, node_id: str) -> None:
        expected_keys = set(self.arg_keys)
        if set(args.keys()) != expected_keys:
            raise ValueError(f"{self.name} args must be exactly {expected_keys} in {node_id}")
        for spec in self.arg_specs:
            spec.validate(args[spec.name], action_name=self.name, node_id=node_id)

    def validate_output(self, value: Any, *, args: dict[str, Any], node_id: str) -> None:
        self.output_spec.validate(
            value,
            args=args,
            action_name=self.name,
            node_id=node_id,
        )
