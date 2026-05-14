from __future__ import annotations

from typing import Any

from TRACE.core.actions.registry import ActionRegistry
from TRACE.core.actions.types import (
    ActionDef,
    ActionExecContext,
    ArgSpec,
    OutputSpec,
)
from TRACE.core.executor.support import (
    ExecErrorCode,
    ExecPhase,
    _attach_period,
    _is_scalar,
    convert_scale,
    exec_error,
)


def _require_quantity(value: Any, *, op: str, arg: str) -> dict[str, Any]:
    if not isinstance(value, dict) or "value" not in value:
        raise exec_error(
            ExecErrorCode.TYPE_MISMATCH,
            f"{op} expected Quantity for {arg}",
            phase=ExecPhase.ACTION,
            op=op,
            arg=arg,
            expected="Quantity",
            got=value,
        )
    return value


def _require_bool(value: Any, *, op: str, arg: str) -> dict[str, Any]:
    if not (isinstance(value, dict) and value.get("type") == "bool"):
        raise exec_error(
            ExecErrorCode.TYPE_MISMATCH,
            f"{op} expected bool for {arg}",
            phase=ExecPhase.ACTION,
            op=op,
            arg=arg,
            expected="bool",
            got=value,
        )
    return value


def _require_matching_quantities(a: dict[str, Any], b: dict[str, Any], *, op: str) -> None:
    if a.get("type") != b.get("type"):
        raise exec_error(
            ExecErrorCode.TYPE_MISMATCH,
            f"{op} quantity.type mismatch",
            phase=ExecPhase.ACTION,
            op=op,
            a=a,
            b=b,
        )
    if a.get("unit") != b.get("unit"):
        raise exec_error(
            ExecErrorCode.UNIT_MISMATCH,
            f"{op} unit mismatch",
            phase=ExecPhase.ACTION,
            op=op,
            a=a,
            b=b,
        )
    if a.get("scale") != b.get("scale"):
        raise exec_error(
            ExecErrorCode.SCALE_MISMATCH,
            f"{op} scale mismatch",
            phase=ExecPhase.ACTION,
            op=op,
            a=a,
            b=b,
        )


def _exec_model_fact(ctx: ActionExecContext, _: str, args: dict[str, Any]) -> dict[str, Any]:
    snippet_id = args["snippet_id"]
    label = args["label"]
    period = args["period"]
    quantity = args["quantity"]

    context_ids = {
        s.get("snippet_id")
        for s in ctx.capsule.get("context", {}).get("snippets", [])
        if isinstance(s, dict)
    }
    if snippet_id not in context_ids:
        raise exec_error(
            ExecErrorCode.BAD_ARGS,
            "MODEL_FACT snippet_id must refer to a context snippet",
            phase=ExecPhase.ACTION,
            op="MODEL_FACT",
            arg="snippet_id",
            got=snippet_id,
        )

    allowed_labels = set(ctx.benchmark_def.load_allowed_labels(ctx.benchmark_def.schemas_dir))
    if label not in allowed_labels:
        raise exec_error(
            ExecErrorCode.BAD_ARGS,
            "MODEL_FACT label is not allowed for this benchmark",
            phase=ExecPhase.ACTION,
            op="MODEL_FACT",
            arg="label",
            got=label,
        )

    if not isinstance(period, dict) or set(period) != {"period", "value"}:
        raise exec_error(
            ExecErrorCode.BAD_ARGS,
            "MODEL_FACT period must have exactly period and value",
            phase=ExecPhase.ACTION,
            op="MODEL_FACT",
            arg="period",
            got=period,
        )

    quantity = _require_quantity(quantity, op="MODEL_FACT", arg="quantity")
    return {
        **_attach_period(quantity, period),
        "source": {
            "snippet_id": snippet_id,
            "label": label,
            "period": period,
        },
    }


def _exec_convert_scale(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    quantity = _require_quantity(args["q"], op="CONVERT_SCALE", arg="q")
    return convert_scale(quantity, args["target_scale"])


def _exec_const(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    value = args["value"]
    if not isinstance(value, (int, float)):
        raise exec_error(
            ExecErrorCode.BAD_ARGS,
            "CONST requires numeric value",
            phase=ExecPhase.ACTION,
            op="CONST",
            arg="value",
            got=value,
        )
    return {"value": float(value), "unit": "", "scale": 1, "type": "scalar"}


def _exec_add(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a = _require_quantity(args["a"], op="ADD", arg="a")
    b = _require_quantity(args["b"], op="ADD", arg="b")
    _require_matching_quantities(a, b, op="ADD")
    return {
        "value": a["value"] + b["value"],
        "unit": a["unit"],
        "scale": a["scale"],
        "type": a["type"],
    }


def _exec_gt(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a = _require_quantity(args["a"], op="GT", arg="a")
    b = _require_quantity(args["b"], op="GT", arg="b")
    _require_matching_quantities(a, b, op="GT")
    return {"value": bool(a["value"] > b["value"]), "unit": "bool", "scale": 1, "type": "bool"}


def _exec_lt(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a = _require_quantity(args["a"], op="LT", arg="a")
    b = _require_quantity(args["b"], op="LT", arg="b")
    _require_matching_quantities(a, b, op="LT")
    return {"value": bool(a["value"] < b["value"]), "unit": "bool", "scale": 1, "type": "bool"}


def _exec_eq(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a = _require_quantity(args["a"], op="EQ", arg="a")
    b = _require_quantity(args["b"], op="EQ", arg="b")
    _require_matching_quantities(a, b, op="EQ")
    return {"value": bool(a["value"] == b["value"]), "unit": "bool", "scale": 1, "type": "bool"}


def _exec_and(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a = _require_bool(args["a"], op="AND", arg="a")
    b = _require_bool(args["b"], op="AND", arg="b")
    return {"value": bool(a["value"] and b["value"]), "unit": "bool", "scale": 1, "type": "bool"}


def _exec_or(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a = _require_bool(args["a"], op="OR", arg="a")
    b = _require_bool(args["b"], op="OR", arg="b")
    return {"value": bool(a["value"] or b["value"]), "unit": "bool", "scale": 1, "type": "bool"}


def _exec_mul(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a = _require_quantity(args["a"], op="MUL", arg="a")
    b = _require_quantity(args["b"], op="MUL", arg="b")

    if _is_scalar(a) and _is_scalar(b):
        return {"value": float(a["value"]) * float(b["value"]), "unit": "", "scale": 1, "type": "scalar"}

    if isinstance(a, dict) and a.get("type") != "bool" and _is_scalar(b):
        return {**a, "value": float(a["value"]) * float(b["value"])}

    if _is_scalar(a) and isinstance(b, dict) and b.get("type") != "bool":
        return {**b, "value": float(a["value"]) * float(b["value"])}

    raise exec_error(
        ExecErrorCode.TYPE_MISMATCH,
        "MUL supports scalar*scalar, quantity*scalar, or scalar*quantity",
        phase=ExecPhase.ACTION,
        op="MUL",
        a=a,
        b=b,
    )


def _exec_div(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a = _require_quantity(args["a"], op="DIV", arg="a")
    b = _require_quantity(args["b"], op="DIV", arg="b")

    if _is_scalar(a) and _is_scalar(b):
        denom = float(b["value"])
        if denom == 0.0:
            raise exec_error(
                ExecErrorCode.DIV_ZERO,
                "DIV by zero",
                phase=ExecPhase.ACTION,
                op="DIV",
                b=b,
        )
        return {"value": float(a["value"]) / denom, "unit": "", "scale": 1, "type": "scalar"}

    if isinstance(a, dict) and a.get("type") != "bool" and _is_scalar(b):
        denom = float(b["value"])
        if denom == 0.0:
            raise exec_error(
                ExecErrorCode.DIV_ZERO,
                "DIV by zero",
                phase=ExecPhase.ACTION,
                op="DIV",
                b=b,
            )
        return {**a, "value": float(a["value"]) / denom}

    if (
        isinstance(a, dict)
        and isinstance(b, dict)
        and a.get("type") != "bool"
        and b.get("type") != "bool"
    ):
        _require_matching_quantities(a, b, op="DIV")
        denom = float(b["value"])
        if denom == 0.0:
            raise exec_error(
                ExecErrorCode.DIV_ZERO,
                "DIV by zero",
                phase=ExecPhase.ACTION,
                op="DIV",
                b=b,
            )
        return {"value": float(a["value"]) / denom, "unit": "", "scale": 1, "type": "scalar"}

    raise exec_error(
        ExecErrorCode.TYPE_MISMATCH,
        "DIV supports scalar/scalar, quantity/scalar, or ratio of matching quantities",
        phase=ExecPhase.ACTION,
        op="DIV",
        a=a,
        b=b,
    )


def build_registry() -> ActionRegistry:
    registry = ActionRegistry()
    for action in (
        ActionDef(
            name="MODEL_FACT",
            arg_specs=(
                ArgSpec("snippet_id", "string", non_empty=True),
                ArgSpec("label", "string", non_empty=True),
                ArgSpec("period", "object"),
                ArgSpec("quantity", "object"),
            ),
            summary="Assert one directly stated fact extracted from context and return its quantity",
            output_spec=OutputSpec(
                category="quantity",
                summary="Qty with value, unit, scale, type, and source provenance",
            ),
            executor=_exec_model_fact,
        ),
        ActionDef(
            name="CONVERT_SCALE",
            arg_specs=(ArgSpec("q", "ref"), ArgSpec("target_scale", "number")),
            summary="Rewrite a quantity to a requested numeric scale",
            output_spec=OutputSpec(
                category="quantity",
                summary="Quantity with the same semantic type and unit as q",
                same_as_arg="q",
                same_fields=("type", "unit"),
            ),
            executor=_exec_convert_scale,
        ),
        ActionDef(
            name="CONST",
            arg_specs=(ArgSpec("value", "number"),),
            summary="Create a scalar constant quantity",
            output_spec=OutputSpec(
                category="quantity",
                summary="Scalar quantity",
                fixed_type="scalar",
                fixed_unit="",
                fixed_scale=1,
            ),
            executor=_exec_const,
        ),
        ActionDef(
            name="ADD",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            summary="Add two matching quantities",
            output_spec=OutputSpec(
                category="quantity",
                summary="Quantity with the same type, unit, and scale as a",
                same_as_arg="a",
                same_fields=("type", "unit", "scale"),
            ),
            executor=_exec_add,
        ),
        ActionDef(
            name="MUL",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            summary="Multiply a scalar with another scalar or a non-boolean quantity",
            output_spec=OutputSpec(
                category="quantity",
                summary="Scalar result or scaled copy of the non-scalar input",
            ),
            executor=_exec_mul,
        ),
        ActionDef(
            name="DIV",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            summary="Divide by a scalar or compute the ratio of matching quantities",
            output_spec=OutputSpec(
                category="quantity",
                summary="Scaled quantity or scalar ratio, depending on inputs",
            ),
            executor=_exec_div,
        ),
        ActionDef(
            name="GT",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            summary="Return whether a is greater than b",
            output_spec=OutputSpec(
                category="quantity",
                summary="Boolean quantity",
                fixed_type="bool",
                fixed_unit="bool",
                fixed_scale=1,
            ),
            executor=_exec_gt,
        ),
        ActionDef(
            name="LT",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            summary="Return whether a is less than b",
            output_spec=OutputSpec(
                category="quantity",
                summary="Boolean quantity",
                fixed_type="bool",
                fixed_unit="bool",
                fixed_scale=1,
            ),
            executor=_exec_lt,
        ),
        ActionDef(
            name="EQ",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            summary="Return whether a equals b",
            output_spec=OutputSpec(
                category="quantity",
                summary="Boolean quantity",
                fixed_type="bool",
                fixed_unit="bool",
                fixed_scale=1,
            ),
            executor=_exec_eq,
        ),
        ActionDef(
            name="AND",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            summary="Logical conjunction of two boolean quantities",
            output_spec=OutputSpec(
                category="quantity",
                summary="Boolean quantity",
                fixed_type="bool",
                fixed_unit="bool",
                fixed_scale=1,
            ),
            executor=_exec_and,
        ),
        ActionDef(
            name="OR",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            summary="Logical disjunction of two boolean quantities",
            output_spec=OutputSpec(
                category="quantity",
                summary="Boolean quantity",
                fixed_type="bool",
                fixed_unit="bool",
                fixed_scale=1,
            ),
            executor=_exec_or,
        ),
    ):
        registry.register(action)
    return registry


def build_registry_for_benchmark(benchmark_def) -> ActionRegistry:
    registry = build_registry()
    benchmark_def.register_actions(registry)
    missing = set(benchmark_def.allowed_actions) - registry.allowed_ops()
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(
            f"benchmark {benchmark_def.benchmark_id} declared unregistered actions: {missing_str}"
        )
    return registry
