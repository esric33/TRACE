from __future__ import annotations

import hashlib
import json
from typing import Any

from TRACE.core.actions.registry import ActionRegistry
from TRACE.core.actions.types import ActionDef, ActionExecContext, ArgSpec
from TRACE.core.executor.support import (
    ExecErrorCode,
    ExecPhase,
    _attach_period,
    _get_q_period,
    _is_rate,
    _is_scalar,
    _rate_from,
    _rate_to,
    convert_scale,
    exec_error,
)


def _cache_key_for_lookup(ctx: ActionExecContext, node_id: str, query: str) -> str:
    context_ids = [s["snippet_id"] for s in ctx.capsule["context"]["snippets"]]
    blob = {
        "op": "TEXT_LOOKUP",
        "qid": ctx.capsule["qid"],
        "node": node_id,
        "query": query,
        "snips": context_ids,
    }
    payload = json.dumps(blob, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


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


def _exec_text_lookup(ctx: ActionExecContext, node_id: str, args: dict[str, Any]) -> dict[str, Any]:
    query = args["query"]
    if not isinstance(query, str) or not query.strip():
        raise exec_error(
            ExecErrorCode.BAD_ARGS,
            "TEXT_LOOKUP requires query string",
            phase=ExecPhase.ACTION,
            op="TEXT_LOOKUP",
            arg="query",
            got=query,
        )

    key = _cache_key_for_lookup(ctx, node_id, query)
    if key not in ctx.cache:
        ctx.cache[key] = ctx.lookup_fn(
            node_id, query, ctx.capsule, ctx.extracts_by_snippet
        )
    return ctx.cache[key]


def _exec_get_quantity(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    fact = args["fact"]
    if not isinstance(fact, dict) or "quantity" not in fact:
        raise exec_error(
            ExecErrorCode.LOOKUP_FAILED,
            "GET_QUANTITY expected ModelFact with quantity",
            phase=ExecPhase.ACTION,
            op="GET_QUANTITY",
            arg="fact",
            expected="ModelFact",
            got=fact,
        )
    quantity = fact["quantity"]
    if not isinstance(quantity, dict):
        raise exec_error(
            ExecErrorCode.TYPE_MISMATCH,
            "GET_QUANTITY quantity must be dict",
            phase=ExecPhase.ACTION,
            op="GET_QUANTITY",
            arg="quantity",
            expected="dict",
            got=quantity,
        )
    return _attach_period(quantity, fact.get("period", {}))


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

    if a.get("type") == "money" and _is_scalar(b):
        return {**a, "value": float(a["value"]) * float(b["value"])}

    if _is_scalar(a) and b.get("type") == "money":
        return {**b, "value": float(a["value"]) * float(b["value"])}

    if a.get("type") == "money" and _is_rate(b, "fx_rate"):
        frm = _rate_from(b).get("currency")
        to = _rate_to(b).get("currency")
        if not isinstance(frm, str) or not isinstance(to, str):
            raise exec_error(
                ExecErrorCode.BAD_RATE,
                "fx_rate missing from/to.currency",
                phase=ExecPhase.ACTION,
                op="MUL",
                rate=b,
            )
        if a.get("unit") != frm:
            raise exec_error(
                ExecErrorCode.UNIT_MISMATCH,
                "FX rate from.currency must match money unit",
                phase=ExecPhase.ACTION,
                op="MUL",
                money_unit=a.get("unit"),
                rate_from=frm,
                rate=b,
            )
        return {**a, "value": float(a["value"]) * float(b["value"]), "unit": to}

    if a.get("type") == "money" and _is_rate(b, "cpi_rate"):
        fy = _rate_from(b).get("year")
        ty = _rate_to(b).get("year")
        if not isinstance(fy, int) or not isinstance(ty, int):
            raise exec_error(
                ExecErrorCode.BAD_RATE,
                "cpi_rate missing from/to.year ints",
                phase=ExecPhase.ACTION,
                op="MUL",
                rate=b,
            )

        period = _get_q_period(a)
        if period is None:
            raise exec_error(
                ExecErrorCode.MISSING_PERIOD,
                "CPI adjustment requires money to carry FY provenance (_period_kind/_period_value)",
                phase=ExecPhase.ACTION,
                op="MUL",
                money=a,
                rate=b,
            )
        period_kind, period_value = period
        if period_kind != "FY" or not isinstance(period_value, int):
            raise exec_error(
                ExecErrorCode.PERIOD_MISMATCH,
                "CPI adjustment requires FY int provenance on money",
                phase=ExecPhase.ACTION,
                op="MUL",
                money_period=period,
                rate=b,
            )
        if period_value != fy:
            raise exec_error(
                ExecErrorCode.PERIOD_MISMATCH,
                "CPI rate from_year must match money FY year",
                phase=ExecPhase.ACTION,
                op="MUL",
                money_year=period_value,
                rate_from_year=fy,
                rate=b,
            )
        return {**a, "value": float(a["value"]) * float(b["value"])}

    raise exec_error(
        ExecErrorCode.TYPE_MISMATCH,
        "MUL supports scalar*scalar, money*scalar, scalar*money, money*fx_rate, money*cpi_rate",
        phase=ExecPhase.ACTION,
        op="MUL",
        a=a,
        b=b,
    )


def _exec_div(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a = _require_quantity(args["a"], op="DIV", arg="a")
    b = _require_quantity(args["b"], op="DIV", arg="b")

    if a.get("type") == "rate" and a.get("unit") == "percent" and a.get("scale") == 1 and _is_scalar(b):
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

    if a.get("type") == "money" and b.get("type") == "money":
        if a.get("unit") != b.get("unit"):
            raise exec_error(
                ExecErrorCode.UNIT_MISMATCH,
                "DIV money currency mismatch",
                phase=ExecPhase.ACTION,
                op="DIV",
                a=a,
                b=b,
            )
        if a.get("scale") != b.get("scale"):
            raise exec_error(
                ExecErrorCode.SCALE_MISMATCH,
                "DIV money scale mismatch",
                phase=ExecPhase.ACTION,
                op="DIV",
                a=a,
                b=b,
            )
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
        "DIV supports (percent/scalar)->scalar or (scalar/scalar)->scalar or (money/money)->scalar",
        phase=ExecPhase.ACTION,
        op="DIV",
        a=a,
        b=b,
    )


def build_registry() -> ActionRegistry:
    registry = ActionRegistry()
    for action in (
        ActionDef(
            name="TEXT_LOOKUP",
            arg_specs=(ArgSpec("query", "string", non_empty=True),),
            executor=_exec_text_lookup,
        ),
        ActionDef(
            name="GET_QUANTITY",
            arg_specs=(ArgSpec("fact", "ref"),),
            executor=_exec_get_quantity,
        ),
        ActionDef(
            name="CONVERT_SCALE",
            arg_specs=(ArgSpec("q", "ref"), ArgSpec("target_scale", "number")),
            executor=_exec_convert_scale,
        ),
        ActionDef(
            name="CONST",
            arg_specs=(ArgSpec("value", "number"),),
            executor=_exec_const,
        ),
        ActionDef(
            name="ADD",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            executor=_exec_add,
        ),
        ActionDef(
            name="MUL",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            executor=_exec_mul,
        ),
        ActionDef(
            name="DIV",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            executor=_exec_div,
        ),
        ActionDef(
            name="GT",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            executor=_exec_gt,
        ),
        ActionDef(
            name="LT",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            executor=_exec_lt,
        ),
        ActionDef(
            name="EQ",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            executor=_exec_eq,
        ),
        ActionDef(
            name="AND",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            executor=_exec_and,
        ),
        ActionDef(
            name="OR",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
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
