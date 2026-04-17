from __future__ import annotations

import hashlib
import json
from typing import Any

from TRACE.core.actions.registry import ActionRegistry
from TRACE.core.actions.types import ActionDef, ActionExecContext
from TRACE.core.executor.support import (
    ExecError,
    _attach_period,
    _get_q_period,
    _is_rate,
    _is_scalar,
    _rate_from,
    _rate_to,
    convert_scale,
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
        raise ExecError(
            "E_type_mismatch", f"{op} expected Quantity for {arg}", {"got": value}
        )
    return value


def _require_bool(value: Any, *, op: str, arg: str) -> dict[str, Any]:
    if not (isinstance(value, dict) and value.get("type") == "bool"):
        raise ExecError(
            "E_type_mismatch", f"{op} expected bool for {arg}", {"got": value}
        )
    return value


def _require_matching_quantities(a: dict[str, Any], b: dict[str, Any], *, op: str) -> None:
    if a.get("type") != b.get("type"):
        raise ExecError(
            "E_type_mismatch", f"{op} quantity.type mismatch", {"a": a, "b": b}
        )
    if a.get("unit") != b.get("unit"):
        raise ExecError("E_unit_mismatch", f"{op} unit mismatch", {"a": a, "b": b})
    if a.get("scale") != b.get("scale"):
        raise ExecError(
            "E_scale_mismatch", f"{op} scale mismatch", {"a": a, "b": b}
        )


def _exec_text_lookup(ctx: ActionExecContext, node_id: str, args: dict[str, Any]) -> dict[str, Any]:
    query = args["query"]
    if not isinstance(query, str) or not query.strip():
        raise ExecError("E_bad_args", "TEXT_LOOKUP requires query string")

    key = _cache_key_for_lookup(ctx, node_id, query)
    if key not in ctx.cache:
        ctx.cache[key] = ctx.lookup_fn(
            node_id, query, ctx.capsule, ctx.extracts_by_snippet
        )
    return ctx.cache[key]


def _exec_get_quantity(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    fact = args["fact"]
    if not isinstance(fact, dict) or "quantity" not in fact:
        raise ExecError(
            "E_lookup_failed",
            "GET_QUANTITY expected ModelFact with quantity",
            {"got": fact},
        )
    quantity = fact["quantity"]
    if not isinstance(quantity, dict):
        raise ExecError(
            "E_type_mismatch", "GET_QUANTITY quantity must be dict", {"got": quantity}
        )
    return _attach_period(quantity, fact.get("period", {}))


def _exec_convert_scale(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    quantity = _require_quantity(args["q"], op="CONVERT_SCALE", arg="q")
    return convert_scale(quantity, args["target_scale"])


def _exec_const(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    value = args["value"]
    if not isinstance(value, (int, float)):
        raise ExecError("E_bad_args", "CONST requires numeric value", {"got": value})
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
            raise ExecError("E_bad_rate", "fx_rate missing from/to.currency", {"rate": b})
        if a.get("unit") != frm:
            raise ExecError(
                "E_unit_mismatch",
                "FX rate from.currency must match money unit",
                {"money_unit": a.get("unit"), "rate_from": frm, "rate": b},
            )
        return {**a, "value": float(a["value"]) * float(b["value"]), "unit": to}

    if a.get("type") == "money" and _is_rate(b, "cpi_rate"):
        fy = _rate_from(b).get("year")
        ty = _rate_to(b).get("year")
        if not isinstance(fy, int) or not isinstance(ty, int):
            raise ExecError("E_bad_rate", "cpi_rate missing from/to.year ints", {"rate": b})

        period = _get_q_period(a)
        if period is None:
            raise ExecError(
                "E_missing_period",
                "CPI adjustment requires money to carry FY provenance (_period_kind/_period_value)",
                {"money": a, "rate": b},
            )
        period_kind, period_value = period
        if period_kind != "FY" or not isinstance(period_value, int):
            raise ExecError(
                "E_period_mismatch",
                "CPI adjustment requires FY int provenance on money",
                {"money_period": period, "rate": b},
            )
        if period_value != fy:
            raise ExecError(
                "E_period_mismatch",
                "CPI rate from_year must match money FY year",
                {"money_year": period_value, "rate_from_year": fy, "rate": b},
            )
        return {**a, "value": float(a["value"]) * float(b["value"])}

    raise ExecError(
        "E_type_mismatch",
        "MUL supports scalar*scalar, money*scalar, scalar*money, money*fx_rate, money*cpi_rate",
        {"a": a, "b": b},
    )


def _exec_div(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a = _require_quantity(args["a"], op="DIV", arg="a")
    b = _require_quantity(args["b"], op="DIV", arg="b")

    if a.get("type") == "rate" and a.get("unit") == "percent" and a.get("scale") == 1 and _is_scalar(b):
        denom = float(b["value"])
        if denom == 0.0:
            raise ExecError("E_div_zero", "DIV by zero", {"b": b})
        return {"value": float(a["value"]) / denom, "unit": "", "scale": 1, "type": "scalar"}

    if _is_scalar(a) and _is_scalar(b):
        denom = float(b["value"])
        if denom == 0.0:
            raise ExecError("E_div_zero", "DIV by zero", {"b": b})
        return {"value": float(a["value"]) / denom, "unit": "", "scale": 1, "type": "scalar"}

    if a.get("type") == "money" and b.get("type") == "money":
        if a.get("unit") != b.get("unit"):
            raise ExecError(
                "E_unit_mismatch",
                "DIV money currency mismatch",
                {"a": a, "b": b},
            )
        if a.get("scale") != b.get("scale"):
            raise ExecError(
                "E_scale_mismatch", "DIV money scale mismatch", {"a": a, "b": b}
            )
        denom = float(b["value"])
        if denom == 0.0:
            raise ExecError("E_div_zero", "DIV by zero", {"b": b})
        return {"value": float(a["value"]) / denom, "unit": "", "scale": 1, "type": "scalar"}

    raise ExecError(
        "E_type_mismatch",
        "DIV supports (percent/scalar)->scalar or (scalar/scalar)->scalar or (money/money)->scalar",
        {"a": a, "b": b},
    )


def build_registry() -> ActionRegistry:
    registry = ActionRegistry()
    for action in (
        ActionDef(name="TEXT_LOOKUP", arg_keys=("query",), executor=_exec_text_lookup),
        ActionDef(name="GET_QUANTITY", arg_keys=("fact",), executor=_exec_get_quantity),
        ActionDef(name="CONVERT_SCALE", arg_keys=("q", "target_scale"), executor=_exec_convert_scale),
        ActionDef(name="CONST", arg_keys=("value",), executor=_exec_const),
        ActionDef(name="ADD", arg_keys=("a", "b"), executor=_exec_add),
        ActionDef(name="MUL", arg_keys=("a", "b"), executor=_exec_mul),
        ActionDef(name="DIV", arg_keys=("a", "b"), executor=_exec_div),
        ActionDef(name="GT", arg_keys=("a", "b"), executor=_exec_gt),
        ActionDef(name="LT", arg_keys=("a", "b"), executor=_exec_lt),
        ActionDef(name="EQ", arg_keys=("a", "b"), executor=_exec_eq),
        ActionDef(name="AND", arg_keys=("a", "b"), executor=_exec_and),
        ActionDef(name="OR", arg_keys=("a", "b"), executor=_exec_or),
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
