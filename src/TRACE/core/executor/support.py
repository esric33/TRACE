from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from TRACE.shared.io import read_json

Quantity = Dict[str, Any]
Period = Dict[str, Any]
ModelFact = Dict[str, Any]
LookupFn = Callable[
    [str, str, Dict[str, Any], Dict[str, List[Dict[str, Any]]]], Dict[str, Any]
]


def canonical_period(period: dict) -> tuple[str, object]:
    kind = period.get("period")
    value = period.get("value")

    if kind == "FY":
        if isinstance(value, int):
            return kind, value
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.isdigit():
                return kind, int(stripped)
        return kind, value

    if kind in ("Q", "ASOF"):
        if isinstance(value, str):
            return kind, value.strip()
        return kind, value

    return str(kind), value


def period_equal(a: Period, b: Period) -> bool:
    return canonical_period(a) == canonical_period(b)


def quantity_equal(a: Quantity, b: Quantity) -> bool:
    return (
        a.get("type") == b.get("type")
        and a.get("unit") == b.get("unit")
        and a.get("scale") == b.get("scale")
        and a.get("value") == b.get("value")
    )


@dataclass
class ExecError(Exception):
    code: str
    message: str
    data: Optional[Dict[str, Any]] = None


def load_extract_store(extracts_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    by_snippet: Dict[str, List[Dict[str, Any]]] = {}
    for path in extracts_dir.glob("*.json"):
        if not path.is_file():
            continue
        extract = read_json(path)
        by_snippet.setdefault(extract["snippet_id"], []).append(extract)
    return by_snippet


def resolve_fact_for_tagging(
    model_fact: ModelFact,
    context_snippet_ids: List[str],
    extracts_by_snippet: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    snippet_id = model_fact.get("snippet_id")
    if snippet_id not in context_snippet_ids:
        return {"status": "OUT_OF_CONTEXT", "candidates": []}

    candidates = extracts_by_snippet.get(snippet_id, [])
    label = model_fact.get("label")
    period = model_fact.get("period", {})
    quantity = model_fact.get("quantity", {})

    matches = []
    for extract in candidates:
        if extract.get("label") != label:
            continue
        if not period_equal(extract.get("period", {}), period):
            continue
        if not quantity_equal(extract.get("quantity", {}), quantity):
            continue
        matches.append(extract["extraction_id"])

    if len(matches) == 1:
        return {"status": "RESOLVED", "extraction_id": matches[0]}
    if len(matches) == 0:
        return {"status": "UNRESOLVED", "candidates": []}
    return {"status": "AMBIGUOUS", "candidates": matches}


def _get_q_period(quantity: dict) -> Optional[tuple[str, object]]:
    period_kind = quantity.get("_period_kind")
    period_value = quantity.get("_period_value")
    if period_kind is None or period_value is None:
        return None
    return str(period_kind), period_value


def _attach_period(quantity: dict, period: dict) -> dict:
    period_kind, period_value = canonical_period(period)
    return {**quantity, "_period_kind": period_kind, "_period_value": period_value}


def _is_scalar(quantity: dict) -> bool:
    return (
        quantity.get("type") == "scalar"
        and quantity.get("unit") == ""
        and quantity.get("scale") == 1
    )


def _is_rate(quantity: dict, unit: Optional[str] = None) -> bool:
    if quantity.get("type") != "rate" or quantity.get("scale") != 1:
        return False
    if unit is None:
        return True
    return quantity.get("unit") == unit


def _rate_from(quantity: dict) -> dict:
    value = quantity.get("from")
    return value if isinstance(value, dict) else {}


def _rate_to(quantity: dict) -> dict:
    value = quantity.get("to")
    return value if isinstance(value, dict) else {}


def convert_scale(quantity: Quantity, target_scale: int | float) -> Quantity:
    if quantity.get("scale") == target_scale:
        return quantity
    if target_scale == 0:
        raise ExecError(
            "E_bad_args", "target_scale cannot be zero", {"got": target_scale}
        )
    value = quantity["value"]
    source_scale = quantity["scale"]
    new_value = (value * source_scale) / target_scale
    return {**quantity, "value": new_value, "scale": target_scale}


def _q_norm(quantity: Quantity) -> Quantity:
    value = quantity.get("value")
    scale = quantity.get("scale")
    if isinstance(value, float) and isinstance(scale, (int, float)) and scale:
        base = round(value * float(scale), 6)
        normalized = base / float(scale)
        return {**quantity, "value": normalized}
    if isinstance(value, float):
        return {**quantity, "value": round(value, 12)}
    return quantity


__all__ = [
    "ExecError",
    "LookupFn",
    "ModelFact",
    "Period",
    "Quantity",
    "_attach_period",
    "_get_q_period",
    "_is_rate",
    "_is_scalar",
    "_q_norm",
    "_rate_from",
    "_rate_to",
    "canonical_period",
    "convert_scale",
    "load_extract_store",
    "period_equal",
    "quantity_equal",
    "resolve_fact_for_tagging",
]
