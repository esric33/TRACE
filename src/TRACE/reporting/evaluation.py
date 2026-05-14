from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any

from TRACE.shared.text_norm import normalize_relation_text


REL_TOL = 1e-12
ABS_TOL = 1e-9


@dataclass(frozen=True)
class OutputComparison:
    correct: bool
    mismatch_kind: str | None
    details: dict[str, Any]


def is_quantity_dict(value: Any) -> bool:
    return isinstance(value, dict) and {"value", "unit", "scale", "type"} <= set(value.keys())


def is_relation_set_dict(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and value.get("type") == "relation_set"
        and isinstance(value.get("items"), list)
    )


def _float_pair(a: Any, b: Any) -> tuple[float, float] | None:
    try:
        af = float(a)
        bf = float(b)
    except Exception:
        return None
    return af, bf


def _float_equal(a: Any, b: Any, *, rel_tol: float = REL_TOL, abs_tol: float = ABS_TOL) -> bool:
    pair = _float_pair(a, b)
    if pair is None:
        return a == b
    af, bf = pair
    return math.isclose(af, bf, rel_tol=rel_tol, abs_tol=abs_tol)


def _float_diff(a: Any, b: Any) -> dict[str, float | None]:
    pair = _float_pair(a, b)
    if pair is None:
        return {"abs_diff": None, "rel_diff": None}
    af, bf = pair
    abs_diff = abs(af - bf)
    denom = abs(bf) if bf else 1.0
    return {"abs_diff": abs_diff, "rel_diff": abs_diff / denom}


def _base_value(quantity: dict[str, Any]) -> float | None:
    try:
        return float(quantity["value"]) * float(quantity["scale"])
    except Exception:
        return None


def compare_quantities(output: dict[str, Any], gold: dict[str, Any]) -> OutputComparison:
    type_match = output.get("type") == gold.get("type")
    unit_match = output.get("unit") == gold.get("unit")
    scale_match = _float_equal(output.get("scale"), gold.get("scale"))
    raw_value_match = _float_equal(output.get("value"), gold.get("value"))

    output_base = _base_value(output)
    gold_base = _base_value(gold)
    base_value_match = (
        output_base is not None and gold_base is not None and _float_equal(output_base, gold_base)
    )

    correct = bool(type_match and unit_match and base_value_match)

    if correct and scale_match and raw_value_match:
        mismatch_kind = None
    elif not type_match:
        mismatch_kind = "type_mismatch"
    elif not unit_match:
        mismatch_kind = "unit_mismatch"
    elif base_value_match and not scale_match:
        mismatch_kind = "scale_mismatch_only"
    elif base_value_match and scale_match and not raw_value_match:
        mismatch_kind = "value_mismatch_only"
    elif not base_value_match and not scale_match:
        mismatch_kind = "value_and_scale_mismatch"
    elif not base_value_match:
        mismatch_kind = "semantic_value_mismatch"
    else:
        mismatch_kind = "representation_mismatch"

    return OutputComparison(
        correct=correct,
        mismatch_kind=mismatch_kind,
        details={
            "is_quantity": True,
            "type_match": bool(type_match),
            "unit_match": bool(unit_match),
            "scale_match": bool(scale_match),
            "raw_value_match": bool(raw_value_match),
            "base_value_match": bool(base_value_match),
            "output_base_value": output_base,
            "gold_base_value": gold_base,
            "value_abs_tol": ABS_TOL,
            "value_rel_tol": REL_TOL,
            "raw_value_abs_diff": _float_diff(output.get("value"), gold.get("value"))["abs_diff"],
            "raw_value_rel_diff": _float_diff(output.get("value"), gold.get("value"))["rel_diff"],
            "base_value_abs_diff": _float_diff(output_base, gold_base)["abs_diff"],
            "base_value_rel_diff": _float_diff(output_base, gold_base)["rel_diff"],
        },
    )


def _relation_item_sigs(value: dict[str, Any]) -> set[tuple[str, str]]:
    sigs: set[tuple[str, str]] = set()
    for item in value.get("items") or []:
        if not isinstance(item, dict):
            continue
        obj = item.get("object") if isinstance(item.get("object"), dict) else {}
        obj_type = str(obj.get("type") or item.get("object_type") or value.get("object_type") or "")
        obj_value = obj.get("value", item.get("value"))
        sigs.add((obj_type, normalize_relation_text(obj_value)))
    return sigs


def compare_relation_sets(output: dict[str, Any], gold: dict[str, Any]) -> OutputComparison:
    label_match = output.get("label") == gold.get("label")
    object_type_match = output.get("object_type") == gold.get("object_type")
    output_items = _relation_item_sigs(output)
    gold_items = _relation_item_sigs(gold)
    items_match = output_items == gold_items
    correct = bool(label_match and object_type_match and items_match)
    return OutputComparison(
        correct=correct,
        mismatch_kind=None if correct else "value_mismatch",
        details={
            "is_quantity": False,
            "is_relation_set": True,
            "label_match": bool(label_match),
            "object_type_match": bool(object_type_match),
            "items_match": bool(items_match),
            "output_item_count": len(output_items),
            "gold_item_count": len(gold_items),
            "missing_items": sorted(gold_items - output_items),
            "extra_items": sorted(output_items - gold_items),
        },
    )


def compare_outputs(output: Any, gold: Any) -> OutputComparison:
    if is_quantity_dict(output) and is_quantity_dict(gold):
        return compare_quantities(output, gold)
    if is_relation_set_dict(output) and is_relation_set_dict(gold):
        return compare_relation_sets(output, gold)

    if output == gold:
        return OutputComparison(correct=True, mismatch_kind=None, details={"is_quantity": False})

    return OutputComparison(
        correct=False,
        mismatch_kind="value_mismatch",
        details={"is_quantity": False},
    )
