from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable

from TRACE.shared.io import (
    read_json,
)

Quantity = Dict[str, Any]  # {value, unit, scale, type}
Period = Dict[str, Any]  # {period, value}
ModelFact = Dict[str, Any]
LookupFn = Callable[
    [str, str, Dict[str, Any], Dict[str, List[Dict[str, Any]]]], Dict[str, Any]
]
# (node_id, query, capsule, extracts_by_snippet) -> ModelFact


def canonical_period(p: dict) -> tuple[str, object]:
    """
    Returns (period_kind, canonical_value)
    period_kind: "FY" | "Q" | "ASOF"
    canonical_value:
      - FY: int year
      - Q:  string like "Q4 2023" (leave as-is)
      - ASOF: string date "YYYY-MM-DD" (leave as-is)
    """
    kind = p.get("period")
    val = p.get("value")

    if kind == "FY":
        if isinstance(val, int):
            return kind, val
        if isinstance(val, str):
            s = val.strip()
            if s.isdigit():
                return kind, int(s)
        return kind, val

    if kind in ("Q", "ASOF"):
        if isinstance(val, str):
            return kind, val.strip()
        return kind, val

    return str(kind), val


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
    """
    Reads all JSON files in extracts_dir. Filenames can be arbitrary (often extraction_id.json).
    Groups by snippet_id for fast lookup/tagging.
    """
    by_snip: Dict[str, List[Dict[str, Any]]] = {}

    for p in extracts_dir.glob("*.json"):
        if not p.is_file():
            continue
        ex = read_json(p)
        by_snip.setdefault(ex["snippet_id"], []).append(ex)
    return by_snip


def resolve_fact_for_tagging(
    mf: ModelFact,
    context_snippet_ids: List[str],
    extracts_by_snippet: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    """
    Evaluator-side only: does NOT affect execution.
    Used only to tag what went wrong.
    """
    sid = mf.get("snippet_id")
    if sid not in context_snippet_ids:
        return {"status": "OUT_OF_CONTEXT", "candidates": []}

    cands = extracts_by_snippet.get(sid, [])
    label = mf.get("label")
    period = mf.get("period", {})
    qty = mf.get("quantity", {})

    matches = []
    for ex in cands:
        if ex.get("label") != label:
            continue
        if not period_equal(ex.get("period", {}), period):
            continue
        if not quantity_equal(ex.get("quantity", {}), qty):
            continue
        matches.append(ex["extraction_id"])

    if len(matches) == 1:
        return {"status": "RESOLVED", "extraction_id": matches[0]}
    if len(matches) == 0:
        return {"status": "UNRESOLVED", "candidates": []}
    return {"status": "AMBIGUOUS", "candidates": matches}


# --- add these helpers near top (after Quantity/Period typedefs) ---


def _canon_year(x: Any) -> int:
    if isinstance(x, int):
        return x
    s = str(x).strip()
    if s.isdigit():
        return int(s)
    raise ExecError("E_bad_args", "Expected year int (or numeric string)", {"got": x})


def _get_q_period(q: dict) -> Optional[tuple[str, object]]:
    pk = q.get("_period_kind")
    pv = q.get("_period_value")
    if pk is None or pv is None:
        return None
    return (str(pk), pv)


def _attach_period(q: dict, p: dict) -> dict:
    # attach canonical period onto a quantity (for later CPI validation)
    pk, pv = canonical_period(p)
    return {**q, "_period_kind": pk, "_period_value": pv}


def _is_scalar(q: dict) -> bool:
    return q.get("type") == "scalar" and q.get("unit") == "" and q.get("scale") == 1


def _is_money(q: dict) -> bool:
    return (
        q.get("type") == "money"
        and isinstance(q.get("unit"), str)
        and isinstance(q.get("scale"), (int, float))
        and "value" in q
    )


def _is_rate(q: dict, unit: Optional[str] = None) -> bool:
    if q.get("type") != "rate" or q.get("scale") != 1:
        return False
    if unit is None:
        return True
    return q.get("unit") == unit


def _rate_from(q: dict) -> dict:
    f = q.get("from")
    return f if isinstance(f, dict) else {}


def _rate_to(q: dict) -> dict:
    t = q.get("to")
    return t if isinstance(t, dict) else {}


# --- make convert_scale preserve extra keys (like _period_*, etc) ---
def convert_scale(q: Quantity, target_scale: Union[int, float]) -> Quantity:
    if q.get("scale") == target_scale:
        return q
    v = q["value"]
    src = q["scale"]

    if target_scale == 0:
        raise ExecError(
            "E_bad_args", "target_scale cannot be zero", {"got": target_scale}
        )
    new_value = (v * src) / target_scale
    return {**q, "value": new_value, "scale": target_scale}


# --- update FX/CPI table expectations (minimal) ---
def load_fx_table(series_id: str) -> dict:
    p = Path("data") / "tables" / "fx" / f"{series_id.lower()}.json"
    try:
        tbl = read_json(p)
    except FileNotFoundError:
        raise ExecError(
            "E_missing_table", "FX table not found", {"series_id": series_id}
        )
    if tbl.get("series_id") != series_id:
        raise ExecError("E_bad_table", "FX table series_id mismatch", {"path": str(p)})

    # recommended table fields:
    # {"series_id": "...", "from": "RMB", "to": "USD", "rate_by_year": {...}}
    return tbl


def fx_lookup(
    series_id: str, year: Union[int, str], *, cache: Dict[str, Any]
) -> Quantity:
    year_i = _canon_year(year)

    tkey = f"fx_table::{series_id}"
    if tkey not in cache:
        cache[tkey] = load_fx_table(series_id)

    tbl = cache[tkey]
    rate = tbl.get("rate_by_year", {}).get(str(year_i))
    if rate is None:
        raise ExecError(
            "E_missing_table_key",
            "FX year not found",
            {"series_id": series_id, "year": year_i},
        )

    frm = tbl.get("from")
    to = tbl.get("to")
    if not isinstance(frm, str) or not isinstance(to, str):
        raise ExecError(
            "E_bad_table",
            "FX table must include 'from' and 'to' currency strings",
            {"series_id": series_id},
        )

    return {
        "value": float(rate),
        "unit": "fx_rate",
        "scale": 1,
        "type": "rate",
        "from": {"currency": frm},
        "to": {"currency": to},
        "at": {"year": year_i},
        "series_id": series_id,
    }


def cpi_index_lookup(
    series_id: str, year: Union[int, str], *, cache: Dict[str, Any]
) -> Quantity:
    year_i = _canon_year(year)

    tkey = f"cpi_table::{series_id}"
    if tkey not in cache:
        cache[tkey] = load_cpi_table(series_id)

    tbl = cache[tkey]
    idx = tbl.get("index_by_year", {})
    v = idx.get(str(year_i))
    if v is None:
        raise ExecError(
            "E_missing_table_key",
            "CPI year not found",
            {"series_id": series_id, "year": year_i},
        )

    return {
        "value": float(v),
        "unit": "cpi_index",
        "scale": 1,
        "type": "index",
        "at": {"year": year_i},
        "series_id": series_id,
    }


def cpi_rate_lookup(
    series_id: str,
    from_year: Union[int, str],
    to_year: Union[int, str],
    *,
    cache: Dict[str, Any],
) -> Quantity:
    fy = _canon_year(from_year)
    ty = _canon_year(to_year)

    a = cpi_index_lookup(series_id, ty, cache=cache)
    b = cpi_index_lookup(series_id, fy, cache=cache)

    denom = float(b["value"])
    if denom == 0.0:
        raise ExecError("E_div_zero", "CPI rate denom is zero", {"from_year": fy})

    return {
        "value": float(a["value"]) / denom,
        "unit": "cpi_rate",
        "scale": 1,
        "type": "rate",
        "from": {"year": fy},
        "to": {"year": ty},
        "series_id": series_id,
    }


def _round_value(x: Any, *, ndigits: int = 6) -> Any:
    if isinstance(x, float):
        return round(x, ndigits)
    return x


def _q_norm(q: Quantity) -> Quantity:
    v = q.get("value")
    s = q.get("scale")
    if isinstance(v, float) and isinstance(s, (int, float)) and s:
        base = round(v * float(s), 6)  # round in base units
        v2 = base / float(s)
        return {**q, "value": v2}
    if isinstance(v, float):
        return {**q, "value": round(v, 12)}  # fallback
    return q


def load_cpi_table(series_id: str) -> dict:
    # simplest: one file per series under tables/
    if series_id == "CPI_US_CPIU":
        p = Path("data") / "tables" / "cpi_us_cpiu.json"
    else:
        raise ExecError("E_bad_args", f"Unknown CPI series_id: {series_id}")

    tbl = read_json(p)
    if tbl.get("series_id") != series_id:
        raise ExecError("E_bad_table", "CPI table series_id mismatch", {"path": str(p)})
    return tbl


ALLOWED_OPS = {
    "TEXT_LOOKUP",
    "GET_QUANTITY",
    "CONVERT_SCALE",
    "FX_LOOKUP",
    "ADD",
    "GT",
    "LT",  # NEW
    "EQ",  # NEW (you’ll want it)
    "CPI_LOOKUP",
    "DIV",
    "MUL",
    "CONST",  # if your compiler emits Const nodes
    "AND",  # if you compose booleans
    "OR",  # if you compose booleans
    "NOT",  # optional
}


def execute_dag_strict(
    dag: Dict[str, Any],
    capsule: Dict[str, Any],
    extracts_by_snippet: Dict[str, List[Dict[str, Any]]],
    *,
    cache: Optional[Dict[str, Any]] = None,
    lookup_fn: Optional[LookupFn] = None,
) -> Dict[str, Any]:
    """
    Strict executor. For Step 0, TEXT_LOOKUP must be provided via lookup_fn (offline).
    """
    if cache is None:
        cache = {}

    nodes = dag.get("nodes", [])
    out_ref = dag.get("output")
    if not isinstance(nodes, list) or not out_ref:
        raise ExecError("E_bad_dag", "dag must have nodes[] and output")

    if lookup_fn is None:
        raise ExecError(
            "E_bad_args",
            "execute_dag_strict requires lookup_fn for TEXT_LOOKUP (offline mode)",
        )

    env: Dict[str, Any] = {}
    trace: List[Dict[str, Any]] = []

    context_snippets = capsule["context"]["snippets"]
    context_ids = [s["snippet_id"] for s in context_snippets]

    def resolve_ref(x: Any) -> Any:
        if isinstance(x, str) and x.startswith("ref:"):
            nid = x.split("ref:", 1)[1]
            if nid not in env:
                raise ExecError("E_bad_ref", f"Unknown ref {x}")
            return env[nid]
        return x

    def ck(obj: Any) -> str:
        b = json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8")
        return hashlib.sha256(b).hexdigest()

    for node in nodes:
        op = node.get("op")
        nid = node.get("id")
        args = node.get("args", {})
        if op not in ALLOWED_OPS:
            raise ExecError("E_bad_op", f"Op not allowed: {op}")
        if not nid:
            raise ExecError("E_bad_node", "Node missing id")

        if op == "TEXT_LOOKUP":
            query = args.get("query", "")
            if not isinstance(query, str) or not query.strip():
                raise ExecError("E_bad_args", "TEXT_LOOKUP requires query string")

            key = ck(
                {
                    "op": "TEXT_LOOKUP",
                    "qid": capsule["qid"],
                    "node": nid,  # <-- add this
                    "query": query,
                    "snips": context_ids,
                }
            )

            if key in cache:
                mf = cache[key]
            else:
                mf = lookup_fn(nid, query, capsule, extracts_by_snippet)
                cache[key] = mf

            resolve_tag = resolve_fact_for_tagging(mf, context_ids, extracts_by_snippet)
            env[nid] = mf
            trace.append(
                {
                    "node": nid,
                    "op": "TEXT_LOOKUP",
                    "query": query,
                    "model_fact": mf,
                    "resolve_tag": resolve_tag,
                }
            )

        elif op == "GET_QUANTITY":
            mf = resolve_ref(args["fact"])
            if not isinstance(mf, dict) or "quantity" not in mf:
                raise ExecError(
                    "E_lookup_failed",
                    "GET_QUANTITY expected ModelFact with quantity",
                    {"got": mf},
                )
            q = mf["quantity"]
            if not isinstance(q, dict):
                raise ExecError(
                    "E_type_mismatch", "GET_QUANTITY quantity must be dict", {"got": q}
                )
            # Attach canonical period so CPI ops can validate later
            env[nid] = _attach_period(q, mf.get("period", {}))

        elif op == "CONVERT_SCALE":
            q = resolve_ref(args["q"])
            target_scale = args["target_scale"]
            if not isinstance(q, dict) or "value" not in q:
                raise ExecError(
                    "E_type_mismatch", "CONVERT_SCALE expected Quantity", {"got": q}
                )
            env[nid] = convert_scale(q, target_scale)

        elif op == "ADD":
            a = resolve_ref(args["a"])
            b = resolve_ref(args["b"])
            if not isinstance(a, dict) or "value" not in a:
                raise ExecError(
                    "E_type_mismatch", "ADD expected Quantity for a", {"got": a}
                )
            if not isinstance(b, dict) or "value" not in b:
                raise ExecError(
                    "E_type_mismatch", "ADD expected Quantity for b", {"got": b}
                )

            if a.get("type") != b.get("type"):
                raise ExecError(
                    "E_type_mismatch", "ADD quantity.type mismatch", {"a": a, "b": b}
                )
            if a.get("unit") != b.get("unit"):
                raise ExecError(
                    "E_unit_mismatch", "ADD unit mismatch", {"a": a, "b": b}
                )
            if a.get("scale") != b.get("scale"):
                raise ExecError(
                    "E_scale_mismatch", "ADD scale mismatch", {"a": a, "b": b}
                )

            env[nid] = {
                "value": a["value"] + b["value"],
                "unit": a["unit"],
                "scale": a["scale"],
                "type": a["type"],
            }

        elif op == "GT":
            a = resolve_ref(args["a"])
            b = resolve_ref(args["b"])
            if not isinstance(a, dict) or "value" not in a:
                raise ExecError(
                    "E_type_mismatch", "GT expected Quantity for a", {"got": a}
                )
            if not isinstance(b, dict) or "value" not in b:
                raise ExecError(
                    "E_type_mismatch", "GT expected Quantity for b", {"got": b}
                )

            if a.get("type") != b.get("type"):
                raise ExecError(
                    "E_type_mismatch", "GT quantity.type mismatch", {"a": a, "b": b}
                )
            if a.get("unit") != b.get("unit"):
                raise ExecError("E_unit_mismatch", "GT unit mismatch", {"a": a, "b": b})
            if a.get("scale") != b.get("scale"):
                raise ExecError(
                    "E_scale_mismatch", "GT scale mismatch", {"a": a, "b": b}
                )

            env[nid] = {
                "value": bool(a["value"] > b["value"]),
                "unit": "bool",
                "scale": 1,
                "type": "bool",
            }
        elif op == "LT":
            a = resolve_ref(args["a"])
            b = resolve_ref(args["b"])
            if not isinstance(a, dict) or "value" not in a:
                raise ExecError(
                    "E_type_mismatch", "LT expected Quantity for a", {"got": a}
                )
            if not isinstance(b, dict) or "value" not in b:
                raise ExecError(
                    "E_type_mismatch", "LT expected Quantity for b", {"got": b}
                )

            if a.get("type") != b.get("type"):
                raise ExecError(
                    "E_type_mismatch", "LT quantity.type mismatch", {"a": a, "b": b}
                )
            if a.get("unit") != b.get("unit"):
                raise ExecError("E_unit_mismatch", "LT unit mismatch", {"a": a, "b": b})
            if a.get("scale") != b.get("scale"):
                raise ExecError(
                    "E_scale_mismatch", "LT scale mismatch", {"a": a, "b": b}
                )

            env[nid] = {
                "value": bool(a["value"] < b["value"]),
                "unit": "bool",
                "scale": 1,
                "type": "bool",
            }
        elif op == "EQ":
            a = resolve_ref(args["a"])
            b = resolve_ref(args["b"])
            if not isinstance(a, dict) or "value" not in a:
                raise ExecError(
                    "E_type_mismatch", "EQ expected Quantity for a", {"got": a}
                )
            if not isinstance(b, dict) or "value" not in b:
                raise ExecError(
                    "E_type_mismatch", "EQ expected Quantity for b", {"got": b}
                )

            if a.get("type") != b.get("type"):
                raise ExecError(
                    "E_type_mismatch", "EQ quantity.type mismatch", {"a": a, "b": b}
                )
            if a.get("unit") != b.get("unit"):
                raise ExecError("E_unit_mismatch", "EQ unit mismatch", {"a": a, "b": b})
            if a.get("scale") != b.get("scale"):
                raise ExecError(
                    "E_scale_mismatch", "EQ scale mismatch", {"a": a, "b": b}
                )

            env[nid] = {
                "value": bool(a["value"] == b["value"]),
                "unit": "bool",
                "scale": 1,
                "type": "bool",
            }

        elif op == "CONST":
            v = args.get("value")
            if not isinstance(v, (int, float)):
                raise ExecError(
                    "E_bad_args", "CONST requires numeric value", {"got": v}
                )
            env[nid] = {"value": float(v), "unit": "", "scale": 1, "type": "scalar"}

        elif op == "AND":
            a = resolve_ref(args["a"])
            b = resolve_ref(args["b"])
            if not (isinstance(a, dict) and a.get("type") == "bool"):
                raise ExecError(
                    "E_type_mismatch", "AND expected bool for a", {"got": a}
                )
            if not (isinstance(b, dict) and b.get("type") == "bool"):
                raise ExecError(
                    "E_type_mismatch", "AND expected bool for b", {"got": b}
                )
            env[nid] = {
                "value": bool(a["value"] and b["value"]),
                "unit": "bool",
                "scale": 1,
                "type": "bool",
            }

        elif op == "OR":
            a = resolve_ref(args["a"])
            b = resolve_ref(args["b"])
            if not (isinstance(a, dict) and a.get("type") == "bool"):
                raise ExecError("E_type_mismatch", "OR expected bool for a", {"got": a})
            if not (isinstance(b, dict) and b.get("type") == "bool"):
                raise ExecError("E_type_mismatch", "OR expected bool for b", {"got": b})
            env[nid] = {
                "value": bool(a["value"] or b["value"]),
                "unit": "bool",
                "scale": 1,
                "type": "bool",
            }

        elif op == "CPI_LOOKUP":
            series_id = args.get("series_id")
            from_year = args.get("from_year")
            to_year = args.get("to_year")

            if not isinstance(series_id, str) or not series_id.strip():
                raise ExecError("E_bad_args", "CPI_LOOKUP requires series_id string")
            if not isinstance(from_year, (int, str)) or not isinstance(
                to_year, (int, str)
            ):
                raise ExecError(
                    "E_bad_args", "CPI_LOOKUP requires from_year and to_year"
                )

            env[nid] = cpi_rate_lookup(series_id, from_year, to_year, cache=cache)

        elif op == "FX_LOOKUP":
            series_id = args.get("series_id")
            year = args.get("year")
            if not isinstance(series_id, str) or not series_id.strip():
                raise ExecError("E_bad_args", "FX_LOOKUP requires series_id string")
            if not isinstance(year, (int, str)):
                raise ExecError("E_bad_args", "FX_LOOKUP requires year")

            env[nid] = fx_lookup(series_id, year, cache=cache)

        elif op == "DIV":
            a = resolve_ref(args["a"])
            b = resolve_ref(args["b"])
            if not isinstance(a, dict) or "value" not in a:
                raise ExecError(
                    "E_type_mismatch", "DIV expected Quantity for a", {"got": a}
                )
            if not isinstance(b, dict) or "value" not in b:
                raise ExecError(
                    "E_type_mismatch", "DIV expected Quantity for b", {"got": b}
                )

            # ---------- case 1: rate(percent) / scalar -> scalar ----------
            if (
                a.get("type") == "rate"
                and a.get("unit") == "percent"
                and a.get("scale") == 1
                and _is_scalar(b)
            ):
                denom = float(b["value"])
                if denom == 0.0:
                    raise ExecError("E_div_zero", "DIV by zero", {"b": b})
                env[nid] = {
                    "value": float(a["value"]) / denom,
                    "unit": "",
                    "scale": 1,
                    "type": "scalar",
                }

            # ---------- case 2: scalar / scalar -> scalar ----------
            elif _is_scalar(a) and _is_scalar(b):
                denom = float(b["value"])
                if denom == 0.0:
                    raise ExecError("E_div_zero", "DIV by zero", {"b": b})
                env[nid] = {
                    "value": float(a["value"]) / denom,
                    "unit": "",
                    "scale": 1,
                    "type": "scalar",
                }

            # ---------- case 3: money / money -> scalar ratio ----------
            elif a.get("type") == "money" and b.get("type") == "money":
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

                env[nid] = {
                    "value": float(a["value"]) / denom,
                    "unit": "",
                    "scale": 1,
                    "type": "scalar",
                }

            else:
                raise ExecError(
                    "E_type_mismatch",
                    "DIV supports (percent/scalar)->scalar or (scalar/scalar)->scalar or (money/money)->scalar",
                    {"a": a, "b": b},
                )

        elif op == "MUL":
            a = resolve_ref(args["a"])
            b = resolve_ref(args["b"])
            if not isinstance(a, dict) or "value" not in a:
                raise ExecError(
                    "E_type_mismatch", "MUL expected Quantity for a", {"got": a}
                )
            if not isinstance(b, dict) or "value" not in b:
                raise ExecError(
                    "E_type_mismatch", "MUL expected Quantity for b", {"got": b}
                )

            # scalar * scalar -> scalar
            if _is_scalar(a) and _is_scalar(b):
                env[nid] = {
                    "value": float(a["value"]) * float(b["value"]),
                    "unit": "",
                    "scale": 1,
                    "type": "scalar",
                }

            # money * scalar -> money
            elif a.get("type") == "money" and _is_scalar(b):
                env[nid] = {**a, "value": float(a["value"]) * float(b["value"])}

            # scalar * money -> money
            elif _is_scalar(a) and b.get("type") == "money":
                env[nid] = {**b, "value": float(a["value"]) * float(b["value"])}

            # money * fx_rate -> money (currency conversion)
            elif a.get("type") == "money" and _is_rate(b, "fx_rate"):
                frm = _rate_from(b).get("currency")
                to = _rate_to(b).get("currency")
                if not isinstance(frm, str) or not isinstance(to, str):
                    raise ExecError(
                        "E_bad_rate", "fx_rate missing from/to.currency", {"rate": b}
                    )
                if a.get("unit") != frm:
                    raise ExecError(
                        "E_unit_mismatch",
                        "FX rate from.currency must match money unit",
                        {"money_unit": a.get("unit"), "rate_from": frm, "rate": b},
                    )

                env[nid] = {
                    **a,
                    "value": float(a["value"]) * float(b["value"]),
                    "unit": to,
                }

            # money * cpi_rate -> money (real-value adjustment)
            elif a.get("type") == "money" and _is_rate(b, "cpi_rate"):
                fy = _rate_from(b).get("year")
                ty = _rate_to(b).get("year")
                if not isinstance(fy, int) or not isinstance(ty, int):
                    raise ExecError(
                        "E_bad_rate", "cpi_rate missing from/to.year ints", {"rate": b}
                    )

                ap = _get_q_period(a)
                if ap is None:
                    raise ExecError(
                        "E_missing_period",
                        "CPI adjustment requires money to carry FY provenance (_period_kind/_period_value)",
                        {"money": a, "rate": b},
                    )
                pk, pv = ap
                if pk != "FY" or not isinstance(pv, int):
                    raise ExecError(
                        "E_period_mismatch",
                        "CPI adjustment requires FY int provenance on money",
                        {"money_period": ap, "rate": b},
                    )
                if pv != fy:
                    raise ExecError(
                        "E_period_mismatch",
                        "CPI rate from_year must match money FY year",
                        {"money_year": pv, "rate_from_year": fy, "rate": b},
                    )

                env[nid] = {**a, "value": float(a["value"]) * float(b["value"])}

            else:
                raise ExecError(
                    "E_type_mismatch",
                    "MUL supports scalar*scalar, money*scalar, scalar*money, money*fx_rate, money*cpi_rate",
                    {"a": a, "b": b},
                )

        if op != "TEXT_LOOKUP":
            trace.append(
                {
                    "node": nid,
                    "op": op,
                    "args": args,
                    "result": env[nid],
                }
            )

    output = resolve_ref(out_ref)
    if isinstance(output, dict) and {"value", "unit", "scale", "type"} <= set(
        output.keys()
    ):
        output = output  # TODO restore this? # _q_norm(output)
    return {"output": output, "trace": trace}
