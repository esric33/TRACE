from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from collections import Counter

from pathlib import Path
from datetime import datetime, timezone
from collections import Counter

from TRACE.reporting.dag_metrics import (
    dag_struct_metrics,
    lookup_grounding_metrics,
)

from TRACE.shared.io import append_jsonl


def _op_counts(trace: Optional[list[dict]]) -> Dict[str, int]:
    c: Counter[str] = Counter()
    for t in trace or []:
        op = t.get("op")
        if isinstance(op, str):
            c[op] += 1
    return dict(c)


def _family_from_template_id(template_id: Optional[str]) -> Optional[str]:
    if not template_id or not isinstance(template_id, str):
        return None
    # e.g. "A0_ADD__..." -> "A0"
    return template_id.split("_", 1)[0] if "_" in template_id else template_id


def _qkey(template_id: Optional[str]) -> Optional[str]:
    """
    Coarser "question type" bucket.
    Example: A0_ADD4_NORM_TO_A__... -> A0_ADD4_NORM_TO_A
             B0_GT__... -> B0_GT
    """
    if not template_id or not isinstance(template_id, str):
        return None
    return template_id.split("__", 1)[0]


def _err_code(exec_error: Optional[Dict[str, Any]]) -> Optional[str]:
    if not exec_error:
        return None
    c = exec_error.get("code")
    return c if isinstance(c, str) else None


def _qty_sig(x: Any) -> Optional[Tuple[str, str, float]]:
    """
    If x is a Quantity dict, return (type, unit, scale_float).
    """
    if not isinstance(x, dict):
        return None
    if not {"value", "unit", "scale", "type"} <= set(x.keys()):
        return None
    t = x.get("type")
    u = x.get("unit")
    s = x.get("scale", None)
    if not isinstance(t, str) or not isinstance(u, str):
        return None
    try:
        sf = float(s)
    except Exception:
        return None
    return (t, u, sf)


def _mismatch_kind(output: Any, gold: Any) -> Optional[str]:
    """
    Coarse mismatch classifier for non-ExecError failures.
    """
    osig = _qty_sig(output)
    gsig = _qty_sig(gold)
    if osig is None or gsig is None:
        # could be bool/scalar mismatch or raw mismatch; keep generic
        return "value_mismatch"

    ot, ou, os = osig
    gt, gu, gs = gsig

    if ot != gt:
        return "type_mismatch"
    if ou != gu:
        return "unit_mismatch"
    if os != gs:
        return "scale_mismatch"

    # type/unit/scale match but value differs
    return "value_mismatch"


def _period_kind_value(p: Any) -> Tuple[Optional[str], Optional[int]]:
    if not isinstance(p, dict):
        return (None, None)
    k = p.get("period")
    v = p.get("value")
    if isinstance(k, str):
        pass
    else:
        k = None
    # defensively coerce to int
    try:
        v_i = int(v)
    except Exception:
        v_i = None
    return (k, v_i)


@dataclass(frozen=True)
class RunConfig:
    mode: str
    planner: str
    lookup: str
    model: Optional[str] = None


def write_result_row(
    out_path: str | Path,
    *,
    capsule: Dict[str, Any],
    cfg: RunConfig,
    ok: bool,
    output: Any,
    gold: Any,
    trace: Optional[list[dict]],
    exec_error: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    meta = capsule.get("meta", {}) or {}
    template_id = meta.get("template_id")

    meta = capsule.get("meta", {}) or {}
    template_id = meta.get("template_id")

    # --- NEW: DAG + lookup metrics (default None) ---
    dag_m: Dict[str, Any] = {}
    lookup_m: Dict[str, Any] = {}

    # We consider these meaningful only when a planner produced a DAG (mode=full)
    # but we guard anyway.
    gold_dag = capsule.get("gold", {}).get("dag")
    # 'extra' from caller may include exec_dag / gold_dag too; prefer capsule gold.
    exec_dag = None
    if extra and isinstance(extra, dict):
        exec_dag = extra.get("exec_dag")

    if cfg.mode == "full" and isinstance(gold_dag, dict) and isinstance(exec_dag, dict):
        dag_m = dag_struct_metrics(gold_dag, exec_dag)

    # Grounding metrics use capsule meta + trace TEXT_LOOKUP.model_fact
    if cfg.mode == "full":
        lookup_m = lookup_grounding_metrics(capsule, trace)

    row: Dict[str, Any] = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "qid": capsule.get("qid"),
        "template_id": template_id,
        "qkey": _qkey(template_id),
        "family": meta.get("family") or _family_from_template_id(template_id),
        "distractor_policy": meta.get("distractor_policy"),
        "seed": meta.get("seed"),
        "generator_version": meta.get("generator_version"),
        "mode": cfg.mode,
        "planner": cfg.planner,
        "lookup": cfg.lookup,
        "model": cfg.model,
        "correct": bool(ok),
        "exec_error": exec_error,
        "exec_error_code": _err_code(exec_error),
        "trace_nodes": len(trace or []),
        "ops": _op_counts(trace),
        "output": output,
        "gold": gold,
        # Only meaningful when correct==False and exec_error is None:
        "mismatch_kind": None if ok or exec_error else _mismatch_kind(output, gold),
        "extra": extra,
        **dag_m,
        **lookup_m,
    }

    append_jsonl(path=Path(out_path), obj=row)
