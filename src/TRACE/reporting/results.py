from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from TRACE.reporting.dag_metrics import (
    anchored_graph_diagnostics,
    dag_diagnostic_categories,
    dag_struct_metrics,
    fact_grounding_diagnostics,
    fact_grounding_metrics,
)
from TRACE.reporting.evaluation import compare_outputs

from TRACE.shared.io import append_jsonl


def _op_counts(trace: Optional[list[dict]]) -> Dict[str, int]:
    c: Counter[str] = Counter()
    for t in trace or []:
        op = t.get("op")
        if isinstance(op, str):
            c[op] += 1
    return dict(c)


def _fact_trace(trace: Optional[list[dict]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for step in trace or []:
        if step.get("op") != "MODEL_FACT":
            continue
        out.append(
            {
                "node": step.get("node"),
                "model_fact": step.get("args"),
                "result": step.get("result"),
            }
        )
    return out


def _gold_fact_extraction_ids(capsule: Dict[str, Any]) -> list[str]:
    fact_map = (capsule.get("gold", {}) or {}).get("fact_map") or {}
    if not isinstance(fact_map, dict):
        return []
    return [ex_id for ex_id in fact_map.values() if isinstance(ex_id, str)]


def _pred_fact_extraction_ids(capsule: Dict[str, Any], trace: Optional[list[dict]]) -> list[Optional[str]]:
    resolutions = fact_grounding_diagnostics(capsule, trace).get("fact_resolutions", [])
    out: list[Optional[str]] = []
    for item in resolutions if isinstance(resolutions, list) else []:
        ex_id = item.get("resolved_extraction_id") if isinstance(item, dict) else None
        out.append(ex_id if isinstance(ex_id, str) else None)
    return out


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


def _err_data(exec_error: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not exec_error:
        return {}
    data = exec_error.get("data")
    return data if isinstance(data, dict) else {}


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


def _failure_stage(
    *,
    ok: bool,
    exec_error: Optional[Dict[str, Any]],
    fact_diag: Dict[str, Any],
) -> str:
    if ok:
        return "correct"

    code = _err_code(exec_error)
    data = _err_data(exec_error)
    phase = data.get("phase")
    if code == "E_planner_invalid" or phase == "planner":
        return "planner_invalid"
    if exec_error:
        return "execution_failed"
    if (
        fact_diag.get("fact_under_extraction", 0)
        or fact_diag.get("fact_over_extraction", 0)
        or fact_diag.get("fact_unresolved", 0)
    ):
        return "under_grounded"
    return "output_mismatch"


@dataclass(frozen=True)
class RunConfig:
    mode: str
    planner: str
    model: Optional[str] = None


def build_result_row(
    *,
    capsule: Dict[str, Any],
    cfg: RunConfig,
    ok: bool,
    output: Any,
    gold: Any,
    trace: Optional[list[dict]],
    exec_error: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta = capsule.get("meta", {}) or {}
    template_id = meta.get("template_id")

    # --- DAG + fact metrics (default empty) ---
    dag_m: Dict[str, Any] = {}
    dag_diag: Dict[str, Any] = {}
    fact_m: Dict[str, Any] = {}
    fact_diag: Dict[str, Any] = {}
    anchored_diag: Dict[str, Any] = {}

    # We consider these meaningful only when a planner produced a DAG (mode=full)
    # but we guard anyway.
    gold_dag = capsule.get("gold", {}).get("dag")
    # 'extra' from caller may include exec_dag / gold_dag too; prefer capsule gold.
    exec_dag = None
    if extra and isinstance(extra, dict):
        exec_dag = extra.get("exec_dag")

    if cfg.mode == "full" and isinstance(gold_dag, dict) and isinstance(exec_dag, dict):
        dag_m = dag_struct_metrics(gold_dag, exec_dag)
        dag_diag = dag_diagnostic_categories(gold_dag, exec_dag)
        anchored_diag = anchored_graph_diagnostics(capsule, trace, gold_dag, exec_dag)

    if cfg.mode == "full":
        fact_m = fact_grounding_metrics(capsule, trace)
        fact_diag = fact_grounding_diagnostics(capsule, trace)

    err_data = _err_data(exec_error)
    comparison = compare_outputs(output, gold) if not exec_error else None
    fact_trace = _fact_trace(trace)

    row: Dict[str, Any] = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "qid": capsule.get("qid"),
        "benchmark_id": meta.get("benchmark_id")
        or (extra or {}).get("benchmark_id"),
        "template_id": template_id,
        "qkey": _qkey(template_id),
        "family": meta.get("family") or _family_from_template_id(template_id),
        "distractor_policy": meta.get("distractor_policy"),
        "seed": meta.get("seed"),
        "generator_version": meta.get("generator_version"),
        "mode": cfg.mode,
        "planner": cfg.planner,
        "model": cfg.model,
        "correct": bool(ok),
        "exec_error": exec_error,
        "exec_error_code": _err_code(exec_error),
        "exec_error_phase": err_data.get("phase"),
        "exec_error_op": err_data.get("op"),
        "exec_error_node_id": err_data.get("node_id"),
        "exec_error_arg": err_data.get("arg"),
        "trace_nodes": len(trace or []),
        "ops": _op_counts(trace),
        "fact_trace": fact_trace,
        "fact_gold_extraction_ids": _gold_fact_extraction_ids(capsule),
        "fact_pred_extraction_ids": _pred_fact_extraction_ids(capsule, trace),
        "output": output,
        "gold": gold,
        "planner_prompt": (extra or {}).get("planner_prompt"),
        # Only meaningful when correct==False and exec_error is None:
        "mismatch_kind": None if ok or exec_error or comparison is None else comparison.mismatch_kind,
        "comparison": None if exec_error or comparison is None else comparison.details,
        "failure_stage": _failure_stage(
            ok=bool(ok), exec_error=exec_error, fact_diag=fact_diag
        ),
        "extra": extra,
        **dag_m,
        **dag_diag,
        **fact_m,
        **fact_diag,
        **anchored_diag,
    }

    return row


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
    row = build_result_row(
        capsule=capsule,
        cfg=cfg,
        ok=ok,
        output=output,
        gold=gold,
        trace=trace,
        exec_error=exec_error,
        extra=extra,
    )
    append_jsonl(path=Path(out_path), obj=row)
