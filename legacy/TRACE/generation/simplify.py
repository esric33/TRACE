from __future__ import annotations
from typing import Any, Dict, Tuple

from TRACE.generation.generation_types import Bindings, Spec, CompiledPlan


def simplify_plan(
    spec: Spec, bindings: Bindings, compiled: CompiledPlan
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    dag_raw = compiled.dag
    meta: Dict[str, Any] = {}

    nodes = list(dag_raw.get("nodes", []))
    out_ref = dag_raw.get("output")

    noop_ids = list(compiled.meta.get("noop_convert_scale_nodes", []))
    removed: list[str] = []

    if not noop_ids:
        meta["simplify_removed_nodes"] = removed
        return dag_raw, meta

    # id -> node
    by_id = {n.get("id"): n for n in nodes if n.get("id")}

    # Build replacement map: "ref:<noop_id>" -> "<input_ref>"
    repl: Dict[str, str] = {}
    for nid in noop_ids:
        n = by_id.get(nid)
        if not n:
            continue
        if n.get("op") != "CONVERT_SCALE":
            continue
        qref = n.get("args", {}).get("q")
        if not isinstance(qref, str) or not qref.startswith("ref:"):
            # If this ever happens, don't simplify it.
            continue
        repl[f"ref:{nid}"] = qref
        removed.append(nid)

    if not repl:
        meta["simplify_removed_nodes"] = []
        return dag_raw, meta

    # Remove noop nodes
    nodes2 = [n for n in nodes if n.get("id") not in set(removed)]

    # Rewrite refs in args
    def rewrite(x: Any) -> Any:
        if isinstance(x, str) and x in repl:
            return repl[x]
        if isinstance(x, list):
            return [rewrite(v) for v in x]
        if isinstance(x, dict):
            return {k: rewrite(v) for k, v in x.items()}
        return x

    for n in nodes2:
        n["args"] = rewrite(n.get("args", {}))

    out2 = rewrite(out_ref)

    dag_can = {"nodes": nodes2, "output": out2}
    meta["simplify_removed_nodes"] = removed
    meta["simplify_noop_convert_scale_nodes"] = noop_ids
    return dag_can, meta
