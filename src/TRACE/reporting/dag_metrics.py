# reason_bench/reporting/dag_metrics.py
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from TRACE.shared.text_norm import normalize_relation_text

ASSOCIATIVE_COMMUTATIVE_OPS = {"ADD", "MUL", "AND", "OR", "SET_UNION", "SET_INTERSECT"}
COMMUTATIVE_OPS = ASSOCIATIVE_COMMUTATIVE_OPS | {"EQ", "MAKE_SET"}


def _period_kind_value(p: Any) -> Tuple[Optional[str], Optional[int]]:
    if not isinstance(p, dict):
        return (None, None)
    k = p.get("period")
    v = p.get("value")
    if not isinstance(k, str):
        k = None
    try:
        v_i = int(v)
    except Exception:
        if isinstance(v, str):
            match = re.search(r"(\d{2,4})", v)
            v_i = int(match.group(1)) if match else None
        else:
            v_i = None
    # Financial source tables often abbreviate fiscal years as '20, '21, etc.
    # Treat those as 2020, 2021 for reporting/matching so otherwise identical
    # MODEL_FACT nodes are not scored as completely unrelated.
    if k in {"FY", "ASOF"} and v_i is not None and 0 <= v_i < 100:
        v_i += 2000
    return (k, v_i)


@dataclass(frozen=True)
class Node:
    nid: str
    op: str
    attrs: Tuple[Tuple[str, str], ...]
    # deps are node ids (not refs)
    deps: Tuple[str, ...]


@dataclass(frozen=True)
class Dag:
    nodes: Dict[str, Node]
    output: Optional[str]


def _ref_target(x: Any) -> Optional[str]:
    if isinstance(x, str) and x.startswith("ref:"):
        return x.split(":", 1)[1]
    return None


def _collect_ref_targets(value: Any) -> list[str]:
    target = _ref_target(value)
    if target:
        return [target]
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            out.extend(_collect_ref_targets(item))
        return out
    if isinstance(value, dict):
        out: list[str] = []
        for item in value.values():
            out.extend(_collect_ref_targets(item))
        return out
    return []


def _freeze_value(value: Any) -> str:
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def _normalize_number(value: Any) -> Any:
    try:
        f = float(value)
    except Exception:
        return value
    return int(f) if f.is_integer() else f


def _normalize_period(period: Any) -> Any:
    if not isinstance(period, dict):
        return period
    kind, value = _period_kind_value(period)
    if kind is None:
        return period
    return {"period": kind, "value": value if value is not None else period.get("value")}


def _normalize_quantity(quantity: Any) -> Any:
    if not isinstance(quantity, dict):
        return quantity
    out = dict(quantity)
    if "value" in out:
        out["value"] = _normalize_number(out["value"])
    if "scale" in out:
        out["scale"] = _normalize_number(out["scale"])
    return out


def _normalize_model_fact_args(args: dict[str, Any]) -> dict[str, Any]:
    out = dict(args)
    if "period" in out:
        out["period"] = _normalize_period(out["period"])
    if "quantity" in out:
        out["quantity"] = _normalize_quantity(out["quantity"])
    if isinstance(out.get("subject"), dict):
        subject = dict(out["subject"])
        if "value" in subject:
            subject["value"] = _norm_text(subject["value"])
        out["subject"] = subject
    if isinstance(out.get("object"), dict):
        obj = dict(out["object"])
        if "value" in obj:
            obj["value"] = _norm_text(obj["value"])
        out["object"] = obj
    return out


def parse_dag(dag: Dict[str, Any]) -> Dag:
    nodes: Dict[str, Node] = {}
    for n in dag.get("nodes", []) or []:
        nid = n.get("id")
        op = n.get("op")
        args = n.get("args") or {}
        if not isinstance(nid, str) or not isinstance(op, str):
            continue

        deps: List[str] = []
        attrs: List[Tuple[str, str]] = []
        if isinstance(args, dict):
            if op == "MODEL_FACT":
                args = _normalize_model_fact_args(args)
            for k, v in args.items():
                refs = _collect_ref_targets(v)
                if refs:
                    deps.extend(refs)
                else:
                    attrs.append((str(k), _freeze_value(v)))

        nodes[nid] = Node(nid=nid, op=op, attrs=tuple(sorted(attrs)), deps=tuple(deps))

    out_ref = _ref_target(dag.get("output"))
    return Dag(nodes=nodes, output=out_ref)


def canonical_subtree_sig(d: Dag, nid: str, memo: Dict[str, str]) -> str:
    if nid in memo:
        return memo[nid]
    n = d.nodes.get(nid)
    if n is None:
        memo[nid] = "MISSING"
        return memo[nid]

    child_sigs = [canonical_subtree_sig(d, c, memo) for c in n.deps]
    if n.op in COMMUTATIVE_OPS:
        child_sigs.sort()

    attrs = ""
    if n.attrs:
        attrs = "[" + ",".join(f"{key}={value}" for key, value in n.attrs) + "]"
    sig = f"{n.op}{attrs}(" + ",".join(child_sigs) + ")"
    memo[nid] = sig
    return sig


Expr = Tuple[str, Tuple[Tuple[str, str], ...], Tuple[Any, ...]]


def _expr_to_str(expr: Any) -> str:
    if not isinstance(expr, tuple) or len(expr) != 3:
        return _freeze_value(expr)
    op, attrs, children = expr
    attrs_s = ""
    if attrs:
        attrs_s = "[" + ",".join(f"{key}={value}" for key, value in attrs) + "]"
    return f"{op}{attrs_s}(" + ",".join(_expr_to_str(child) for child in children) + ")"


def canonical_expr(d: Dag, nid: str, memo: Dict[str, Expr]) -> Expr:
    if nid in memo:
        return memo[nid]
    n = d.nodes.get(nid)
    if n is None:
        expr: Expr = ("MISSING", tuple(), tuple())
        memo[nid] = expr
        return expr

    children = [canonical_expr(d, child, memo) for child in n.deps]
    if n.op in ASSOCIATIVE_COMMUTATIVE_OPS:
        flattened: list[Any] = []
        for child in children:
            if (
                isinstance(child, tuple)
                and len(child) == 3
                and child[0] == n.op
                and child[1] == n.attrs
            ):
                flattened.extend(child[2])
            else:
                flattened.append(child)
        children = sorted(flattened, key=_expr_to_str)
    elif n.op in COMMUTATIVE_OPS:
        children = sorted(children, key=_expr_to_str)

    expr = (n.op, n.attrs, tuple(children))
    memo[nid] = expr
    return expr


def _expr_node_sig(expr: Expr) -> Tuple[str, Tuple[Tuple[str, str], ...], int]:
    op, attrs, children = expr
    return (op, attrs, len(children))


def _expr_nodes(expr: Expr) -> list[Tuple[str, Tuple[Tuple[str, str], ...], int]]:
    out = [_expr_node_sig(expr)]
    for child in expr[2]:
        out.extend(_expr_nodes(child))
    return out


def _expr_edges(
    expr: Expr,
) -> list[
    Tuple[
        Tuple[str, Tuple[Tuple[str, str], ...], int],
        Tuple[str, Tuple[Tuple[str, str], ...], int],
    ]
]:
    out = []
    parent = _expr_node_sig(expr)
    for child in expr[2]:
        out.append((parent, _expr_node_sig(child)))
        out.extend(_expr_edges(child))
    return out


def _output_expr(dag: Dict[str, Any]) -> Expr | None:
    d = parse_dag(dag)
    if not d.output or d.output not in d.nodes:
        return None
    return canonical_expr(d, d.output, {})


def canonicalize(d: Dag) -> Dag:
    """
    Canonicalize commutative ops by sorting deps by subtree signature.
    """
    memo: Dict[str, str] = {}
    out: Dict[str, Node] = {}

    for nid, n in d.nodes.items():
        if n.op in COMMUTATIVE_OPS and n.deps:
            deps = list(n.deps)
            deps.sort(key=lambda c: canonical_subtree_sig(d, c, memo))
            out[nid] = Node(nid=nid, op=n.op, attrs=n.attrs, deps=tuple(deps))
        else:
            out[nid] = n
    return Dag(nodes=out, output=d.output)


def node_signature(n: Node) -> Tuple[str, Tuple[Tuple[str, str], ...], int]:
    # op + semantic constants + arity
    return (n.op, n.attrs, len(n.deps))


def edge_signature(
    parent: Node, child_id: str, child: Node
) -> Tuple[
    Tuple[str, Tuple[Tuple[str, str], ...], int],
    Tuple[str, Tuple[Tuple[str, str], ...], int],
]:
    # typed-ish edge: (parent op/attrs/arity) -> (child op/attrs/arity)
    return (node_signature(parent), node_signature(child))


def multiset_prf(gold: List[Any], pred: List[Any]) -> Dict[str, float]:
    """
    Precision/recall/F1 for multisets (counts matter).
    """
    from collections import Counter

    cg = Counter(gold)
    cp = Counter(pred)
    inter = sum((cg & cp).values())
    g_n = sum(cg.values())
    p_n = sum(cp.values())

    prec = inter / p_n if p_n else 0.0
    rec = inter / g_n if g_n else 0.0
    f1 = (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0
    return {
        "match": float(inter),
        "gold_n": float(g_n),
        "pred_n": float(p_n),
        "prec": float(prec),
        "rec": float(rec),
        "f1": float(f1),
    }


def dag_struct_metrics(
    gold_dag: Dict[str, Any], pred_dag: Dict[str, Any]
) -> Dict[str, Any]:
    g_expr = _output_expr(gold_dag)
    p_expr = _output_expr(pred_dag)

    g_nodes = _expr_nodes(g_expr) if g_expr is not None else []
    p_nodes = _expr_nodes(p_expr) if p_expr is not None else []
    node_prf = multiset_prf(g_nodes, p_nodes)

    g_edges = _expr_edges(g_expr) if g_expr is not None else []
    p_edges = _expr_edges(p_expr) if p_expr is not None else []
    edge_prf = multiset_prf(g_edges, p_edges)

    exact = g_expr is not None and p_expr is not None and g_expr == p_expr

    return {
        "dag_node_prec": node_prf["prec"],
        "dag_node_rec": node_prf["rec"],
        "dag_node_f1": node_prf["f1"],
        "dag_edge_prec": edge_prf["prec"],
        "dag_edge_rec": edge_prf["rec"],
        "dag_edge_f1": edge_prf["f1"],
        "dag_exact": bool(exact),
        "dag_nodes_gold": int(node_prf["gold_n"]),
        "dag_nodes_pred": int(node_prf["pred_n"]),
        "dag_edges_gold": int(edge_prf["gold_n"]),
        "dag_edges_pred": int(edge_prf["pred_n"]),
    }


# -----------------------------------------------------------------------------
# Fact grounding metrics
# -----------------------------------------------------------------------------


def _quantity_base(q: Any) -> Optional[float]:
    if not isinstance(q, dict):
        return None
    try:
        return float(q["value"]) * float(q["scale"])
    except Exception:
        return None


def _float_close(a: Any, b: Any, *, rel_tol: float = 1e-12, abs_tol: float = 1e-9) -> bool:
    try:
        import math

        return math.isclose(float(a), float(b), rel_tol=rel_tol, abs_tol=abs_tol)
    except Exception:
        return a == b


def _fact_sig(fact: Any) -> Tuple[Any, ...]:
    if not isinstance(fact, dict):
        return ("invalid",)
    if "subject" in fact and "object" in fact:
        subject = fact.get("subject")
        obj = fact.get("object")
        return (
            "relation",
            fact.get("snippet_id"),
            fact.get("label"),
            subject.get("type") if isinstance(subject, dict) else None,
            _norm_text(subject.get("value")) if isinstance(subject, dict) else None,
            obj.get("type") if isinstance(obj, dict) else None,
            _norm_text(obj.get("value")) if isinstance(obj, dict) else None,
        )
    q = fact.get("quantity")
    pk, pv = _period_kind_value(fact.get("period"))
    base = _quantity_base(q)
    return (
        fact.get("snippet_id"),
        fact.get("label"),
        pk,
        pv,
        q.get("type") if isinstance(q, dict) else None,
        q.get("unit") if isinstance(q, dict) else None,
        round(base, 9) if isinstance(base, float) else base,
    )


def _norm_text(value: Any) -> str:
    return normalize_relation_text(value)


def _gold_facts_by_extraction_id(capsule: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    gold = capsule.get("gold", {}) or {}
    fact_map = gold.get("fact_map") or {}
    dag = gold.get("dag") or {}
    node_by_id = {
        node.get("id"): node
        for node in dag.get("nodes", []) or []
        if isinstance(node, dict)
    }
    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(fact_map, dict):
        for node_id, ex_id in fact_map.items():
            node = node_by_id.get(node_id)
            args = node.get("args") if isinstance(node, dict) else None
            if isinstance(ex_id, str) and isinstance(args, dict):
                out[ex_id] = args
    return out


def _pred_model_facts(trace: Optional[list[dict]]) -> List[Tuple[str, Dict[str, Any]]]:
    out: List[Tuple[str, Dict[str, Any]]] = []
    for step in trace or []:
        if step.get("op") != "MODEL_FACT":
            continue
        node = step.get("node")
        args = step.get("args")
        if isinstance(node, str) and isinstance(args, dict):
            out.append((node, args))
    return out


def resolve_predicted_facts(capsule: Dict[str, Any], trace: Optional[list[dict]]) -> List[Dict[str, Any]]:
    gold_by_id = _gold_facts_by_extraction_id(capsule)
    available = dict(gold_by_id)
    resolutions: List[Dict[str, Any]] = []
    for node_id, pred_fact in _pred_model_facts(trace):
        pred_sig = _fact_sig(pred_fact)
        matches = [
            ex_id
            for ex_id, gold_fact in available.items()
            if _fact_sig(gold_fact) == pred_sig
        ]
        status = "resolved" if len(matches) == 1 else ("ambiguous" if matches else "no_match")
        resolved = matches[0] if len(matches) == 1 else None
        if resolved is not None:
            available.pop(resolved, None)
        tags: list[str] = []
        if status == "ambiguous":
            tags.append("fact_ambiguous")
        elif status == "no_match":
            tags.append("fact_no_match")
            tags.extend(_fact_mismatch_tags(pred_fact, gold_by_id))
        resolutions.append(
            {
                "node": node_id,
                "status": status,
                "resolved_extraction_id": resolved,
                "tags": tags,
            }
        )
    return resolutions


def _fact_mismatch_tags(pred_fact: Dict[str, Any], gold_by_id: Dict[str, Dict[str, Any]]) -> list[str]:
    if not gold_by_id:
        return ["fact_no_gold_candidates"]
    if "subject" in pred_fact and "object" in pred_fact:
        return _relation_fact_mismatch_tags(pred_fact, gold_by_id)
    tags: set[str] = set()
    pred_q = pred_fact.get("quantity")
    pred_pk, pred_pv = _period_kind_value(pred_fact.get("period"))
    for gold_fact in gold_by_id.values():
        gold_q = gold_fact.get("quantity")
        gold_pk, gold_pv = _period_kind_value(gold_fact.get("period"))
        if pred_fact.get("snippet_id") == gold_fact.get("snippet_id"):
            if pred_fact.get("label") != gold_fact.get("label"):
                tags.add("fact_wrong_label")
            if (pred_pk, pred_pv) != (gold_pk, gold_pv):
                tags.add("fact_wrong_period")
            if isinstance(pred_q, dict) and isinstance(gold_q, dict):
                if pred_q.get("unit") != gold_q.get("unit"):
                    tags.add("fact_wrong_unit")
                if pred_q.get("type") != gold_q.get("type"):
                    tags.add("fact_wrong_type")
                pred_base = _quantity_base(pred_q)
                gold_base = _quantity_base(gold_q)
                if pred_base is None or gold_base is None or not _float_close(pred_base, gold_base):
                    tags.add("fact_wrong_quantity")
                elif not _float_close(pred_q.get("scale"), gold_q.get("scale")):
                    tags.add("fact_scale_repr_diff_only")
            return sorted(tags) or ["fact_other_mismatch"]
    return ["fact_wrong_snippet"]


def _relation_fact_mismatch_tags(
    pred_fact: Dict[str, Any], gold_by_id: Dict[str, Dict[str, Any]]
) -> list[str]:
    tags: set[str] = set()
    pred_subject = pred_fact.get("subject") if isinstance(pred_fact.get("subject"), dict) else {}
    pred_object = pred_fact.get("object") if isinstance(pred_fact.get("object"), dict) else {}
    for gold_fact in gold_by_id.values():
        gold_subject = gold_fact.get("subject") if isinstance(gold_fact.get("subject"), dict) else {}
        gold_object = gold_fact.get("object") if isinstance(gold_fact.get("object"), dict) else {}
        if pred_fact.get("snippet_id") == gold_fact.get("snippet_id"):
            if pred_fact.get("label") != gold_fact.get("label"):
                tags.add("fact_wrong_label")
            if pred_subject.get("type") != gold_subject.get("type"):
                tags.add("fact_wrong_subject_type")
            if _norm_text(pred_subject.get("value")) != _norm_text(gold_subject.get("value")):
                tags.add("fact_wrong_subject")
            if pred_object.get("type") != gold_object.get("type"):
                tags.add("fact_wrong_object_type")
            if _norm_text(pred_object.get("value")) != _norm_text(gold_object.get("value")):
                tags.add("fact_wrong_object")
            return sorted(tags) or ["fact_other_mismatch"]
    return ["fact_wrong_snippet"]


def extract_pred_fact_sigs(capsule: Dict[str, Any], trace: Optional[list[dict]]) -> List[Tuple[Any, ...]]:
    out: List[Tuple[Any, ...]] = []
    for resolution in resolve_predicted_facts(capsule, trace):
        ex_id = resolution.get("resolved_extraction_id")
        if isinstance(ex_id, str):
            out.append(("extraction_id", ex_id))
    return out


def extract_gold_fact_sigs(capsule: Dict[str, Any]) -> List[Tuple[Any, ...]]:
    return [("extraction_id", ex_id) for ex_id in _gold_facts_by_extraction_id(capsule)]


def fact_grounding_metrics(
    capsule: Dict[str, Any], trace: Optional[list[dict]]
) -> Dict[str, Any]:
    gold_sigs = extract_gold_fact_sigs(capsule)
    pred_sigs = extract_pred_fact_sigs(capsule, trace)

    prf = multiset_prf(gold_sigs, pred_sigs)

    return {
        "fact_prec": prf["prec"],
        "fact_rec": prf["rec"],
        "fact_f1": prf["f1"],
        "fact_gold_n": int(prf["gold_n"]),
        "fact_pred_n": int(prf["pred_n"]),
    }


def _counter_to_dict(counter: Counter[Any]) -> Dict[str, int]:
    return {str(key): int(counter[key]) for key in sorted(counter, key=str)}


def dag_diagnostic_categories(
    gold_dag: Dict[str, Any], pred_dag: Dict[str, Any]
) -> Dict[str, Any]:
    g_expr = _output_expr(gold_dag)
    p_expr = _output_expr(pred_dag)
    g_output_op = g_expr[0] if g_expr is not None else None
    p_output_op = p_expr[0] if p_expr is not None else None

    g_node_types = Counter(sig[0] for sig in (_expr_nodes(g_expr) if g_expr is not None else []))
    p_node_types = Counter(sig[0] for sig in (_expr_nodes(p_expr) if p_expr is not None else []))
    missing_node_types = g_node_types - p_node_types
    extra_node_types = p_node_types - g_node_types

    g_edges = Counter(_expr_edges(g_expr) if g_expr is not None else [])
    p_edges = Counter(_expr_edges(p_expr) if p_expr is not None else [])

    missing_edges = g_edges - p_edges
    extra_edges = p_edges - g_edges

    return {
        "dag_wrong_output_op": bool(g_output_op != p_output_op),
        "dag_output_op_gold": g_output_op,
        "dag_output_op_pred": p_output_op,
        "dag_missing_node_types": _counter_to_dict(missing_node_types),
        "dag_extra_node_types": _counter_to_dict(extra_node_types),
        "dag_missing_node_type_count": int(sum(missing_node_types.values())),
        "dag_extra_node_type_count": int(sum(extra_node_types.values())),
        "dag_missing_edge_count": int(sum(missing_edges.values())),
        "dag_extra_edge_count": int(sum(extra_edges.values())),
        "dag_wrong_dependency_edges": int(
            sum(missing_edges.values()) + sum(extra_edges.values())
        ),
    }


def fact_grounding_diagnostics(
    capsule: Dict[str, Any], trace: Optional[list[dict]]
) -> Dict[str, Any]:
    gold_sigs = extract_gold_fact_sigs(capsule)
    pred_sigs = extract_pred_fact_sigs(capsule, trace)
    gold_counter: Counter[Tuple[Any, ...]] = Counter(gold_sigs)
    pred_counter: Counter[Tuple[Any, ...]] = Counter(pred_sigs)
    exact_matches = gold_counter & pred_counter

    inter = sum(exact_matches.values())
    gold_n = sum(gold_counter.values())
    resolutions = resolve_predicted_facts(capsule, trace)
    pred_attempt_n = len(resolutions)
    status_counts = Counter(
        str(item.get("status")) for item in resolutions if isinstance(item.get("status"), str)
    )
    tag_counts: Counter[str] = Counter()
    for item in resolutions:
        for tag in item.get("tags", []) if isinstance(item.get("tags"), list) else []:
            if isinstance(tag, str):
                tag_counts[tag] += 1

    return {
        "fact_exact": bool(gold_counter == pred_counter),
        "fact_under_extraction": int(max(0, gold_n - inter)),
        "fact_over_extraction": int(max(0, pred_attempt_n - inter)),
        "fact_unresolved": int(sum(1 for item in resolutions if item.get("status") != "resolved")),
        "fact_resolution_status_counts": _counter_to_dict(status_counts),
        "fact_error_tag_counts": _counter_to_dict(tag_counts),
        "fact_resolutions": resolutions,
    }


def _trace_result_by_node(trace: Optional[list[dict]]) -> Dict[str, Any]:
    return {
        step["node"]: step.get("result")
        for step in trace or []
        if isinstance(step, dict) and isinstance(step.get("node"), str)
    }


def _qty_sig(value: Any) -> Tuple[Any, ...]:
    if not isinstance(value, dict):
        return ("non_quantity", _freeze_value(value))
    if value.get("type") == "relation_set":
        items = value.get("items")
        if isinstance(items, list):
            item_sigs = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                obj = item.get("object") if isinstance(item.get("object"), dict) else {}
                item_sigs.append((obj.get("type"), _norm_text(obj.get("value"))))
            return (
                "relation_set",
                value.get("label"),
                value.get("object_type"),
                tuple(sorted(item_sigs)),
            )
    base = None
    if {"value", "scale"} <= set(value):
        base = _quantity_base(value)
    return (
        value.get("type"),
        value.get("unit"),
        round(base, 9) if isinstance(base, float) else base,
    )


def anchored_graph_diagnostics(
    capsule: Dict[str, Any],
    trace: Optional[list[dict]],
    gold_dag: Dict[str, Any],
    pred_dag: Dict[str, Any],
) -> Dict[str, Any]:
    resolutions = resolve_predicted_facts(capsule, trace)
    pred_anchor_by_node = {
        item["node"]: item["resolved_extraction_id"]
        for item in resolutions
        if isinstance(item.get("node"), str) and isinstance(item.get("resolved_extraction_id"), str)
    }
    gold_fact_map = (capsule.get("gold", {}) or {}).get("fact_map") or {}
    if not isinstance(gold_fact_map, dict):
        gold_fact_map = {}

    matched_fact_ids = set(pred_anchor_by_node.values()) & {
        ex_id for ex_id in gold_fact_map.values() if isinstance(ex_id, str)
    }

    gold_nodes = {
        node.get("id"): node
        for node in gold_dag.get("nodes", []) or []
        if isinstance(node, dict) and isinstance(node.get("id"), str)
    }
    pred_nodes = {
        node.get("id"): node
        for node in pred_dag.get("nodes", []) or []
        if isinstance(node, dict) and isinstance(node.get("id"), str)
    }
    pred_results = _trace_result_by_node(trace)

    gold_result_by_node: Dict[str, Any] = {}
    # Gold results are not passed separately; use node args for fact anchors and
    # leave non-fact intermediate comparison to structural matching in this pass.
    for node_id, node in gold_nodes.items():
        if node.get("op") == "MODEL_FACT" and isinstance(node.get("args"), dict):
            args = node["args"]
            if isinstance(args.get("quantity"), dict):
                gold_result_by_node[node_id] = {
                    **args.get("quantity", {}),
                    "source": {
                        "snippet_id": args.get("snippet_id"),
                        "label": args.get("label"),
                        "period": args.get("period"),
                    },
                }
            elif isinstance(args.get("subject"), dict) and isinstance(args.get("object"), dict):
                obj = args["object"]
                subject = args["subject"]
                gold_result_by_node[node_id] = {
                    "type": "relation_set",
                    "label": args.get("label"),
                    "object_type": obj.get("type"),
                    "subject": subject,
                    "subjects": [subject],
                    "items": [
                        {
                            "label": args.get("label"),
                            "subject": subject,
                            "object": obj,
                            "value": obj.get("value"),
                            "source": {
                                "snippet_id": args.get("snippet_id"),
                                "label": args.get("label"),
                                "subject": subject,
                                "object": obj,
                            },
                        }
                    ],
                }

    intermediate_matches = 0
    intermediate_compared = 0
    for pred_node, ex_id in pred_anchor_by_node.items():
        gold_node = next(
            (node_id for node_id, gold_ex_id in gold_fact_map.items() if gold_ex_id == ex_id),
            None,
        )
        if not isinstance(gold_node, str):
            continue
        if pred_node in pred_results and gold_node in gold_result_by_node:
            intermediate_compared += 1
            if _qty_sig(pred_results[pred_node]) == _qty_sig(gold_result_by_node[gold_node]):
                intermediate_matches += 1

    return {
        "anchored_fact_match_count": int(len(matched_fact_ids)),
        "anchored_graph_match": bool(dag_struct_metrics(gold_dag, pred_dag)["dag_exact"]),
        "intermediate_value_match_count": int(intermediate_matches),
        "intermediate_value_compared": int(intermediate_compared),
        "intermediate_value_match_rate": (
            float(intermediate_matches / intermediate_compared)
            if intermediate_compared
            else None
        ),
    }
