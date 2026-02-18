# reason_bench/reporting/dag_metrics.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

COMMUTATIVE_OPS = {"ADD", "MUL"}


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
        v_i = None
    return (k, v_i)


@dataclass(frozen=True)
class Node:
    nid: str
    op: str
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


def parse_dag(dag: Dict[str, Any]) -> Dag:
    nodes: Dict[str, Node] = {}
    for n in dag.get("nodes", []) or []:
        nid = n.get("id")
        op = n.get("op")
        args = n.get("args") or {}
        if not isinstance(nid, str) or not isinstance(op, str):
            continue

        deps: List[str] = []
        if isinstance(args, dict):
            for _k, v in args.items():
                if isinstance(v, str):
                    t = _ref_target(v)
                    if t:
                        deps.append(t)

        nodes[nid] = Node(nid=nid, op=op, deps=tuple(deps))

    out_ref = _ref_target(dag.get("output"))
    return Dag(nodes=nodes, output=out_ref)


def collapse_lookup_qty(d: Dag) -> Dag:
    """
    Collapse TEXT_LOOKUP -> GET_QUANTITY into a single LOOKUP_QTY node
    (keeping the GET_QUANTITY node id as representative).
    """
    nodes = dict(d.nodes)

    # find GET_QUANTITY nodes whose only dependency is a TEXT_LOOKUP
    to_remove = set()
    new_nodes: Dict[str, Node] = {}

    for nid, n in nodes.items():
        if n.op != "GET_QUANTITY":
            continue
        if len(n.deps) != 1:
            continue
        dep = n.deps[0]
        dep_node = nodes.get(dep)
        if dep_node is None or dep_node.op != "TEXT_LOOKUP":
            continue

        # Replace GET_QUANTITY node with LOOKUP_QTY node (deps of TEXT_LOOKUP become none)
        new_nodes[nid] = Node(nid=nid, op="LOOKUP_QTY", deps=tuple())
        to_remove.add(dep)

    # Build final node dict:
    out: Dict[str, Node] = {}
    for nid, n in nodes.items():
        if nid in to_remove:
            continue
        if nid in new_nodes:
            out[nid] = new_nodes[nid]
        else:
            # drop deps that point to removed TEXT_LOOKUP nodes; and redirect any deps that
            # pointed to TEXT_LOOKUP? (shouldn't happen other than GET_QUANTITY)
            deps = tuple(dep for dep in n.deps if dep not in to_remove)
            out[nid] = Node(nid=nid, op=n.op, deps=deps)

    # output may refer to removed node in degenerate cases; normally not.
    output = d.output
    if output in to_remove:
        output = None

    return Dag(nodes=out, output=output)


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

    sig = f"{n.op}(" + ",".join(child_sigs) + ")"
    memo[nid] = sig
    return sig


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
            out[nid] = Node(nid=nid, op=n.op, deps=tuple(deps))
        else:
            out[nid] = n
    return Dag(nodes=out, output=d.output)


def node_signature(n: Node) -> Tuple[str, int]:
    # op + arity
    return (n.op, len(n.deps))


def edge_signature(
    parent: Node, child_id: str, child: Node
) -> Tuple[Tuple[str, int], Tuple[str, int]]:
    # typed-ish edge: (parent op/arity) -> (child op/arity)
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
    g0 = canonicalize(collapse_lookup_qty(parse_dag(gold_dag)))
    p0 = canonicalize(collapse_lookup_qty(parse_dag(pred_dag)))

    g_nodes = [node_signature(n) for n in g0.nodes.values()]
    p_nodes = [node_signature(n) for n in p0.nodes.values()]
    node_prf = multiset_prf(g_nodes, p_nodes)

    g_edges = []
    for nid, n in g0.nodes.items():
        for c in n.deps:
            cn = g0.nodes.get(c)
            if cn:
                g_edges.append(edge_signature(n, c, cn))

    p_edges = []
    for nid, n in p0.nodes.items():
        for c in n.deps:
            cn = p0.nodes.get(c)
            if cn:
                p_edges.append(edge_signature(n, c, cn))

    edge_prf = multiset_prf(g_edges, p_edges)

    # exact match: compare canonical output subtree signatures (strong, order-invariant)
    exact = False
    if g0.output and p0.output and g0.output in g0.nodes and p0.output in p0.nodes:
        g_sig = canonical_subtree_sig(g0, g0.output, {})
        p_sig = canonical_subtree_sig(p0, p0.output, {})
        exact = g_sig == p_sig

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
# Lookup grounding metrics (query-free, order-free)
# -----------------------------------------------------------------------------


def extract_pred_lookup_sigs(
    trace: Optional[list[dict]],
) -> List[Tuple[str, str, str, int]]:
    """
    For each TEXT_LOOKUP trace entry with model_fact:
      sig = (snippet_id, label, period_kind, period_value_int)
    """
    out: List[Tuple[str, str, str, int]] = []
    for t in trace or []:
        if t.get("op") != "TEXT_LOOKUP":
            continue
        mf = t.get("model_fact")
        if not isinstance(mf, dict):
            continue
        sid = mf.get("snippet_id")
        lab = mf.get("label")
        pk, pv = _period_kind_value(mf.get("period"))
        if not (
            isinstance(sid, str)
            and isinstance(lab, str)
            and isinstance(pk, str)
            and isinstance(pv, int)
        ):
            continue
        out.append((sid, lab, pk, pv))
    return out


def extract_gold_lookup_sigs(
    capsule: Dict[str, Any],
) -> List[Tuple[str, str, str, int]]:
    """
    Gold lookup sigs derived from capsule:
      - Use gold.lookup_map keys (node ids) to select which snippets are "intended".
      - Disambiguate same label/period across companies via snippet_id.
      - We map node-id -> extraction_id via lookup_map and then resolve to snippet_id by matching:
        extraction_id prefix is usually snippet_id-ish? Not reliable.
      - Better: use meta.snippet_ids/labels/periods arrays (aligned with extraction_ids list order).
    """
    gold = capsule.get("gold", {}) or {}
    lm = gold.get("lookup_map") or {}
    meta = capsule.get("meta", {}) or {}

    # meta fields are aligned lists per binding/extraction
    ex_ids = meta.get("extraction_ids") or []
    sids = meta.get("snippet_ids") or []
    labels = meta.get("labels") or []
    periods = meta.get("periods") or []

    # Build extraction_id -> (snippet_id,label,pk,pv)
    ex_map: Dict[str, Tuple[str, str, str, int]] = {}
    for ex_id, sid, lab, per in zip(ex_ids, sids, labels, periods):
        if not (
            isinstance(ex_id, str)
            and isinstance(sid, str)
            and isinstance(lab, str)
            and isinstance(per, dict)
        ):
            continue
        pk, pv = _period_kind_value(per)
        if isinstance(pk, str) and isinstance(pv, int):
            ex_map[ex_id] = (sid, lab, pk, pv)

    out: List[Tuple[str, str, str, int]] = []
    # lookup_map is node_id -> extraction_id
    for _nid, ex_id in lm.items():
        if isinstance(ex_id, str) and ex_id in ex_map:
            out.append(ex_map[ex_id])

    # Fallback: if lookup_map missing, approximate from meta lists
    if not out:
        for ex_id in ex_ids:
            if isinstance(ex_id, str) and ex_id in ex_map:
                out.append(ex_map[ex_id])

    return out


def lookup_grounding_metrics(
    capsule: Dict[str, Any], trace: Optional[list[dict]]
) -> Dict[str, Any]:
    gold_sigs = extract_gold_lookup_sigs(capsule)
    pred_sigs = extract_pred_lookup_sigs(trace)

    prf = multiset_prf(gold_sigs, pred_sigs)

    return {
        "lookup_prec": prf["prec"],
        "lookup_rec": prf["rec"],
        "lookup_f1": prf["f1"],
        "lookup_gold_n": int(prf["gold_n"]),
        "lookup_pred_n": int(prf["pred_n"]),
    }
