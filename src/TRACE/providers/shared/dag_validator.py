from __future__ import annotations

from typing import Any, Iterable, Set

from TRACE.core.actions import build_registry_for_benchmark
from TRACE.core.benchmarks.loader import load_benchmark


def _is_ref(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("ref:")


def _ref_id(value: str) -> str:
    return value.split("ref:", 1)[1]


def _validation_registry(benchmark_def=None, allowed_actions: Iterable[str] | None = None):
    if benchmark_def is None:
        benchmark_def = load_benchmark("trace_ufr")
    registry = build_registry_for_benchmark(benchmark_def)
    ops = set(allowed_actions) if allowed_actions is not None else set(benchmark_def.allowed_actions)
    missing = ops - registry.allowed_ops()
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"validator missing registered actions: {missing_str}")
    return registry, ops


def validate_dag_obj(
    planner: dict,
    *,
    benchmark_def=None,
    allowed_actions: Iterable[str] | None = None,
) -> dict:
    registry, allowed_ops = _validation_registry(
        benchmark_def=benchmark_def, allowed_actions=allowed_actions
    )

    if not isinstance(planner, dict) or set(planner.keys()) != {"dag"}:
        raise ValueError('Top-level JSON must be exactly {"dag": ...}')

    dag = planner["dag"]
    if not isinstance(dag, dict) or set(dag.keys()) != {"nodes", "output"}:
        raise ValueError("dag must contain exactly keys: nodes, output")

    nodes = dag["nodes"]
    if not isinstance(nodes, list) or not nodes:
        raise ValueError("dag.nodes must be a non-empty list")

    ids: Set[str] = set()

    for node in nodes:
        if not isinstance(node, dict) or set(node.keys()) != {"id", "op", "args"}:
            raise ValueError("each node must have exactly keys: id, op, args")

        node_id = node["id"]
        op = node["op"]
        args = node["args"]

        if not isinstance(node_id, str) or not node_id:
            raise ValueError("node.id must be non-empty string")
        if node_id in ids:
            raise ValueError(f"duplicate node id: {node_id}")
        ids.add(node_id)

        if not isinstance(op, str) or op not in allowed_ops:
            raise ValueError(f"invalid op: {op}")
        if not isinstance(args, dict):
            raise ValueError(f"node.args must be object for {node_id}")

        action = registry.require(op)
        action.validate_args(args, node_id=node_id)

    if not _is_ref(dag["output"]):
        raise ValueError("dag.output must be a ref:<id> string")
    out_id = _ref_id(dag["output"])
    if out_id not in ids:
        raise ValueError(f"dag.output references unknown node id: {out_id}")

    seen: Set[str] = set()
    for node in nodes:
        node_id = node["id"]
        args = node["args"]
        action = registry.require(node["op"])
        for field, value in args.items():
            if field not in action.arg_keys:
                continue
            spec = next(spec for spec in action.arg_specs if spec.name == field)
            if spec.kind != "ref":
                continue
            ref_node = _ref_id(value)
            if ref_node not in ids:
                raise ValueError(f"{node_id}.{field} references unknown node id: {ref_node}")
            if ref_node not in seen:
                raise ValueError(
                    f"{node_id}.{field} references node {ref_node} that appears later (or same node). Refs must point to earlier nodes."
                )
        seen.add(node_id)

    if benchmark_def.validate_planner_dag is not None:
        benchmark_def.validate_planner_dag(dag)

    return dag
