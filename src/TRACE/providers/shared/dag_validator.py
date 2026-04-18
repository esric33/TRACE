from __future__ import annotations

from typing import Any, Iterable, Set

from TRACE.core.actions.builtin import build_registry_for_benchmark
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
        expected_keys = set(action.arg_keys)
        if set(args.keys()) != expected_keys:
            raise ValueError(f"{op} args must be exactly {expected_keys} in {node_id}")

        if op == "TEXT_LOOKUP":
            if not isinstance(args["query"], str) or not args["query"].strip():
                raise ValueError(
                    f"TEXT_LOOKUP args must be exactly {{query}} (non-empty) in {node_id}"
                )
        elif op == "GET_QUANTITY":
            if not _is_ref(args["fact"]):
                raise ValueError(
                    f"GET_QUANTITY args must be exactly {{fact}} with ref: in {node_id}"
                )
        elif op == "CONVERT_SCALE":
            if not _is_ref(args["q"]):
                raise ValueError(f"CONVERT_SCALE.q must be ref:<id> in {node_id}")
            if not isinstance(args["target_scale"], (int, float)):
                raise ValueError(f"CONVERT_SCALE.target_scale must be number in {node_id}")
        elif op == "FX_LOOKUP":
            if not isinstance(args["series_id"], str) or not args["series_id"].strip():
                raise ValueError(
                    f"FX_LOOKUP.series_id must be non-empty string in {node_id}"
                )
            if not isinstance(args["year"], (int, float)):
                raise ValueError(f"FX_LOOKUP.year must be a number in {node_id}")
        elif op == "CPI_LOOKUP":
            if not isinstance(args["series_id"], str) or not args["series_id"].strip():
                raise ValueError(
                    f"CPI_LOOKUP.series_id must be non-empty string in {node_id}"
                )
            if not isinstance(args["from_year"], (int, float)):
                raise ValueError(f"CPI_LOOKUP.from_year must be a number in {node_id}")
            if not isinstance(args["to_year"], (int, float)):
                raise ValueError(f"CPI_LOOKUP.to_year must be a number in {node_id}")
        elif op == "CONST":
            if not isinstance(args["value"], (int, float)):
                raise ValueError(
                    f"CONST args must be exactly {{value}} (number) in {node_id}"
                )
        elif op in {"ADD", "MUL", "DIV", "GT", "LT", "EQ", "AND", "OR"}:
            if not _is_ref(args["a"]) or not _is_ref(args["b"]):
                raise ValueError(f"{op}.a and {op}.b must be ref:<id> in {node_id}")

    if not _is_ref(dag["output"]):
        raise ValueError("dag.output must be a ref:<id> string")
    out_id = _ref_id(dag["output"])
    if out_id not in ids:
        raise ValueError(f"dag.output references unknown node id: {out_id}")

    seen: Set[str] = set()
    for node in nodes:
        node_id = node["id"]
        args = node["args"]
        for field, value in args.items():
            if not _is_ref(value):
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
