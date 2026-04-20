from __future__ import annotations

from typing import Any, Dict

from TRACE.core.actions import build_registry_for_benchmark
from TRACE.core.actions.types import ActionExecContext
from TRACE.core.executor.oracle import OracleContext
from TRACE.core.executor.support import (
    ExecError,
    ExecErrorCode,
    exec_error_data,
    resolve_fact_for_tagging,
)


def execute_dag(
    dag: Dict[str, Any],
    benchmark_def,
    mode: str,
    provider_ctx,
    oracle_ctx: OracleContext | None = None,
    *,
    capsule: Dict[str, Any],
    cache: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    if cache is None:
        cache = {}

    nodes = dag.get("nodes", [])
    out_ref = dag.get("output")
    if not isinstance(nodes, list) or not out_ref:
        raise ExecError(
            ExecErrorCode.BAD_DAG,
            "dag must have nodes[] and output",
            exec_error_data(phase="runtime"),
        )

    extracts_by_snippet: dict[str, list[dict[str, Any]]] = {}
    if mode == "oracle":
        if oracle_ctx is None:
            raise ValueError("oracle_ctx is required for mode=oracle")

        extracts_by_snippet = oracle_ctx.extracts_by_snippet

        def lookup_fn(node_id, query, _capsule, _extracts_by_snippet):
            try:
                return oracle_ctx.lookup_records[node_id]
            except KeyError as exc:
                raise KeyError(
                    f"missing oracle lookup record for node {node_id}"
                ) from exc

    else:
        if provider_ctx is None or provider_ctx.lookup_fn is None:
            raise ValueError("provider_ctx.lookup_fn is required for non-oracle execution")
        lookup_fn = provider_ctx.lookup_fn
        extracts_by_snippet = provider_ctx.extracts_by_snippet

    registry = build_registry_for_benchmark(benchmark_def)
    env: Dict[str, Any] = {}
    trace: list[dict[str, Any]] = []
    context_ids = [s["snippet_id"] for s in capsule["context"]["snippets"]]

    ctx = ActionExecContext(
        benchmark_def=benchmark_def,
        capsule=capsule,
        extracts_by_snippet=extracts_by_snippet,
        cache=cache,
        lookup_fn=lookup_fn,
    )

    def resolve_ref(value: Any) -> Any:
        if isinstance(value, str) and value.startswith("ref:"):
            node_id = value.split("ref:", 1)[1]
            if node_id not in env:
                raise ExecError(
                    ExecErrorCode.BAD_REF,
                    f"Unknown ref {value}",
                    exec_error_data(phase="runtime", ref=value, node_id=node_id),
                )
            return env[node_id]
        return value

    for node in nodes:
        node_id = node.get("id")
        op = node.get("op")
        raw_args = node.get("args", {})

        if not node_id:
            raise ExecError(
                ExecErrorCode.BAD_NODE,
                "Node missing id",
                exec_error_data(phase="runtime", op=op),
            )
        if not isinstance(raw_args, dict):
            raise ExecError(
                ExecErrorCode.BAD_NODE,
                f"Node args must be object for {node_id}",
                exec_error_data(phase="runtime", node_id=node_id, op=op),
            )
        if op not in benchmark_def.allowed_actions:
            raise ExecError(
                ExecErrorCode.BAD_OP,
                f"Op not allowed for benchmark: {op}",
                exec_error_data(phase="runtime", node_id=node_id, op=op),
            )

        try:
            action = registry.require(op)
        except KeyError as exc:
            raise ExecError(
                ExecErrorCode.BAD_OP,
                f"Op not registered: {op}",
                exec_error_data(phase="runtime", node_id=node_id, op=op),
            ) from exc

        if set(raw_args) != set(action.arg_keys):
            raise ExecError(
                ExecErrorCode.BAD_ARGS,
                f"{op} args must be exactly {set(action.arg_keys)}",
                exec_error_data(
                    phase="runtime",
                    node_id=node_id,
                    op=op,
                    expected=sorted(action.arg_keys),
                    got=sorted(raw_args.keys()),
                ),
            )
        if action.executor is None:
            raise ExecError(
                ExecErrorCode.BAD_OP,
                f"Op has no executor: {op}",
                exec_error_data(phase="runtime", node_id=node_id, op=op),
            )

        resolved_args = {key: resolve_ref(value) for key, value in raw_args.items()}
        result = action.executor(ctx, node_id, resolved_args)
        env[node_id] = result

        if op == "TEXT_LOOKUP":
            trace.append(
                {
                    "node": node_id,
                    "op": op,
                    "query": raw_args["query"],
                    "model_fact": result,
                    "resolve_tag": resolve_fact_for_tagging(
                        result, context_ids, extracts_by_snippet
                    ),
                }
            )
        else:
            trace.append(
                {
                    "node": node_id,
                    "op": op,
                    "args": raw_args,
                    "result": result,
                }
            )

    output = resolve_ref(out_ref)
    return {"output": output, "trace": trace}
