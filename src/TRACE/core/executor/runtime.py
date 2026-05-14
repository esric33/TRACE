from __future__ import annotations

from typing import Any, Dict

from TRACE.core.actions import build_registry_for_benchmark
from TRACE.core.actions.types import ActionExecContext
from TRACE.core.executor.support import (
    ExecErrorCode,
    ExecPhase,
    exec_error,
)


def execute_dag(
    dag: Dict[str, Any],
    benchmark_def,
    mode: str | None = None,
    provider_ctx: Any | None = None,
    oracle_ctx: Any | None = None,
    *,
    capsule: Dict[str, Any],
    cache: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    if cache is None:
        cache = {}

    nodes = dag.get("nodes", [])
    out_ref = dag.get("output")
    if not isinstance(nodes, list) or not out_ref:
        raise exec_error(
            ExecErrorCode.BAD_DAG,
            "dag must have nodes[] and output",
            phase=ExecPhase.RUNTIME,
        )

    registry = build_registry_for_benchmark(benchmark_def)
    env: Dict[str, Any] = {}
    trace: list[dict[str, Any]] = []

    ctx = ActionExecContext(
        benchmark_def=benchmark_def,
        capsule=capsule,
        extracts_by_snippet={},
        cache=cache,
    )

    def resolve_ref(value: Any) -> Any:
        if isinstance(value, str) and value.startswith("ref:"):
            node_id = value.split("ref:", 1)[1]
            if node_id not in env:
                raise exec_error(
                    ExecErrorCode.BAD_REF,
                    f"Unknown ref {value}",
                    phase=ExecPhase.RUNTIME,
                    ref=value,
                    node_id=node_id,
                )
            return env[node_id]
        if isinstance(value, list):
            return [resolve_ref(item) for item in value]
        if isinstance(value, dict):
            return {key: resolve_ref(item) for key, item in value.items()}
        return value

    for node in nodes:
        node_id = node.get("id")
        op = node.get("op")
        raw_args = node.get("args", {})

        if not node_id:
            raise exec_error(
                ExecErrorCode.BAD_NODE,
                "Node missing id",
                phase=ExecPhase.RUNTIME,
                op=op,
            )
        if not isinstance(raw_args, dict):
            raise exec_error(
                ExecErrorCode.BAD_NODE,
                f"Node args must be object for {node_id}",
                phase=ExecPhase.RUNTIME,
                node_id=node_id,
                op=op,
            )
        if op not in benchmark_def.allowed_actions:
            raise exec_error(
                ExecErrorCode.BAD_OP,
                f"Op not allowed for benchmark: {op}",
                phase=ExecPhase.RUNTIME,
                node_id=node_id,
                op=op,
            )

        try:
            action = registry.require(op)
        except KeyError as exc:
            raise exec_error(
                ExecErrorCode.BAD_OP,
                f"Op not registered: {op}",
                phase=ExecPhase.RUNTIME,
                node_id=node_id,
                op=op,
            ) from exc

        if set(raw_args) != set(action.arg_keys):
            raise exec_error(
                ExecErrorCode.BAD_ARGS,
                f"{op} args must be exactly {set(action.arg_keys)}",
                phase=ExecPhase.RUNTIME,
                node_id=node_id,
                op=op,
                expected=sorted(action.arg_keys),
                got=sorted(raw_args.keys()),
            )
        if action.executor is None:
            raise exec_error(
                ExecErrorCode.BAD_OP,
                f"Op has no executor: {op}",
                phase=ExecPhase.RUNTIME,
                node_id=node_id,
                op=op,
            )

        resolved_args = {key: resolve_ref(value) for key, value in raw_args.items()}
        result = action.executor(ctx, node_id, resolved_args)
        try:
            action.validate_output(result, args=resolved_args, node_id=node_id)
        except ValueError as exc:
            raise exec_error(
                ExecErrorCode.BAD_OUTPUT,
                str(exc),
                phase=ExecPhase.RUNTIME,
                node_id=node_id,
                op=op,
                expected=action.output_spec.prompt_repr(),
                got=result,
            ) from exc
        env[node_id] = result

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
