from __future__ import annotations

import json
from typing import Any, Dict

from TRACE.core.executor.support import ExecError, ExecErrorCode, exec_error_data
from TRACE.providers.shared.prompt import build_planner_prompt
from TRACE.providers.shared.dag_validator import validate_dag_obj


def openai_plan_fn(
    capsule: Dict[str, Any], *, client, model: str, benchmark_def=None
) -> Dict[str, Any]:
    prompt = build_planner_prompt(capsule, benchmark_def=benchmark_def)
    resp = client.responses.create(
        model=model,
        input=prompt,
        text={"format": {"type": "json_object"}},
        # temperature=0.0,
    )
    try:
        planner = json.loads(resp.output_text)
        dag = validate_dag_obj(planner, benchmark_def=benchmark_def)
        return dag
    except Exception as e:
        raise ExecError(
            ExecErrorCode.PLANNER_INVALID,
            "Planner output invalid",
            exec_error_data(
                phase="planner",
                provider="openai",
                model=model,
                error=str(e),
                output_text=resp.output_text,
            ),
        )
