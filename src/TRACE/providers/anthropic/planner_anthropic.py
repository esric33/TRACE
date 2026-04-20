from __future__ import annotations

from typing import Any, Dict

from TRACE.core.executor.support import ExecError, ExecErrorCode, exec_error_data
from TRACE.providers.shared.prompt import build_planner_prompt
from TRACE.providers.shared.dag_validator import validate_dag_obj
from TRACE.providers.shared.structured_json import call_json_with_retries


def anthropic_plan_fn(
    capsule: Dict[str, Any], *, client, model: str, benchmark_def=None
) -> Dict[str, Any]:
    prompt = build_planner_prompt(capsule, benchmark_def=benchmark_def)

    from TRACE.providers.anthropic._client import call_text

    def _call(p: str) -> str:
        return call_text(client, model=model, prompt=p, temperature=0.0)

    try:
        # validate_dag_obj both validates and returns the inner dag
        dag = call_json_with_retries(
            call_text=_call,
            prompt=prompt,
            validate_obj=lambda obj: validate_dag_obj(obj, benchmark_def=benchmark_def),
            max_retries=2,
        )
        return dag
    except Exception as e:
        raise ExecError(
            ExecErrorCode.PLANNER_INVALID,
            "Anthropic planner output invalid",
            exec_error_data(
                phase="planner", provider="anthropic", model=model, error=str(e)
            ),
        )
