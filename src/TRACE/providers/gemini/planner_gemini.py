from __future__ import annotations

from typing import Any, Dict

from TRACE.execute.executor import ExecError
from TRACE.providers.shared.prompt import build_planner_prompt
from TRACE.providers.shared.dag_validator import validate_dag_obj
from TRACE.providers.shared.structured_json import call_json_with_retries


def gemini_plan_fn(capsule: Dict[str, Any], *, client, model: str) -> Dict[str, Any]:
    prompt = build_planner_prompt(capsule)

    from TRACE.providers.gemini._client import call_text

    def _call(p: str) -> str:
        return call_text(client, model=model, prompt=p, temperature=0.0)

    try:
        dag = call_json_with_retries(
            call_text=_call, prompt=prompt, validate_obj=validate_dag_obj, max_retries=2
        )
        return dag
    except Exception as e:
        raise ExecError(
            "E_planner_invalid", "Gemini planner output invalid", {"error": str(e)}
        )
