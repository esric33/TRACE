from __future__ import annotations

import json
from typing import Any, Dict

from TRACE.execute.executor import ExecError
from TRACE.providers.shared.prompt import build_planner_prompt
from TRACE.providers.shared.dag_validator import validate_dag_obj


def openai_plan_fn(capsule: Dict[str, Any], *, client, model: str) -> Dict[str, Any]:
    prompt = build_planner_prompt(capsule)
    resp = client.responses.create(
        model=model,
        input=prompt,
        text={"format": {"type": "json_object"}},
        # temperature=0.0,
    )
    try:
        planner = json.loads(resp.output_text)
        dag = validate_dag_obj(planner)
        return dag
    except Exception as e:
        raise ExecError(
            "E_planner_invalid",
            "Planner output invalid",
            {"error": str(e), "output_text": resp.output_text},
        )
