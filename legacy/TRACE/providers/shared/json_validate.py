# reason_bench/providers/shared/json_validate.py
from __future__ import annotations
from typing import Any, Dict
import jsonschema


def validate_json_schema(obj: Any, schema: Dict[str, Any]) -> None:
    jsonschema.validate(instance=obj, schema=schema)
