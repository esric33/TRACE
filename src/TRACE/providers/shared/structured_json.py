from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional


@dataclass
class StructuredJSONError(RuntimeError):
    message: str
    last_text: str = ""
    last_text_clean: str = ""
    last_exception: str = ""

    def __str__(self) -> str:
        return self.message


def _strip_json_fences(s: str) -> str:
    s = (s or "").strip()

    if s.startswith("```"):
        lines = s.splitlines()
        if (
            len(lines) >= 2
            and lines[0].startswith("```")
            and lines[-1].startswith("```")
        ):
            s = "\n".join(lines[1:-1]).strip()

    i = s.find("{")
    j = s.rfind("}")
    if i != -1 and j != -1 and j > i:
        s = s[i : j + 1].strip()

    return s


def call_json_with_retries(
    *,
    call_text: Callable[[str], str],
    prompt: str,
    json_schema: Optional[Dict[str, Any]] = None,
    validate_obj: Optional[Callable[[Any], Any]] = None,
    max_retries: int = 2,
) -> Any:
    cur_prompt = prompt
    last_text = ""
    last_text_clean = ""
    last_exc = ""

    for _attempt in range(max_retries + 1):
        text = call_text(cur_prompt)
        last_text = text or ""
        text_clean = _strip_json_fences(last_text)
        last_text_clean = text_clean

        try:
            obj = json.loads(text_clean)

            if json_schema is not None:
                import jsonschema

                jsonschema.validate(instance=obj, schema=json_schema)

            if validate_obj is not None:
                obj = validate_obj(obj)

            return obj

        except Exception as e:
            last_exc = repr(e)
            cur_prompt = (
                "Your previous output was invalid.\n"
                "Return ONLY a single JSON object that matches the REQUIRED JSON SHAPE below.\n"
                "No markdown. No code fences. No commentary.\n\n"
                "REQUIRED JSON SHAPE:\n"
                '{ "snippet_id": "...", "label": "...", "period": {"period":"FY|Q|ASOF","value":...}, '
                '"quantity":{"value":...,"unit":"USD|EUR|JPY|TWD|GBP|KRW|RMB|CHF|percent|items|people","scale":...,"type":"money|rate|per_share|count"} }\n\n'
                "YOUR PREVIOUS OUTPUT WAS:\n"
                f"{last_text}\n"
            )

    raise StructuredJSONError(
        "Failed to produce valid JSON after retries",
        last_text=last_text,
        last_text_clean=last_text_clean,
        last_exception=last_exc,
    )
