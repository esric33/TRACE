from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from TRACE.execute.executor import ExecError
from TRACE.providers.shared.structured_json import call_json_with_retries
from TRACE.providers.shared.prompt import build_lookup_prompt

LABEL_ENUM_PATH = Path("schemas") / "label_enum.json"


def load_schema_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _ck(obj: Any) -> str:
    b = json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def gemini_lookup_fn(
    node_id: str,
    query: str,
    capsule: Dict[str, Any],
    extracts_by_snippet: Dict[str, List[Dict[str, Any]]],
    *,
    client,
    model: str,
    schema: Dict[str, Any],
    cache: Dict[str, Any],
    cache_path: Optional[Path] = None,
    temperature: float = 0.0,
) -> Dict[str, Any]:
    allowed_labels = json.loads(LABEL_ENUM_PATH.read_text(encoding="utf-8"))
    if not isinstance(allowed_labels, list) or not all(
        isinstance(x, str) for x in allowed_labels
    ):
        raise ExecError(
            "E_bad_schema",
            "label_enum.json must be a list of strings",
            {"path": str(LABEL_ENUM_PATH)},
        )

    context_snippets = capsule["context"]["snippets"]
    context_ids = [s["snippet_id"] for s in context_snippets]
    snippet_text = "\n\n".join(
        f"{s['snippet_id']}: {s['text']}" for s in context_snippets
    )

    prompt = build_lookup_prompt(query, snippet_text, allowed_labels)

    key = _ck(
        {
            "op": "TEXT_LOOKUP",
            "provider": "gemini",
            "qid": capsule.get("qid"),
            "query": query,
            "snips": context_ids,
            "model": model,
        }
    )

    if key in cache:
        return cache[key]

    from TRACE.providers.gemini._client import call_text

    def _call(p: str) -> str:
        return call_text(client, model=model, prompt=p, temperature=temperature)

    try:
        mf = call_json_with_retries(
            call_text=_call, prompt=prompt, json_schema=schema, max_retries=2
        )
    except Exception as e:
        raise ExecError(
            "E_lookup_failed",
            "Gemini lookup returned invalid JSON/schema",
            {"err": str(e)},
        )

    cache[key] = mf
    if cache_path is not None:
        cache_path.write_text(
            json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    return mf
