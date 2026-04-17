from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from TRACE.execute.executor import ExecError
from TRACE.providers.shared.prompt import build_lookup_prompt


LABEL_ENUM_PATH = Path("schemas") / "label_enum.json"


def _ck(obj: Any) -> str:
    b = json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def openai_lookup_fn(
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
    """
    TEXT_LOOKUP backend using OpenAI. Ignores node_id (but accepts it for ABI compatibility).
    Returns a ModelFact dict.
    """
    # labels
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
            "provider": "openai",
            "op": "TEXT_LOOKUP",
            "qid": capsule.get("qid"),
            "query": query,
            "snips": context_ids,
            "model": model,
        }
    )

    if key in cache:
        return cache[key]

    resp = client.responses.create(
        model=model,
        input=prompt,
        text={
            "format": {
                "type": "json_schema",
                "name": "model_fact",
                "strict": True,
                "schema": schema,
            }
        },
        # temperature=temperature,
    )

    try:
        mf = json.loads(resp.output_text)
    except Exception as e:
        raise ExecError(
            "E_lookup_failed",
            "OpenAI returned non-JSON output_text",
            {"err": str(e), "output_text": resp.output_text},
        )

    cache[key] = mf
    if cache_path is not None:
        cache_path.write_text(
            json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    return mf
