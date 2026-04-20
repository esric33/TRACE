from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from TRACE.core.executor.support import ExecErrorCode, ExecPhase, exec_error
from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.providers.shared.prompt import build_lookup_prompt


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
    benchmark_def=None,
) -> Dict[str, Any]:
    """
    TEXT_LOOKUP backend using OpenAI. Ignores node_id (but accepts it for ABI compatibility).
    Returns a ModelFact dict.
    """
    # labels
    if benchmark_def is None:
        benchmark_def = load_benchmark("trace_ufr")
    try:
        allowed_labels = benchmark_def.load_allowed_labels(benchmark_def.schemas_dir)
    except Exception as e:
        raise exec_error(
            ExecErrorCode.BAD_SCHEMA,
            "benchmark label loading failed",
            phase=ExecPhase.LOOKUP,
            provider="openai",
            benchmark_id=benchmark_def.benchmark_id,
            error=str(e),
        )

    context_snippets = capsule["context"]["snippets"]
    context_ids = [s["snippet_id"] for s in context_snippets]
    snippet_text = "\n\n".join(
        f"{s['snippet_id']}: {s['text']}" for s in context_snippets
    )

    prompt = build_lookup_prompt(
        query,
        snippet_text,
        allowed_labels,
        benchmark_def=benchmark_def,
    )

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
        raise exec_error(
            ExecErrorCode.LOOKUP_FAILED,
            "OpenAI returned non-JSON output_text",
            phase=ExecPhase.LOOKUP,
            provider="openai",
            model=model,
            node_id=node_id,
            error=str(e),
            output_text=resp.output_text,
        )

    cache[key] = mf
    if cache_path is not None:
        cache_path.write_text(
            json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    return mf
