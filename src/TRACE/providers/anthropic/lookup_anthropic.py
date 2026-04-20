from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.core.executor.support import ExecError, ExecErrorCode, exec_error_data
from TRACE.providers.shared.structured_json import call_json_with_retries
from TRACE.providers.shared.prompt import build_lookup_prompt


def load_schema_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _ck(obj: Any) -> str:
    b = json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def anthropic_lookup_fn(
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
    # labels
    if benchmark_def is None:
        benchmark_def = load_benchmark("trace_ufr")
    try:
        allowed_labels = benchmark_def.load_allowed_labels(benchmark_def.schemas_dir)
    except Exception as e:
        raise ExecError(
            ExecErrorCode.BAD_SCHEMA,
            "benchmark label loading failed",
            exec_error_data(
                phase="lookup",
                provider="anthropic",
                benchmark_id=benchmark_def.benchmark_id,
                error=str(e),
            ),
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
            "op": "TEXT_LOOKUP",
            "provider": "anthropic",
            "qid": capsule.get("qid"),
            "query": query,
            "snips": context_ids,
            "model": model,
        }
    )

    if key in cache:
        return cache[key]

    # Provider call (prompt -> text)
    from TRACE.providers.anthropic._client import call_text

    def _call(p: str) -> str:
        return call_text(client, model=model, prompt=p, temperature=temperature)

    try:
        mf = call_json_with_retries(
            call_text=_call, prompt=prompt, json_schema=schema, max_retries=2
        )

    except Exception as e:
        raise ExecError(
            ExecErrorCode.LOOKUP_FAILED,
            "Anthropic lookup returned invalid JSON/schema",
            exec_error_data(
                phase="lookup",
                provider="anthropic",
                model=model,
                node_id=node_id,
                error=str(e),
            ),
        )

    cache[key] = mf
    if cache_path is not None:
        cache_path.write_text(
            json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    return mf
