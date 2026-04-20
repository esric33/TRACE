from __future__ import annotations

from pathlib import Path

from TRACE.core.actions.types import ActionExecContext
from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.generation.generation_types import ExtractRecord, load_snippets


def load_trace_ufr_benchmark():
    return load_benchmark("trace_ufr")


def load_trace_ufr_extracts() -> list[ExtractRecord]:
    benchmark_def = load_trace_ufr_benchmark()
    return benchmark_def.load_extracts(benchmark_def.extracts_dir)


def find_record(
    extracts: list[ExtractRecord],
    *,
    company: str,
    label: str,
    period_kind: str,
    period_value: object,
) -> ExtractRecord:
    for record in extracts:
        if (
            record.company == company
            and record.label == label
            and record.period_kind == period_kind
            and record.period_value == period_value
        ):
            return record
    raise LookupError(
        f"record not found for company={company!r} label={label!r} "
        f"period={period_kind} {period_value!r}"
    )


def make_action_ctx(*, cache: dict | None = None) -> ActionExecContext:
    benchmark_def = load_trace_ufr_benchmark()
    return ActionExecContext(
        benchmark_def=benchmark_def,
        capsule={"qid": "test-qid", "context": {"snippets": []}},
        extracts_by_snippet={},
        cache={} if cache is None else cache,
        lookup_fn=lambda *_args, **_kwargs: {},
    )


def make_capsule_from_snippet_ids(
    snippet_ids: list[str],
    *,
    qid: str = "test-qid",
    question: str = "test question",
) -> dict:
    benchmark_def = load_trace_ufr_benchmark()
    snippets_by_id = load_snippets(Path(benchmark_def.snippets_dir))
    snippets = []
    for snippet_id in snippet_ids:
        snippet = snippets_by_id[snippet_id]
        snippets.append(
            {
                "snippet_id": snippet["snippet_id"],
                "text": snippet["text"],
                **({"source": snippet["source"]} if "source" in snippet else {}),
            }
        )
    return {"qid": qid, "question": question, "context": {"snippets": snippets}}

