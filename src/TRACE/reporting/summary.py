from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean
from typing import Any

from TRACE.shared.io import read_jsonl

SLICE_FIELDS = (
    "benchmark_id",
    "family",
    "qkey",
    "template_id",
    "planner",
    "model",
    "distractor_policy",
    "provider",
    "mode",
    "model_tag",
    "distractor",
)

NUMERIC_SUMMARY_FIELDS = (
    "trace_nodes",
    "dag_node_f1",
    "dag_edge_f1",
    "fact_f1",
    "fact_under_extraction",
    "fact_over_extraction",
    "fact_unresolved",
    "anchored_fact_match_count",
    "intermediate_value_match_rate",
    "dag_missing_node_type_count",
    "dag_extra_node_type_count",
    "dag_missing_edge_count",
    "dag_extra_edge_count",
    "dag_wrong_dependency_edges",
)

RATE_FIELDS = (
    "correct",
    "dag_exact",
    "dag_wrong_output_op",
    "fact_exact",
    "anchored_graph_match",
)

COUNTER_FIELDS = (
    "ops",
    "dag_missing_node_types",
    "dag_extra_node_types",
    "fact_resolution_status_counts",
    "fact_error_tag_counts",
)


def _quantiles(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    ordered = sorted(values)

    def percentile(p: float) -> float:
        if len(ordered) == 1:
            return float(ordered[0])
        index = (len(ordered) - 1) * p
        lo = math.floor(index)
        hi = math.ceil(index)
        if lo == hi:
            return float(ordered[lo])
        frac = index - lo
        return (float(ordered[lo]) * (1.0 - frac)) + (float(ordered[hi]) * frac)

    return {
        "min": float(ordered[0]),
        "mean": float(fmean(ordered)),
        "p50": percentile(0.50),
        "p90": percentile(0.90),
        "max": float(ordered[-1]),
    }


def _numeric_values(rows: list[dict[str, Any]], field: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = row.get(field)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            values.append(float(value))
    return values


def _rate(rows: list[dict[str, Any]], field: str) -> float | None:
    values = [bool(row[field]) for row in rows if isinstance(row.get(field), bool)]
    if not values:
        return None
    return float(sum(1 for value in values if value) / len(values))


def _sum_counter_field(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        value = row.get(field)
        if not isinstance(value, dict):
            continue
        for key, count in value.items():
            if isinstance(key, str) and isinstance(count, int):
                counts[key] += count
    return {key: counts[key] for key in sorted(counts)}


def _failure_funnel(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    for row in rows:
        stage = row.get("failure_stage")
        if isinstance(stage, str):
            counts[stage] += 1
    total = len(rows)
    return {
        "counts": {key: counts[key] for key in sorted(counts)},
        "rates": {
            key: (counts[key] / total if total else 0.0) for key in sorted(counts)
        },
    }


def _top_failure_modes(rows: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str | None, str | None, str | None, str | None]] = Counter()
    for row in rows:
        if row.get("correct") is True:
            continue
        counts[
            (
                row.get("failure_stage") if isinstance(row.get("failure_stage"), str) else "unknown",
                row.get("exec_error_code") if isinstance(row.get("exec_error_code"), str) else None,
                row.get("exec_error_phase") if isinstance(row.get("exec_error_phase"), str) else None,
                row.get("exec_error_op") if isinstance(row.get("exec_error_op"), str) else None,
                row.get("mismatch_kind") if isinstance(row.get("mismatch_kind"), str) else None,
            )
        ] += 1

    modes: list[dict[str, Any]] = []
    for (stage, code, phase, op, mismatch_kind), count in counts.most_common(limit):
        modes.append(
            {
                "count": count,
                "failure_stage": stage,
                "exec_error_code": code,
                "exec_error_phase": phase,
                "exec_error_op": op,
                "mismatch_kind": mismatch_kind,
            }
        )
    return modes


def _slice_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    correct = sum(1 for row in rows if row.get("correct") is True)
    incorrect = total - correct
    return {
        "total_examples": total,
        "correct": correct,
        "incorrect": incorrect,
        "accuracy": (correct / total if total else 0.0),
        "failure_funnel": _failure_funnel(rows),
        "metrics": {
            field: _quantiles(_numeric_values(rows, field))
            for field in NUMERIC_SUMMARY_FIELDS
            if _numeric_values(rows, field)
        },
        "rates": {
            field: rate
            for field in RATE_FIELDS
            if (rate := _rate(rows, field)) is not None
        },
    }


def summarize_results_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_slice: dict[str, dict[str, list[dict[str, Any]]]] = {
        field: defaultdict(list) for field in SLICE_FIELDS
    }
    for row in rows:
        for field in SLICE_FIELDS:
            value = row.get(field)
            if value is None:
                continue
            by_slice[field][str(value)].append(row)

    overall = _slice_summary(rows)
    overall["counter_sums"] = {
        field: _sum_counter_field(rows, field)
        for field in COUNTER_FIELDS
        if _sum_counter_field(rows, field)
    }
    overall["error_breakdowns"] = {
        "exec_error_code": {
            key: count
            for key, count in sorted(
                Counter(
                    row["exec_error_code"]
                    for row in rows
                    if isinstance(row.get("exec_error_code"), str)
                ).items()
            )
        },
        "exec_error_phase": {
            key: count
            for key, count in sorted(
                Counter(
                    row["exec_error_phase"]
                    for row in rows
                    if isinstance(row.get("exec_error_phase"), str)
                ).items()
            )
        },
        "exec_error_op": {
            key: count
            for key, count in sorted(
                Counter(
                    row["exec_error_op"]
                    for row in rows
                    if isinstance(row.get("exec_error_op"), str)
                ).items()
            )
        },
        "failure_stage": {
            key: count
            for key, count in sorted(
                Counter(
                    row["failure_stage"]
                    for row in rows
                    if isinstance(row.get("failure_stage"), str)
                ).items()
            )
        },
    }
    overall["top_failure_modes"] = _top_failure_modes(rows)
    overall["diagnostics"] = {
        "dag_missing_node_types": _sum_counter_field(rows, "dag_missing_node_types"),
        "dag_extra_node_types": _sum_counter_field(rows, "dag_extra_node_types"),
        "fact_resolution_status_counts": _sum_counter_field(
            rows, "fact_resolution_status_counts"
        ),
        "fact_error_tag_counts": _sum_counter_field(rows, "fact_error_tag_counts"),
        "fact_under_extraction_total": int(
            sum(int(row.get("fact_under_extraction", 0) or 0) for row in rows)
        ),
        "fact_over_extraction_total": int(
            sum(int(row.get("fact_over_extraction", 0) or 0) for row in rows)
        ),
        "fact_unresolved_total": int(
            sum(int(row.get("fact_unresolved", 0) or 0) for row in rows)
        ),
        "dag_wrong_dependency_edges_total": int(
            sum(int(row.get("dag_wrong_dependency_edges", 0) or 0) for row in rows)
        ),
    }

    slices = {
        field: {
            value: _slice_summary(group_rows)
            for value, group_rows in sorted(groups.items())
        }
        for field, groups in by_slice.items()
        if groups
    }

    top_failing_slices: list[dict[str, Any]] = []
    for field, groups in by_slice.items():
        for value, group_rows in groups.items():
            summary = _slice_summary(group_rows)
            if summary["incorrect"] <= 0:
                continue
            top_failing_slices.append(
                {
                    "dimension": field,
                    "value": value,
                    "total_examples": summary["total_examples"],
                    "incorrect": summary["incorrect"],
                    "accuracy": summary["accuracy"],
                }
            )
    top_failing_slices.sort(
        key=lambda item: (-item["incorrect"], item["accuracy"], -item["total_examples"], item["dimension"], item["value"])
    )

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_examples": len(rows),
        "overall": overall,
        "slices": slices,
        "top_failing_slices": top_failing_slices[:15],
    }


def summarize_results_file(path: str | Path) -> dict[str, Any]:
    rows = list(read_jsonl(path))
    return summarize_results_rows(rows)


def _fmt_quantiles(stats: dict[str, float] | None) -> str:
    if not stats:
        return "-"
    return (
        f"{stats['mean']:.3f} mean"
        f" / {stats['p50']:.3f} p50"
        f" / {stats['p90']:.3f} p90"
    )


def render_summary_markdown(summary: dict[str, Any]) -> str:
    overall = summary["overall"]
    lines = [
        "# Run Summary",
        "",
        f"- Examples: `{summary['total_examples']}`",
        f"- Correct: `{overall['correct']}`",
        f"- Incorrect: `{overall['incorrect']}`",
        f"- Accuracy: `{overall['accuracy']:.3f}`",
        "",
        "## Failure Funnel",
        "",
        "| Stage | Count | Rate |",
        "| --- | ---: | ---: |",
    ]

    funnel = overall["failure_funnel"]
    for stage, count in funnel["counts"].items():
        lines.append(
            f"| `{stage}` | {count} | {funnel['rates'][stage]:.3f} |"
        )

    lines.extend(
        [
            "",
            "## Core Metrics",
            "",
            "| Metric | Summary |",
            "| --- | --- |",
        ]
    )
    for field, stats in overall["metrics"].items():
        lines.append(f"| `{field}` | {_fmt_quantiles(stats)} |")

    lines.extend(
        [
            "",
            "## Top Failure Modes",
            "",
            "| Count | Stage | Code | Phase | Op | Mismatch |",
            "| ---: | --- | --- | --- | --- | --- |",
        ]
    )
    for mode in overall["top_failure_modes"]:
        lines.append(
            "| "
            f"{mode['count']} | "
            f"`{mode['failure_stage']}` | "
            f"`{mode['exec_error_code'] or '-'}` | "
            f"`{mode['exec_error_phase'] or '-'}` | "
            f"`{mode['exec_error_op'] or '-'}` | "
            f"`{mode['mismatch_kind'] or '-'}` |"
        )

    lines.extend(
        [
            "",
            "## Top Failing Slices",
            "",
            "| Dimension | Value | Incorrect | Total | Accuracy |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )
    for item in summary["top_failing_slices"]:
        lines.append(
            "| "
            f"`{item['dimension']}` | "
            f"`{item['value']}` | "
            f"{item['incorrect']} | "
            f"{item['total_examples']} | "
            f"{item['accuracy']:.3f} |"
        )

    lines.extend(
        [
            "",
            "## Diagnostic Totals",
            "",
            "```json",
            json.dumps(overall["diagnostics"], indent=2, ensure_ascii=False),
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def write_run_summary_artifacts(
    results_path: str | Path,
    *,
    out_dir: str | Path | None = None,
) -> dict[str, Any]:
    results_path = Path(results_path)
    out_dir = Path(out_dir) if out_dir is not None else results_path.parent
    summary = summarize_results_file(results_path)
    summary["results_path"] = str(results_path.resolve())

    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (out_dir / "summary.md").write_text(
        render_summary_markdown(summary),
        encoding="utf-8",
    )
    return summary
