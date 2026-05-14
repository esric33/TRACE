from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean
from typing import Any, Iterable


PROFILE_SCHEMA_VERSION = 2


def _iter_refs(value: Any) -> Iterable[str]:
    if isinstance(value, str) and value.startswith("ref:"):
        yield value.split("ref:", 1)[1]
        return
    if isinstance(value, dict):
        for inner in value.values():
            yield from _iter_refs(inner)
        return
    if isinstance(value, list):
        for inner in value:
            yield from _iter_refs(inner)


def _dag_depth_and_breadth(dag: dict[str, Any]) -> tuple[int, int]:
    nodes = dag.get("nodes", [])
    if not isinstance(nodes, list):
        return (0, 0)

    depth_by_id: dict[str, int] = {}
    width_by_depth: Counter[int] = Counter()
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = node.get("id")
        if not isinstance(node_id, str) or not node_id:
            continue

        args = node.get("args", {})
        parent_depth = 0
        if isinstance(args, dict):
            for parent_id in _iter_refs(args):
                parent_depth = max(parent_depth, depth_by_id.get(parent_id, 0))

        depth = parent_depth + 1
        depth_by_id[node_id] = depth
        width_by_depth[depth] += 1

    if not depth_by_id:
        return (0, 0)
    return (max(depth_by_id.values()), max(width_by_depth.values()))


def _histogram(values: Iterable[int]) -> dict[str, int]:
    counts: Counter[int] = Counter(values)
    return {str(key): counts[key] for key in sorted(counts)}


def _quantiles(values: list[int]) -> dict[str, float] | None:
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


def _safe_str(value: Any, default: str) -> str:
    if isinstance(value, str) and value:
        return value
    return default


def _capsule_metrics(capsule: dict[str, Any]) -> dict[str, Any]:
    meta = capsule.get("meta", {}) or {}
    gold = capsule.get("gold", {}) or {}
    dag = gold.get("dag", {}) or {}
    nodes = dag.get("nodes", [])
    if not isinstance(nodes, list):
        nodes = []

    ops = [
        node.get("op")
        for node in nodes
        if isinstance(node, dict) and isinstance(node.get("op"), str)
    ]
    dag_depth, dag_breadth = _dag_depth_and_breadth(dag)
    fact_map = gold.get("fact_map", {}) or {}
    snippets = capsule.get("context", {}).get("snippets", []) or []
    snippet_ids = [
        snippet.get("snippet_id")
        for snippet in snippets
        if isinstance(snippet, dict) and isinstance(snippet.get("snippet_id"), str)
    ] if isinstance(snippets, list) else []
    fact_binding_ids = [
        value
        for value in fact_map.values()
        if isinstance(value, str)
    ] if isinstance(fact_map, dict) else []

    return {
        "qid": capsule.get("qid"),
        "family": _safe_str(meta.get("family"), "unknown"),
        "template_id": _safe_str(meta.get("template_id"), "unknown"),
        "snippet_count": len(snippet_ids),
        "snippet_ids": snippet_ids,
        "fact_bindings": len(fact_binding_ids),
        "fact_binding_ids": fact_binding_ids,
        "dag_depth": dag_depth,
        "dag_breadth": dag_breadth,
        "action_count": len(ops),
        "action_diversity": len(set(ops)),
        "fact_count": len(fact_map) if isinstance(fact_map, dict) else 0,
        "ops": ops,
    }


def build_benchmark_profile(
    capsules: Iterable[dict[str, Any]],
    *,
    benchmark_id: str,
    corpus_id: str | None = None,
) -> dict[str, Any]:
    rows = [_capsule_metrics(capsule) for capsule in capsules]

    by_template: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    op_counts: Counter[str] = Counter()
    for row in rows:
        by_template[row["template_id"]].append(row)
        by_family[row["family"]].append(row)
        op_counts.update(row["ops"])

    def summarize(group_rows: list[dict[str, Any]]) -> dict[str, Any]:
        snippet_counts = [row["snippet_count"] for row in group_rows]
        fact_counts = [row["fact_count"] for row in group_rows]
        snippet_occurrences = sum(snippet_counts)
        fact_binding_occurrences = sum(fact_counts)
        unique_snippets = {
            snippet_id
            for row in group_rows
            for snippet_id in row["snippet_ids"]
        }
        unique_fact_bindings = {
            binding_id
            for row in group_rows
            for binding_id in row["fact_binding_ids"]
        }
        return {
            "queries": len(group_rows),
            "snippet_occurrences": snippet_occurrences,
            "fact_binding_occurrences": fact_binding_occurrences,
            "unique_snippets": len(unique_snippets),
            "unique_fact_bindings": len(unique_fact_bindings),
            "avg_snippets_per_query": (
                float(snippet_occurrences / len(group_rows)) if group_rows else 0.0
            ),
            "avg_fact_bindings_per_query": (
                float(fact_binding_occurrences / len(group_rows)) if group_rows else 0.0
            ),
            "dag_depth": _quantiles([row["dag_depth"] for row in group_rows]),
            "dag_breadth": _quantiles([row["dag_breadth"] for row in group_rows]),
            "action_count": _quantiles([row["action_count"] for row in group_rows]),
            "action_diversity": _quantiles(
                [row["action_diversity"] for row in group_rows]
            ),
            "snippet_count": _quantiles(snippet_counts),
            "fact_count": _quantiles(fact_counts),
        }

    overall_stats = summarize(rows)
    return {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "benchmark_id": benchmark_id,
        "corpus_id": corpus_id,
        "total_queries": len(rows),
        "total_templates": len(by_template),
        "total_families": len(by_family),
        "totals": {
            "snippet_occurrences": overall_stats["snippet_occurrences"],
            "fact_binding_occurrences": overall_stats["fact_binding_occurrences"],
            "unique_snippets": overall_stats["unique_snippets"],
            "unique_fact_bindings": overall_stats["unique_fact_bindings"],
            "avg_snippets_per_query": overall_stats["avg_snippets_per_query"],
            "avg_fact_bindings_per_query": overall_stats["avg_fact_bindings_per_query"],
        },
        "histograms": {
            "snippet_count": _histogram(row["snippet_count"] for row in rows),
            "dag_depth": _histogram(row["dag_depth"] for row in rows),
            "dag_breadth": _histogram(row["dag_breadth"] for row in rows),
            "action_count": _histogram(row["action_count"] for row in rows),
            "action_diversity": _histogram(
                row["action_diversity"] for row in rows
            ),
            "fact_count": _histogram(row["fact_count"] for row in rows),
        },
        "quantiles": {
            "snippet_count": _quantiles([row["snippet_count"] for row in rows]),
            "dag_depth": _quantiles([row["dag_depth"] for row in rows]),
            "dag_breadth": _quantiles([row["dag_breadth"] for row in rows]),
            "action_count": _quantiles([row["action_count"] for row in rows]),
            "action_diversity": _quantiles(
                [row["action_diversity"] for row in rows]
            ),
            "fact_count": _quantiles([row["fact_count"] for row in rows]),
        },
        "operator_counts": {
            key: op_counts[key] for key in sorted(op_counts)
        },
        "per_template": {
            key: summarize(by_template[key]) for key in sorted(by_template)
        },
        "per_family": {key: summarize(by_family[key]) for key in sorted(by_family)},
    }


def _fmt_quantile_cell(stats: dict[str, float] | None) -> str:
    if not stats:
        return "-"
    return (
        f"{stats['mean']:.2f} mean"
        f" / {stats['p50']:.2f} p50"
        f" / {stats['p90']:.2f} p90"
    )


def render_benchmark_profile_markdown(profile: dict[str, Any]) -> str:
    lines = [
        "# Benchmark Profile",
        "",
        f"- Benchmark: `{profile['benchmark_id']}`",
        f"- Corpus: `{profile.get('corpus_id') or 'unknown'}`",
        f"- Queries: `{profile['total_queries']}`",
        f"- Templates: `{profile['total_templates']}`",
        f"- Families: `{profile['total_families']}`",
        f"- Unique snippets: `{profile['totals']['unique_snippets']}`",
        f"- Unique fact bindings: `{profile['totals']['unique_fact_bindings']}`",
        f"- Average snippets/query: `{profile['totals']['avg_snippets_per_query']:.2f}`",
        f"- Average fact bindings/query: `{profile['totals']['avg_fact_bindings_per_query']:.2f}`",
        f"- Snippet occurrences: `{profile['totals']['snippet_occurrences']}`",
        f"- Fact binding occurrences: `{profile['totals']['fact_binding_occurrences']}`",
        "",
        "## Global Quantiles",
        "",
        "| Metric | Summary |",
        "| --- | --- |",
    ]

    for metric in (
        "snippet_count",
        "dag_depth",
        "dag_breadth",
        "action_count",
        "action_diversity",
        "fact_count",
    ):
        lines.append(
            f"| `{metric}` | {_fmt_quantile_cell(profile['quantiles'][metric])} |"
        )

    lines.extend(
        [
            "",
            "## Per Family",
            "",
            "| Family | Queries | Unique Snippets | Unique Facts | Avg Snippets/Query | Avg Facts/Query | Action Count | DAG Depth | DAG Breadth |",
            "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )

    for family, stats in profile["per_family"].items():
        lines.append(
            "| "
            f"`{family}` | {stats['queries']} | "
            f"{stats['unique_snippets']} | "
            f"{stats['unique_fact_bindings']} | "
            f"{stats['avg_snippets_per_query']:.2f} | "
            f"{stats['avg_fact_bindings_per_query']:.2f} | "
            f"{_fmt_quantile_cell(stats['action_count'])} | "
            f"{_fmt_quantile_cell(stats['dag_depth'])} | "
            f"{_fmt_quantile_cell(stats['dag_breadth'])} |"
        )

    lines.extend(
        [
            "",
            "## Per Template",
            "",
            "| Template | Queries | Unique Snippets | Unique Facts | Avg Snippets/Query | Avg Facts/Query | Action Count | DAG Depth |",
            "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )

    for template_id, stats in profile["per_template"].items():
        lines.append(
            "| "
            f"`{template_id}` | {stats['queries']} | "
            f"{stats['unique_snippets']} | "
            f"{stats['unique_fact_bindings']} | "
            f"{stats['avg_snippets_per_query']:.2f} | "
            f"{stats['avg_fact_bindings_per_query']:.2f} | "
            f"{_fmt_quantile_cell(stats['action_count'])} | "
            f"{_fmt_quantile_cell(stats['dag_depth'])} |"
        )

    lines.extend(
        [
            "",
            "## Histograms",
            "",
            "```json",
            json.dumps(profile["histograms"], indent=2, ensure_ascii=False),
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def write_benchmark_profile_artifacts(
    out_dir: str | Path,
    *,
    capsules: Iterable[dict[str, Any]],
    benchmark_id: str,
    corpus_id: str | None = None,
) -> dict[str, Any]:
    out_dir = Path(out_dir)
    profile = build_benchmark_profile(
        capsules,
        benchmark_id=benchmark_id,
        corpus_id=corpus_id,
    )
    (out_dir / "benchmark_profile.json").write_text(
        json.dumps(profile, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (out_dir / "benchmark_profile.md").write_text(
        render_benchmark_profile_markdown(profile),
        encoding="utf-8",
    )
    return profile
