#!/usr/bin/env python3
"""Correlate TRACE answer accuracy with other model-level metrics."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


METRICS = (
    "execution_success",
    "fact_recall",
    "fact_precision",
    "graph_recall",
    "graph_precision",
)

METRIC_LABELS = {
    "execution_success": "Execution success",
    "fact_recall": "Fact recall",
    "fact_precision": "Fact precision",
    "graph_recall": "Graph recall",
    "graph_precision": "Graph precision",
}

SCOPE_LABELS = {
    "all": "All",
    "trace_ufr": "TRACE-UFR",
    "trace_dir": "TRACE-DIR",
}


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def _results_paths(exp_dir: Path) -> list[Path]:
    paths = sorted(exp_dir.glob("runs/*/*/results_all.jsonl"))
    return paths or [exp_dir / "results_all.jsonl"]


def _read_rows(exp_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _results_paths(exp_dir):
        if not path.exists():
            continue
        for row in _iter_jsonl(path):
            if row.get("mode") == "full":
                rows.append(row)
    if not rows:
        raise SystemExit(f"No full-mode result rows found under {exp_dir}")
    return rows


def _model(row: dict[str, Any]) -> str:
    return str(row.get("model_tag") or row.get("model") or "")


def _provider(row: dict[str, Any]) -> str:
    return str(row.get("provider") or row.get("planner") or "")


def _has_exec_error(row: dict[str, Any]) -> bool:
    return bool(row.get("exec_error_code") or row.get("exec_error"))


def _mean(values: Iterable[Any]) -> float | None:
    xs: list[float] = []
    for value in values:
        if value is None:
            continue
        try:
            x = float(value)
        except (TypeError, ValueError):
            continue
        if not math.isnan(x):
            xs.append(x)
    return sum(xs) / len(xs) if xs else None


def _aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row.get("benchmark_id")), _provider(row), _model(row))].append(row)

    out: list[dict[str, Any]] = []
    for (dataset, provider, model), group in sorted(grouped.items()):
        n = len(group)
        out.append(
            {
                "dataset": dataset,
                "provider": provider,
                "model": model,
                "n": n,
                "answer_accuracy": sum(bool(r.get("correct")) for r in group) / n,
                "execution_success": sum(not _has_exec_error(r) for r in group) / n,
                "fact_recall": _mean(r.get("fact_rec") for r in group),
                "fact_precision": _mean(r.get("fact_prec") for r in group),
                "graph_recall": _mean(r.get("dag_edge_rec") for r in group),
                "graph_precision": _mean(r.get("dag_edge_prec") for r in group),
            }
        )
    return out


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 2:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mx) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - my) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


def _ranks(values: list[float]) -> list[float]:
    order = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i + 1
        while j < len(order) and order[j][1] == order[i][1]:
            j += 1
        avg_rank = (i + 1 + j) / 2
        for k in range(i, j):
            ranks[order[k][0]] = avg_rank
        i = j
    return ranks


def _spearman(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 2:
        return None
    return _pearson(_ranks(xs), _ranks(ys))


def _fmt(value: float | None) -> str:
    return "" if value is None else f"{value:.3f}"


def _latex_escape(value: Any) -> str:
    text = str(value)
    return (
        text.replace("\\", r"\textbackslash{}")
        .replace("&", r"\&")
        .replace("%", r"\%")
        .replace("$", r"\$")
        .replace("#", r"\#")
        .replace("_", r"\_")
        .replace("{", r"\{")
        .replace("}", r"\}")
    )


def _metric_pairs(
    rows: list[dict[str, Any]], metric: str, *, dataset: str | None = None
) -> tuple[list[float], list[float]]:
    xs: list[float] = []
    ys: list[float] = []
    for row in rows:
        if dataset is not None and row["dataset"] != dataset:
            continue
        x = row.get("answer_accuracy")
        y = row.get(metric)
        if x is None or y is None:
            continue
        xs.append(float(x))
        ys.append(float(y))
    return xs, ys


def _instance_metric_pairs(
    rows: list[dict[str, Any]],
    metric: str,
    *,
    dataset: str | None = None,
    model: str | None = None,
) -> tuple[list[float], list[float]]:
    xs: list[float] = []
    ys: list[float] = []
    row_metric = {
        "execution_success": lambda r: 0.0 if _has_exec_error(r) else 1.0,
        "fact_recall": lambda r: r.get("fact_rec"),
        "fact_precision": lambda r: r.get("fact_prec"),
        "graph_recall": lambda r: r.get("dag_edge_rec"),
        "graph_precision": lambda r: r.get("dag_edge_prec"),
    }[metric]
    for row in rows:
        if dataset is not None and row.get("benchmark_id") != dataset:
            continue
        if model is not None and _model(row) != model:
            continue
        y = row_metric(row)
        if y is None:
            continue
        try:
            yf = float(y)
        except (TypeError, ValueError):
            continue
        if math.isnan(yf):
            continue
        xs.append(1.0 if bool(row.get("correct")) else 0.0)
        ys.append(yf)
    return xs, ys


def _accuracy_correlations(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    datasets = ["all", *sorted({str(row["dataset"]) for row in summary})]
    out: list[dict[str, Any]] = []
    for dataset in datasets:
        dataset_filter = None if dataset == "all" else dataset
        for metric in METRICS:
            xs, ys = _metric_pairs(summary, metric, dataset=dataset_filter)
            out.append(
                {
                    "scope": dataset,
                    "x": "answer_accuracy",
                    "y": metric,
                    "n": len(xs),
                    "pearson": _pearson(xs, ys),
                    "spearman": _spearman(xs, ys),
                }
            )
    return out


def _instance_accuracy_correlations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    datasets = ["all", *sorted({str(row.get("benchmark_id")) for row in rows})]
    out: list[dict[str, Any]] = []
    for dataset in datasets:
        dataset_filter = None if dataset == "all" else dataset
        for metric in METRICS:
            xs, ys = _instance_metric_pairs(rows, metric, dataset=dataset_filter)
            out.append(
                {
                    "scope": dataset,
                    "x": "answer_correct",
                    "y": metric,
                    "n": len(xs),
                    "pearson": _pearson(xs, ys),
                    "spearman": _spearman(xs, ys),
                    "mean_when_incorrect": _mean(y for x, y in zip(xs, ys) if x == 0.0),
                    "mean_when_correct": _mean(y for x, y in zip(xs, ys) if x == 1.0),
                }
            )

    models = sorted({_model(row) for row in rows})
    for dataset in sorted({str(row.get("benchmark_id")) for row in rows}):
        for model in models:
            if not any(row.get("benchmark_id") == dataset and _model(row) == model for row in rows):
                continue
            for metric in METRICS:
                xs, ys = _instance_metric_pairs(rows, metric, dataset=dataset, model=model)
                out.append(
                    {
                        "scope": f"{dataset}/{model}",
                        "x": "answer_correct",
                        "y": metric,
                        "n": len(xs),
                        "pearson": _pearson(xs, ys),
                        "spearman": _spearman(xs, ys),
                        "mean_when_incorrect": _mean(y for x, y in zip(xs, ys) if x == 0.0),
                        "mean_when_correct": _mean(y for x, y in zip(xs, ys) if x == 1.0),
                    }
                )
    return out


def _cross_domain_correlations(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    datasets = sorted({str(row["dataset"]) for row in summary})
    if len(datasets) != 2:
        return []

    by_model_dataset = {
        (str(row["model"]), str(row["dataset"])): row for row in summary
    }
    models = sorted(
        {
            model
            for model, dataset in by_model_dataset
            if all((model, d) in by_model_dataset for d in datasets)
        }
    )
    out: list[dict[str, Any]] = []
    for metric in ("answer_accuracy", *METRICS):
        xs: list[float] = []
        ys: list[float] = []
        for model in models:
            x = by_model_dataset[(model, datasets[0])].get(metric)
            y = by_model_dataset[(model, datasets[1])].get(metric)
            if x is None or y is None:
                continue
            xs.append(float(x))
            ys.append(float(y))
        out.append(
            {
                "scope": f"{datasets[0]}_vs_{datasets[1]}",
                "metric": metric,
                "n": len(xs),
                "pearson": _pearson(xs, ys),
                "spearman": _spearman(xs, ys),
            }
        )
    return out


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: _fmt(value) if isinstance(value, float) else value
                    for key, value in row.items()
                    if key in fieldnames
                }
            )


def _write_markdown(
    path: Path,
    summary: list[dict[str, Any]],
    accuracy_corrs: list[dict[str, Any]],
    instance_accuracy_corrs: list[dict[str, Any]],
    cross_domain_corrs: list[dict[str, Any]],
) -> None:
    lines: list[str] = []
    lines.append("# TRACE metric correlations")
    lines.append("")
    lines.append("## Model-domain summary")
    lines.append("")
    cols = ["dataset", "provider", "model", "n", "answer_accuracy", *METRICS]
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join("---" for _ in cols) + " |")
    for row in summary:
        lines.append(
            "| "
            + " | ".join(
                _fmt(row[col]) if isinstance(row.get(col), float) else str(row.get(col, ""))
                for col in cols
            )
            + " |"
        )

    lines.append("")
    lines.append("## Answer accuracy correlations")
    lines.append("")
    cols = ["scope", "x", "y", "n", "pearson", "spearman"]
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join("---" for _ in cols) + " |")
    for row in accuracy_corrs:
        lines.append(
            "| "
            + " | ".join(
                _fmt(row[col]) if isinstance(row.get(col), float) else str(row.get(col, ""))
                for col in cols
            )
            + " |"
        )

    lines.append("")
    lines.append("## Instance-level answer correctness correlations")
    lines.append("")
    lines.append("Pearson is a point-biserial correlation because answer correctness is binary at instance level.")
    lines.append("")
    cols = [
        "scope",
        "x",
        "y",
        "n",
        "pearson",
        "spearman",
        "mean_when_incorrect",
        "mean_when_correct",
    ]
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join("---" for _ in cols) + " |")
    for row in instance_accuracy_corrs:
        if "/" in str(row["scope"]):
            continue
        lines.append(
            "| "
            + " | ".join(
                _fmt(row[col]) if isinstance(row.get(col), float) else str(row.get(col, ""))
                for col in cols
            )
            + " |"
        )

    if cross_domain_corrs:
        lines.append("")
        lines.append("## Cross-domain consistency")
        lines.append("")
        cols = ["scope", "metric", "n", "pearson", "spearman"]
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("| " + " | ".join("---" for _ in cols) + " |")
        for row in cross_domain_corrs:
            lines.append(
                "| "
                + " | ".join(
                    _fmt(row[col]) if isinstance(row.get(col), float) else str(row.get(col, ""))
                    for col in cols
                )
                + " |"
            )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_instance_correlation_latex(
    path: Path, instance_accuracy_corrs: list[dict[str, Any]]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        row
        for row in instance_accuracy_corrs
        if "/" not in str(row["scope"])
    ]
    lines = [
        r"\begin{table}[t]",
        r"\centering",
        r"\footnotesize",
        r"\setlength{\tabcolsep}{5pt}",
        r"\begin{tabular}{llrrrrr}",
        r"\toprule",
        r"\textbf{Scope} & \textbf{Metric} & \textbf{$n$} & \textbf{Pearson} & \textbf{Spearman} & \textbf{Incorrect mean} & \textbf{Correct mean} \\",
        r"\midrule",
    ]
    last_scope = None
    for row in rows:
        scope = str(row["scope"])
        if last_scope is not None and scope != last_scope:
            lines.append(r"\addlinespace[2pt]")
        last_scope = scope
        lines.append(
            " & ".join(
                [
                    _latex_escape(SCOPE_LABELS.get(scope, scope)),
                    _latex_escape(METRIC_LABELS.get(str(row["y"]), str(row["y"]))),
                    str(row["n"]),
                    _fmt(row["pearson"]),
                    _fmt(row["spearman"]),
                    _fmt(row["mean_when_incorrect"]),
                    _fmt(row["mean_when_correct"]),
                ]
            )
            + r" \\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\caption{Instance-level correlations between binary answer correctness and execution, fact, and graph metrics. Pearson is a point-biserial correlation because answer correctness is binary.}",
            r"\label{tab:instance-answer-correctness-correlations}",
            r"\end{table}",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("experiment_dir", type=Path)
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args()

    exp_dir = args.experiment_dir
    out_dir = args.out_dir or exp_dir / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = _read_rows(exp_dir)
    summary = _aggregate(rows)
    accuracy_corrs = _accuracy_correlations(summary)
    instance_accuracy_corrs = _instance_accuracy_correlations(rows)
    cross_domain_corrs = _cross_domain_correlations(summary)

    _write_csv(
        out_dir / "model_domain_metrics.csv",
        summary,
        ["dataset", "provider", "model", "n", "answer_accuracy", *METRICS],
    )
    _write_csv(
        out_dir / "answer_accuracy_correlations.csv",
        accuracy_corrs,
        ["scope", "x", "y", "n", "pearson", "spearman"],
    )
    _write_csv(
        out_dir / "instance_answer_correctness_correlations.csv",
        instance_accuracy_corrs,
        [
            "scope",
            "x",
            "y",
            "n",
            "pearson",
            "spearman",
            "mean_when_incorrect",
            "mean_when_correct",
        ],
    )
    _write_csv(
        out_dir / "cross_domain_metric_correlations.csv",
        cross_domain_corrs,
        ["scope", "metric", "n", "pearson", "spearman"],
    )
    _write_markdown(
        out_dir / "metric_correlations.md",
        summary,
        accuracy_corrs,
        instance_accuracy_corrs,
        cross_domain_corrs,
    )
    _write_instance_correlation_latex(
        exp_dir / "tex_tables" / "instance_answer_correctness_correlations_table.tex",
        instance_accuracy_corrs,
    )
    (out_dir / "metric_correlations.json").write_text(
        json.dumps(
            {
                "model_domain_metrics": summary,
                "answer_accuracy_correlations": accuracy_corrs,
                "instance_answer_correctness_correlations": instance_accuracy_corrs,
                "cross_domain_metric_correlations": cross_domain_corrs,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"Read {len(rows)} rows from {len(_results_paths(exp_dir))} result file(s).")
    print(f"Wrote correlation analysis to {out_dir}")
    for row in accuracy_corrs:
        if row["scope"] == "all":
            print(
                f"answer_accuracy vs {row['y']}: "
                f"Pearson={_fmt(row['pearson'])}, Spearman={_fmt(row['spearman'])}, n={row['n']}"
            )
    print()
    for row in instance_accuracy_corrs:
        if row["scope"] == "all":
            print(
                f"answer_correct vs {row['y']} (instance): "
                f"Pearson={_fmt(row['pearson'])}, Spearman={_fmt(row['spearman'])}, n={row['n']}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
