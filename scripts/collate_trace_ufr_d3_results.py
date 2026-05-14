#!/usr/bin/env python3
"""Collate TRACE-UFR d=3 run results into a compact model table."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

try:
    from TRACE.reporting.dag_metrics import dag_struct_metrics
    from TRACE.reporting.evaluation import compare_outputs
except Exception:  # pragma: no cover - script still works with emitted metrics only.
    dag_struct_metrics = None
    compare_outputs = None


SUMMARY_COLUMNS = [
    "model",
    "provider",
    "mode",
    "n",
    "execution_success",
    "answer_accuracy",
    "fact_recall",
    "fact_precision",
    "graph_recall",
    "graph_precision",
    "graph_metric",
    "source_files",
]

ERROR_TYPES = ["TYPE", "UNIT", "SCALE", "VALUE", "PERIOD", "LOOKUP", "STRUCTURAL"]

LATEX_PROVIDER_ORDER = ["gemini", "anthropic", "openai"]
LATEX_MODEL_ORDER = {
    "gemini": ["gemini-2.5-flash", "gemini-3-flash-preview", "gemini-2.5-pro"],
    "anthropic": ["claude-sonnet-4-5", "claude-opus-4-6", "claude-haiku-4-5"],
    "openai": ["gpt-5.2", "gpt-5-mini", "gpt-5-nano"],
}
LATEX_PROVIDER_LABELS = {
    "gemini": "Gemini",
    "anthropic": "Anthropic",
    "openai": "OpenAI",
}
LATEX_MODEL_LABELS = {
    "gemini-2.5-flash": "Gemini-2.5-Flash",
    "gemini-3-flash-preview": "Gemini-3-Flash",
    "gemini-2.5-pro": "Gemini-2.5-Pro",
    "claude-sonnet-4-5": "Claude-Sonnet-4.5",
    "claude-opus-4-6": "Claude-Opus-4.6",
    "claude-haiku-4-5": "Claude-Haiku-4.5",
    "gpt-5.2": "GPT-5.2",
    "gpt-5-mini": "GPT-5-Mini",
    "gpt-5-nano": "GPT-5-Nano",
}


def _latex_model_ids() -> list[str]:
    return [
        model_id
        for provider in LATEX_PROVIDER_ORDER
        for model_id in LATEX_MODEL_ORDER[provider]
    ]


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{lineno}: invalid JSON: {exc}") from exc


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out):
        return None
    return out


def _mean(values: Iterable[float | None]) -> float | None:
    xs = [x for x in values if x is not None]
    return sum(xs) / len(xs) if xs else None


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def _boolish(value: Any) -> bool:
    return bool(value)


def _has_exec_error(row: dict[str, Any]) -> bool:
    return bool(row.get("exec_error_code") or row.get("exec_error"))


def _answer_correct(row: dict[str, Any]) -> bool:
    if _has_exec_error(row):
        return False
    if compare_outputs is None:
        return _boolish(row.get("correct"))
    return compare_outputs(row.get("output"), row.get("gold")).correct


def _answer_comparison(row: dict[str, Any]):
    if _has_exec_error(row) or compare_outputs is None:
        return None
    return compare_outputs(row.get("output"), row.get("gold"))


def _error_type(row: dict[str, Any]) -> str | None:
    code = row.get("exec_error_code")
    if code:
        if code == "E_type_mismatch":
            return "TYPE"
        if code == "E_unit_mismatch":
            return "UNIT"
        if code == "E_scale_mismatch":
            return "SCALE"
        if code == "E_period_mismatch":
            return "PERIOD"
        if code in {"E_fact_failed", "E_missing_table"}:
            return "LOOKUP"
        return "STRUCTURAL"

    comparison = _answer_comparison(row)
    if comparison is None or comparison.correct:
        return None

    mismatch = comparison.mismatch_kind
    if mismatch == "type_mismatch":
        return "TYPE"
    if mismatch == "unit_mismatch":
        return "UNIT"
    if mismatch == "scale_mismatch_only":
        return "SCALE"
    if mismatch in {"semantic_value_mismatch", "value_and_scale_mismatch", "value_mismatch"}:
        return "VALUE"
    return "STRUCTURAL"


def _derive_graph_metrics(row: dict[str, Any]) -> dict[str, Any]:
    """Recompute DAG metrics from stored gold/exec DAGs when possible."""
    # Planning-graph quality is only meaningful for full runs. Retrieval runs may
    # carry gold DAGs in extra metadata, but those are not model-planned graphs.
    if row.get("mode") != "full":
        return row
    if dag_struct_metrics is None:
        return row

    extra = row.get("extra")
    if not isinstance(extra, dict):
        return row
    gold_dag = extra.get("gold_dag")
    exec_dag = extra.get("exec_dag")
    if not isinstance(gold_dag, dict) or not isinstance(exec_dag, dict):
        return row

    out = dict(row)
    out.update(dag_struct_metrics(gold_dag, exec_dag))
    return out


def _derive_fact_metrics(row: dict[str, Any]) -> dict[str, Any]:
    gold_ids = row.get("fact_gold_extraction_ids")
    pred_ids = row.get("fact_pred_extraction_ids")

    if not isinstance(gold_ids, list):
        return row
    if not isinstance(pred_ids, list):
        fact_trace = row.get("fact_trace")
        if not isinstance(fact_trace, list):
            return row
        pred_ids = []
        for step in fact_trace:
            tag = step.get("resolve_tag") if isinstance(step, dict) else None
            ex_id = tag.get("extraction_id") if isinstance(tag, dict) else None
            pred_ids.append(ex_id if isinstance(ex_id, str) else None)

    gold = [ex_id for ex_id in gold_ids if isinstance(ex_id, str)]
    pred = [ex_id for ex_id in pred_ids if isinstance(ex_id, str)]
    inter = sum((Counter(gold) & Counter(pred)).values())
    gold_n = len(gold)
    pred_n = len(pred_ids)
    rec = inter / gold_n if gold_n else 0.0
    prec = inter / pred_n if pred_n else 0.0
    f1 = (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0

    out = dict(row)
    out.update(
        {
            "fact_rec": rec,
            "fact_prec": prec,
            "fact_f1": f1,
            "fact_gold_n": gold_n,
            "fact_pred_n": pred_n,
        }
    )
    return out


def _read_rows(paths: list[Path], distractor: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        for row in _iter_jsonl(path):
            if row.get("distractor") != distractor:
                continue
            row = _derive_graph_metrics(row)
            row = _derive_fact_metrics(row)
            row["source_file"] = str(path)
            rows.append(row)
    return rows


def _summarise(
    rows: list[dict[str, Any]], *, graph_metric: str, include_oracle: bool
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        mode = str(row.get("mode") or "")
        model = str(row.get("model_tag") or row.get("model") or mode or "unknown")
        if not include_oracle and (mode == "oracle" or model == "oracle"):
            continue
        provider = str(row.get("provider") or row.get("planner") or "")
        groups[(model, provider, mode)].append(row)

    precision_col = f"dag_{graph_metric}_prec"
    recall_col = f"dag_{graph_metric}_rec"

    table: list[dict[str, Any]] = []
    for (model, provider, mode), group_rows in sorted(groups.items()):
        n = len(group_rows)
        source_files = sorted({r["source_file"] for r in group_rows})
        table.append(
            {
                "model": model,
                "provider": provider,
                "mode": mode,
                "n": n,
                "execution_success": sum(not _has_exec_error(r) for r in group_rows) / n,
                "answer_accuracy": sum(_answer_correct(r) for r in group_rows) / n,
                "fact_recall": _mean(_as_float(r.get("fact_rec")) for r in group_rows),
                "fact_precision": _mean(_as_float(r.get("fact_prec")) for r in group_rows),
                "graph_recall": _mean(_as_float(r.get(recall_col)) for r in group_rows),
                "graph_precision": _mean(_as_float(r.get(precision_col)) for r in group_rows),
                "graph_metric": graph_metric,
                "source_files": ";".join(source_files),
            }
        )

    return sorted(
        table,
        key=lambda r: (
            -(r["answer_accuracy"] or 0),
            -(r["execution_success"] or 0),
            r["model"],
            r["mode"],
        ),
    )


def _write_csv(path: Path, table: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        for row in table:
            writer.writerow({col: _fmt(row.get(col)) for col in SUMMARY_COLUMNS})


def _write_markdown(path: Path, table: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        fh.write("| " + " | ".join(SUMMARY_COLUMNS) + " |\n")
        fh.write("| " + " | ".join("---" for _ in SUMMARY_COLUMNS) + " |\n")
        for row in table:
            fh.write("| " + " | ".join(_fmt(row.get(col)) for col in SUMMARY_COLUMNS) + " |\n")


def _latex_pct(value: Any) -> str:
    x = _as_float(value)
    return "" if x is None else f"{100 * x:.1f}"


def _latex_score(value: Any) -> str:
    x = _as_float(value)
    return "" if x is None else f"{x:.3f}"


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


def _latex_rows(table: list[dict[str, Any]]) -> list[str]:
    by_provider_model = {
        (str(row.get("provider")), str(row.get("model"))): row
        for row in table
        if row.get("mode") == "full"
    }

    out: list[str] = []
    for provider in LATEX_PROVIDER_ORDER:
        model_ids = [
            model_id
            for model_id in LATEX_MODEL_ORDER[provider]
            if (provider, model_id) in by_provider_model
        ]
        if not model_ids:
            continue
        for idx, model_id in enumerate(model_ids):
            row = by_provider_model[(provider, model_id)]
            provider_cell = (
                rf"\multirow{{{len(model_ids)}}}{{*}}{{{LATEX_PROVIDER_LABELS[provider]}}}"
                if idx == 0
                else ""
            )
            cells = [
                provider_cell,
                LATEX_MODEL_LABELS.get(model_id, _latex_escape(model_id)),
                _latex_pct(row.get("execution_success")),
                "",
                _latex_pct(row.get("answer_accuracy")),
                "",
                _latex_score(row.get("fact_recall")),
                "",
                _latex_score(row.get("fact_precision")),
                "",
                _latex_score(row.get("graph_recall")),
                "",
                _latex_score(row.get("graph_precision")),
                "",
            ]
            out.append(" & ".join(cells) + r" \\")
    return out


def _write_latex(path: Path, table: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = _latex_rows(table)
    body = "\n".join(rows)
    path.write_text(
        rf"""\begin{{table*}}[t]
\centering
\footnotesize
\setlength{{\tabcolsep}}{{4pt}}
\begin{{tabular}}{{llcccccccccccc}}
\toprule
 &  & \multicolumn{{2}}{{c}}{{Exec. (\%)}}
 & \multicolumn{{2}}{{c}}{{Acc. (\%)}}
 & \multicolumn{{2}}{{c}}{{Retrieval Recall}}
 & \multicolumn{{2}}{{c}}{{Retrieval Precision}}
 & \multicolumn{{2}}{{c}}{{Graph Recall}}
 & \multicolumn{{2}}{{c}}{{Graph Precision}} \\
\cmidrule(lr){{3-4}} \cmidrule(lr){{5-6}} \cmidrule(lr){{7-8}} \cmidrule(lr){{9-10}} \cmidrule(lr){{11-12}} \cmidrule(lr){{13-14}}
Provider & Model
& UFR & DIR
& UFR & DIR
& UFR & DIR
& UFR & DIR
& UFR & DIR
& UFR & DIR \\
\midrule
{body}
\bottomrule
\end{{tabular}}
\caption{{Performance across TRACE-UFR and TRACE-DIR. Metrics include execution success, answer accuracy, fact quality, and structural reasoning quality (graph recall and precision). TRACE-DIR results will be populated upon completion of evaluation.}}
\label{{tab:combined_results}}
\vspace{{-4mm}}
\end{{table*}}
""",
        encoding="utf-8",
    )


def _error_counts(rows: list[dict[str, Any]]) -> dict[str, Counter[str]]:
    counts: dict[str, Counter[str]] = {
        model_id: Counter() for model_id in _latex_model_ids()
    }
    for row in rows:
        if row.get("mode") != "full":
            continue
        model = str(row.get("model_tag") or row.get("model") or "")
        if model not in counts:
            continue
        err = _error_type(row)
        if err is not None:
            counts[model][err] += 1
    return counts


def _write_error_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    counts = _error_counts(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["error_type"]
    for model_id in _latex_model_ids():
        fieldnames.extend([f"{model_id}_ufr", f"{model_id}_dir"])
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for err in ERROR_TYPES:
            row: dict[str, Any] = {"error_type": err}
            for model_id in _latex_model_ids():
                row[f"{model_id}_ufr"] = counts[model_id][err]
                row[f"{model_id}_dir"] = ""
            writer.writerow(row)


def _write_error_latex(path: Path, rows: list[dict[str, Any]]) -> None:
    counts = _error_counts(rows)
    model_ids = _latex_model_ids()
    tabular_cols = "l" + "cc" * len(model_ids)

    group_header = " & ".join(
        rf"\multicolumn{{2}}{{c}}{{{LATEX_MODEL_LABELS.get(model_id, _latex_escape(model_id))}}}"
        for model_id in model_ids
    )
    cmidrules = " ".join(
        rf"\cmidrule(lr){{{2 + 2 * idx}-{3 + 2 * idx}}}"
        for idx in range(len(model_ids))
    )
    subheader = "Error Type " + "".join(" & UFR & DIR" for _ in model_ids) + r" \\"
    body_lines = []
    for err in ERROR_TYPES:
        cells = [err]
        for model_id in model_ids:
            cells.extend([str(counts[model_id][err]), ""])
        body_lines.append(" & ".join(cells) + r" \\")
    body = "\n".join(body_lines)

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        rf"""\begin{{table*}}[t]
\centering
\scriptsize
\setlength{{\tabcolsep}}{{2pt}}
\begin{{tabular}}{{{tabular_cols}}}
\toprule
 & {group_header} \\
{cmidrules}
{subheader}
\midrule
{body}
\bottomrule
\end{{tabular}}
\caption{{Error counts by model and domain for the d=3 setting. TRACE-DIR columns will be populated upon completion of evaluation.}}
\label{{tab:error_analysis}}
\vspace{{-4mm}}
\end{{table*}}
""",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runs-dir",
        type=Path,
        default=Path("outputs/runs"),
        help="Directory containing run subdirectories with results_all.jsonl.",
    )
    parser.add_argument(
        "--results",
        type=Path,
        nargs="*",
        default=None,
        help="Explicit results_all.jsonl files. Defaults to --runs-dir/*/results_all.jsonl.",
    )
    parser.add_argument("--distractor", type=int, default=3, help="Distractor depth to collate.")
    parser.add_argument(
        "--graph-metric",
        choices=("edge", "node"),
        default="edge",
        help="Which DAG metric family to expose as graph precision/recall.",
    )
    parser.add_argument(
        "--include-oracle",
        action="store_true",
        help="Include oracle rows. By default oracle rows are omitted from the model table.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("outputs/analysis/trace_ufr_d3_summary.csv"),
        help="Output CSV path.",
    )
    parser.add_argument(
        "--markdown",
        type=Path,
        default=Path("outputs/analysis/trace_ufr_d3_summary.md"),
        help="Output Markdown path.",
    )
    parser.add_argument(
        "--latex",
        type=Path,
        default=Path("outputs/analysis/trace_ufr_d3_combined_table.tex"),
        help="Output LaTeX table path.",
    )
    parser.add_argument(
        "--error-csv",
        type=Path,
        default=Path("outputs/analysis/trace_ufr_d3_error_counts.csv"),
        help="Output error count CSV path.",
    )
    parser.add_argument(
        "--error-latex",
        type=Path,
        default=Path("outputs/analysis/trace_ufr_d3_error_counts.tex"),
        help="Output error count LaTeX table path.",
    )
    args = parser.parse_args()

    paths = args.results
    if paths is None:
        paths = sorted(args.runs_dir.glob("*/results_all.jsonl"))
    paths = [p for p in paths if p.exists()]
    if not paths:
        raise SystemExit("No results_all.jsonl files found.")

    rows = _read_rows(paths, args.distractor)
    if not rows:
        raise SystemExit(f"No rows found with distractor == {args.distractor}.")

    table = _summarise(rows, graph_metric=args.graph_metric, include_oracle=args.include_oracle)
    _write_csv(args.csv, table)
    _write_markdown(args.markdown, table)
    _write_latex(args.latex, table)
    _write_error_csv(args.error_csv, rows)
    _write_error_latex(args.error_latex, rows)

    print(f"Read {len(rows)} d={args.distractor} rows from {len(paths)} result file(s).")
    print(f"Wrote {args.csv}")
    print(f"Wrote {args.markdown}")
    print(f"Wrote {args.latex}")
    print(f"Wrote {args.error_csv}")
    print(f"Wrote {args.error_latex}")
    print()
    for row in table:
        print(
            f"{row['model']} ({row['mode']}): "
            f"execution_success={_fmt(row['execution_success'])}, "
            f"answer_accuracy={_fmt(row['answer_accuracy'])}, "
            f"fact_recall={_fmt(row['fact_recall'])}, "
            f"fact_precision={_fmt(row['fact_precision'])}, "
            f"graph_recall={_fmt(row['graph_recall'])}, "
            f"graph_precision={_fmt(row['graph_precision'])}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
