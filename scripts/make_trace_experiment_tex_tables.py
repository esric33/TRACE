#!/usr/bin/env python3
"""Generate NeurIPS-style LaTeX table fragments for a TRACE experiment."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


BENCHMARKS = ("trace_ufr", "trace_dir")
BENCH_LABELS = {"trace_ufr": "UFR", "trace_dir": "DIR"}

PROVIDER_ORDER = ("gemini", "anthropic", "openai")
MODEL_ORDER = {
    "gemini": ("gemini-2.5-flash", "gemini-3-flash-preview", "gemini-2.5-pro"),
    "anthropic": ("claude-sonnet-4-5", "claude-opus-4-6", "claude-haiku-4-5"),
    "openai": ("gpt-5.2", "gpt-5-mini", "gpt-5-nano"),
}
PROVIDER_LABELS = {"gemini": "Gemini", "anthropic": "Anthropic", "openai": "OpenAI"}
MODEL_LABELS = {
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

ERROR_GROUPS = ("TYPE", "UNIT", "SCALE", "VALUE", "PERIOD", "LOOKUP", "STRUCTURAL")
DETAIL_ERROR_ORDER = (
    "E_planner_invalid",
    "E_bad_schema",
    "E_bad_dag",
    "E_bad_ref",
    "E_bad_node",
    "E_bad_op",
    "E_bad_output",
    "E_bad_args",
    "E_type_mismatch",
    "E_unit_mismatch",
    "E_scale_mismatch",
    "E_missing_period",
    "E_period_mismatch",
    "E_div_zero",
    "E_bad_rate",
    "E_fact_failed",
    "E_missing_table",
    "E_bad_table",
    "E_missing_table_key",
    "M_type_mismatch",
    "M_unit_mismatch",
    "M_scale_mismatch_only",
    "M_value_mismatch",
    "M_value_mismatch_only",
    "M_semantic_value_mismatch",
    "M_value_and_scale_mismatch",
    "M_representation_mismatch",
    "M_wrong_answer",
)
ERROR_DESCRIPTIONS = {
    "TYPE": ("Semantic type", "Operation combines incompatible semantic types, e.g. money with a percentage or relation set with a scalar."),
    "UNIT": ("Unit", "Operation combines incompatible units, e.g. currencies without an FX conversion."),
    "SCALE": ("Scale", "Operation combines values at incompatible magnitudes, e.g. millions and billions without rescaling."),
    "VALUE": ("Answer value", "Trace executes but the final value, Boolean, or relation-set contents do not match the gold output."),
    "PERIOD": ("Temporal", "Operation uses quantities from incompatible periods or lacks the period needed for a temporal transform."),
    "LOOKUP": ("Lookup/reference", "A required fact, FX/CPI table, or table key cannot be resolved."),
    "STRUCTURAL": ("Structure/schema", "Malformed JSON/DAG, invalid references or operators, bad action arguments, or other executor contract failures."),
}

DETAIL_ERROR_DESCRIPTIONS = {
    "E_bad_args": r"\texttt{DIV} receives a string rather than a number.",
    "E_bad_schema": r"Response missing required \texttt{dag} object.",
    "E_bad_dag": r"DAG missing \texttt{nodes} or \texttt{output}.",
    "E_bad_ref": r"Node references unknown \texttt{ref:<id>}.",
    "E_bad_node": r"Node missing \texttt{id}, \texttt{op}, or \texttt{args}.",
    "E_bad_op": r"Unregistered operation (e.g., \texttt{FOO}).",
    "E_bad_output": r"Output is not a resolvable \texttt{ref:<id>}.",
    "E_lookup_failed": r"\texttt{TEXT\_LOOKUP} returns no usable result.",
    "E_planner_invalid": "Planner outputs non-JSON instead of a DAG.",
    "E_fact_failed": r"\texttt{MODEL\_FACT} does not match a capsule fact.",
    "E_type_mismatch": r"\texttt{ADD} combines money with a percentage.",
    "E_unit_mismatch": r"\texttt{ADD} combines currencies without conversion.",
    "E_scale_mismatch": r"\texttt{ADD} combines values without rescaling.",
    "E_missing_period": "Quantity lacks fiscal year for CPI adjustment.",
    "E_period_mismatch": "Comparison mixes FY2023 and FY2024.",
    "E_div_zero": r"\texttt{DIV} uses denominator \texttt{0}.",
    "E_bad_rate": "FX/CPI rate is missing or non-numeric.",
    "E_missing_table": "Requested FX table file does not exist.",
    "E_bad_table": "Table metadata inconsistent (e.g., wrong id).",
    "E_missing_table_key": "Table lacks entry for requested fiscal year.",
    "M_type_mismatch": "Final answer has the wrong semantic type.",
    "M_unit_mismatch": "Final answer uses the wrong unit.",
    "M_scale_mismatch_only": "Final value is right but reported at the wrong scale.",
    "M_value_mismatch": "Final Boolean, value, or set differs from gold.",
    "M_value_mismatch_only": "Final type, unit, and scale match but value differs.",
    "M_semantic_value_mismatch": "Final base value differs from gold.",
    "M_value_and_scale_mismatch": "Final value and scale both differ from gold.",
    "M_representation_mismatch": "Final answer has an unexpected representation.",
    "M_wrong_answer": "Executed trace returns an incorrect answer.",
}

DETAIL_ERROR_GROUPS = (
    ("Input / Contract", ("E_bad_args", "E_bad_schema")),
    ("DAG Structure", ("E_bad_dag", "E_bad_ref", "E_bad_node", "E_bad_op", "E_bad_output")),
    ("Provider / Planning", ("E_lookup_failed", "E_planner_invalid", "E_fact_failed")),
    ("Semantic Type Checking", ("E_type_mismatch", "E_unit_mismatch", "E_scale_mismatch")),
    ("Temporal / Period Logic", ("E_missing_period", "E_period_mismatch")),
    ("Numeric / Conversion", ("E_div_zero", "E_bad_rate")),
    ("Table / Reference Data", ("E_missing_table", "E_bad_table", "E_missing_table_key")),
    (
        "Executed Output Mismatch",
        (
            "M_type_mismatch",
            "M_unit_mismatch",
            "M_scale_mismatch_only",
            "M_value_mismatch",
            "M_value_mismatch_only",
            "M_semantic_value_mismatch",
            "M_value_and_scale_mismatch",
            "M_representation_mismatch",
            "M_wrong_answer",
        ),
    ),
)

ACTION_ROWS = [
    ("Retrieval/Extraction", "MODEL\\_FACT", "Assert one directly stated fact from a snippet.", "snippet, label, value spec", "quantity or singleton relation set", True, True),
    ("Set Construction", "MAKE\\_SET", "Build a relation set from compatible singleton relation facts.", "relation-set refs", "relation set", False, True),
    ("Transforms", "CONVERT\\_SCALE", "Rescale a quantity to a target numeric magnitude.", "quantity, target scale", "quantity", True, False),
    ("Transforms", "FX\\_LOOKUP", "Retrieve an exchange rate for a currency pair and fiscal year.", "currency pair, year", "rate", True, False),
    ("Transforms", "CPI\\_LOOKUP", "Retrieve an inflation adjustment factor between fiscal years.", "from year, to year", "rate", True, False),
    ("Computation", "CONST", "Create a scalar constant.", "number", "scalar", True, True),
    ("Computation", "ADD", "Add matching quantities with type, unit, and scale checks.", "quantity, quantity", "quantity", True, False),
    ("Computation", "MUL", "Multiply scalars, scaled quantities, FX rates, or CPI rates under TRACE-UFR semantics.", "quantity, quantity", "quantity or scalar", True, False),
    ("Computation", "DIV", "Divide by a scalar or compute a scalar ratio of matching quantities.", "quantity, quantity", "quantity or scalar", True, False),
    ("Computation", "GT / LT / EQ", "Compare compatible quantities or scalar set sizes.", "value, value", "Boolean", True, True),
    ("Set Operations", "SET\\_UNION", "Union compatible relation sets by normalized object value.", "relation set, relation set", "relation set", False, True),
    ("Set Operations", "SET\\_INTERSECT", "Intersect compatible relation sets by normalized object value.", "relation set, relation set", "relation set", False, True),
    ("Set Operations", "SET\\_DIFF", "Subtract one compatible relation set from another by normalized object value.", "relation set, relation set", "relation set", False, True),
    ("Set Operations", "SET\\_SIZE", "Return the number of items in a relation set.", "relation set", "scalar", False, True),
    ("Set Operations", "SET\\_CONTAINS", "Test whether a relation set contains a singleton relation by normalized object value.", "relation set, singleton", "Boolean", False, True),
]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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


def _pct(value: float | None) -> str:
    return "" if value is None else f"{100 * value:.1f}"


def _score(value: float | None) -> str:
    return "" if value is None else f"{value:.3f}"


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


def _has_exec_error(row: dict[str, Any]) -> bool:
    return bool(row.get("exec_error_code") or row.get("exec_error"))


def _model_id(row: dict[str, Any]) -> str:
    return str(row.get("model_tag") or row.get("model") or "")


def _provider(row: dict[str, Any]) -> str:
    return str(row.get("provider") or row.get("planner") or "")


def _error_group(row: dict[str, Any]) -> str | None:
    code = row.get("exec_error_code")
    if code:
        if code == "E_type_mismatch":
            return "TYPE"
        if code == "E_unit_mismatch":
            return "UNIT"
        if code == "E_scale_mismatch":
            return "SCALE"
        if code in {"E_missing_period", "E_period_mismatch"}:
            return "PERIOD"
        if code in {"E_fact_failed", "E_missing_table", "E_missing_table_key", "E_bad_table", "E_lookup_failed"}:
            return "LOOKUP"
        return "STRUCTURAL"

    if row.get("correct") is not False:
        return None
    mismatch = row.get("mismatch_kind")
    if mismatch == "type_mismatch":
        return "TYPE"
    if mismatch == "unit_mismatch":
        return "UNIT"
    if mismatch == "scale_mismatch_only":
        return "SCALE"
    if mismatch in {"value_mismatch", "value_mismatch_only", "semantic_value_mismatch", "value_and_scale_mismatch"}:
        return "VALUE"
    if mismatch:
        return "STRUCTURAL"
    return "VALUE"


def _profile(exp_dir: Path, benchmark: str) -> dict[str, Any]:
    return _read_json(exp_dir / "corpora" / benchmark / "benchmark_profile.json")


def _q(stats: dict[str, Any], key: str) -> str:
    q = stats["quantiles"][key]
    return f"{q['mean']:.2f} / {q['p50']:.0f} / {q['p90']:.0f}"


def _write_dataset_summary(exp_dir: Path, out_dir: Path) -> None:
    profiles = {b: _profile(exp_dir, b) for b in BENCHMARKS}
    rows = [
        ("Queries", f"{profiles['trace_ufr']['total_queries']}", f"{profiles['trace_dir']['total_queries']}"),
        ("Templates", f"{profiles['trace_ufr']['total_templates']}", f"{profiles['trace_dir']['total_templates']}"),
        ("Families", f"{profiles['trace_ufr']['total_families']}", f"{profiles['trace_dir']['total_families']}"),
        ("\\addlinespace[2pt]", "", ""),
        ("Total snippets", f"{profiles['trace_ufr']['totals']['unique_snippets']}", f"{profiles['trace_dir']['totals']['unique_snippets']}"),
        ("Lookup bindings", f"{profiles['trace_ufr']['totals']['unique_fact_bindings']}", f"{profiles['trace_dir']['totals']['unique_fact_bindings']}"),
        ("\\addlinespace[2pt]", "", ""),
        ("Action count (mean / p50 / p90)", _q(profiles["trace_ufr"], "action_count"), _q(profiles["trace_dir"], "action_count")),
        ("DAG depth (mean / p50 / p90)", _q(profiles["trace_ufr"], "dag_depth"), _q(profiles["trace_dir"], "dag_depth")),
        ("DAG breadth (mean / p50 / p90)", _q(profiles["trace_ufr"], "dag_breadth"), _q(profiles["trace_dir"], "dag_breadth")),
        ("Lookup count (mean / p50 / p90)", _q(profiles["trace_ufr"], "fact_count"), _q(profiles["trace_dir"], "fact_count")),
    ]
    body = "\n".join(
        row[0] if row[0].startswith("\\") else f"{row[0]} & {row[1]} & {row[2]} \\\\"
        for row in rows
    )
    (out_dir / "dataset_summary_table.tex").write_text(
        rf"""\begin{{wraptable}}{{r}}{{0.60\linewidth}}
\centering
\footnotesize
\setlength{{\tabcolsep}}{{6pt}}
\vspace{{-3mm}}
\begin{{tabular}}{{lcc}}
\toprule
\textbf{{Statistic}} & \textbf{{TRACE-UFR}} & \textbf{{TRACE-DIR}} \\
\midrule
{body}
\bottomrule
\end{{tabular}}
\caption{{TRACE-UFR and TRACE-DIR statistics.}}
\label{{tab:domain_summary}}
\vspace{{-1mm}}
\end{{wraptable}}
""",
        encoding="utf-8",
    )


def _summaries(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, float | None]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row.get("benchmark_id")), _model_id(row))].append(row)
    out: dict[tuple[str, str], dict[str, float | None]] = {}
    for key, group in grouped.items():
        n = len(group)
        out[key] = {
            "n": float(n),
            "exec": sum(not _has_exec_error(r) for r in group) / n,
            "acc": sum(bool(r.get("correct")) for r in group) / n,
            "fact_rec": _mean(r.get("fact_rec") for r in group),
            "fact_prec": _mean(r.get("fact_prec") for r in group),
            "graph_rec": _mean(r.get("dag_edge_rec") for r in group),
            "graph_prec": _mean(r.get("dag_edge_prec") for r in group),
        }
    return out


def _present_models(rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    present = {(_provider(r), _model_id(r)) for r in rows}
    return {
        provider: [model for model in MODEL_ORDER[provider] if (provider, model) in present]
        for provider in PROVIDER_ORDER
    }


def _write_performance(rows: list[dict[str, Any]], out_dir: Path) -> None:
    summaries = _summaries(rows)
    present = _present_models(rows)
    body_lines: list[str] = []
    for provider in PROVIDER_ORDER:
        models = present[provider]
        if not models:
            continue
        for idx, model in enumerate(models):
            provider_cell = rf"\multirow{{{len(models)}}}{{*}}{{{PROVIDER_LABELS[provider]}}}" if idx == 0 else ""
            cells = [provider_cell, MODEL_LABELS[model]]
            for metric in ("exec", "acc", "fact_rec", "fact_prec", "graph_rec", "graph_prec"):
                for benchmark in BENCHMARKS:
                    value = summaries.get((benchmark, model), {}).get(metric)
                    cells.append(_pct(value) if metric in {"exec", "acc"} else _score(value))
            body_lines.append(" & ".join(cells) + r" \\")
    body = "\n".join(body_lines)
    (out_dir / "performance_table.tex").write_text(
        rf"""\begin{{table*}}[t]
\centering
\setlength{{\tabcolsep}}{{4pt}}
\resizebox{{\textwidth}}{{!}}{{%
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
\end{{tabular}}}}
\caption{{Performance across TRACE-UFR and TRACE-DIR. Metrics include execution success, answer accuracy, retrieval quality, and structural reasoning quality.}}
\label{{tab:combined_results}}
\vspace{{-4mm}}
\end{{table*}}
""",
        encoding="utf-8",
    )


def _question_type(row: dict[str, Any]) -> str:
    return str(row.get("qkey") or row.get("family") or row.get("template_id") or "")


def _question_type_sort_key(qtype: str) -> tuple[int, str]:
    prefix_order = {"L0": 0, "A0": 1, "B0": 2}
    prefix = qtype.split("_", 1)[0]
    return (prefix_order.get(prefix, 99), qtype)


def _write_answer_accuracy_by_question_type(rows: list[dict[str, Any]], out_dir: Path) -> None:
    present = _present_models(rows)
    model_ids = [model for provider in PROVIDER_ORDER for model in present[provider]]
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    type_counts: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for row in rows:
        benchmark = str(row.get("benchmark_id"))
        model = _model_id(row)
        qtype = _question_type(row)
        if not qtype:
            continue
        grouped[(benchmark, qtype, model)].append(row)
        type_counts[(benchmark, qtype)][model] += 1

    body_lines: list[str] = []
    for benchmark in BENCHMARKS:
        qtypes = sorted(
            {qtype for b, qtype in type_counts if b == benchmark},
            key=_question_type_sort_key,
        )
        for idx, qtype in enumerate(qtypes):
            n_values = [type_counts[(benchmark, qtype)][model] for model in model_ids]
            n_cell = str(n_values[0]) if n_values and len(set(n_values)) == 1 else "/".join(str(n) for n in n_values)
            cells = [
                BENCH_LABELS[benchmark] if idx == 0 else "",
                _latex_escape(qtype),
                n_cell,
            ]
            for model in model_ids:
                group = grouped.get((benchmark, qtype, model), [])
                value = sum(bool(r.get("correct")) for r in group) / len(group) if group else None
                cells.append(_pct(value))
            body_lines.append(" & ".join(cells) + r" \\")
        if benchmark != BENCHMARKS[-1]:
            body_lines.append(r"\midrule")
    body = "\n".join(body_lines)
    model_header = " & ".join(rf"\textbf{{{MODEL_LABELS[model]}}}" for model in model_ids)
    colspec = "llr" + "c" * len(model_ids)
    (out_dir / "answer_accuracy_by_question_type_table.tex").write_text(
        rf"""\begin{{table*}}[t]
\centering
\footnotesize
\setlength{{\tabcolsep}}{{3pt}}
\resizebox{{\textwidth}}{{!}}{{%
\begin{{tabular}}{{{colspec}}}
\toprule
\textbf{{Domain}} & \textbf{{Question type}} & \textbf{{$n$/model}} & {model_header} \\
\midrule
{body}
\bottomrule
\end{{tabular}}}}
\caption{{Answer accuracy by question type for each model. Question types are derived from the TRACE template key; $n$/model gives the number of instances of that type evaluated per model.}}
\label{{tab:answer_accuracy_by_question_type}}
\vspace{{-3mm}}
\end{{table*}}
""",
        encoding="utf-8",
    )


def _error_counts(rows: list[dict[str, Any]]) -> dict[tuple[str, str], Counter[str]]:
    counts: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for row in rows:
        group = _error_group(row)
        if group:
            counts[(str(row.get("benchmark_id")), _model_id(row))][group] += 1
    return counts


def _detailed_error_code(row: dict[str, Any]) -> str | None:
    code = row.get("exec_error_code")
    if code:
        return str(code)
    if row.get("correct") is not False:
        return None
    mismatch = row.get("mismatch_kind")
    return f"M_{mismatch}" if mismatch else "M_wrong_answer"


def _detailed_error_counts(rows: list[dict[str, Any]]) -> dict[tuple[str, str], Counter[str]]:
    counts: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for row in rows:
        code = _detailed_error_code(row)
        if code:
            counts[(str(row.get("benchmark_id")), _model_id(row))][code] += 1
    return counts


def _observed_detailed_error_codes(rows: list[dict[str, Any]]) -> list[str]:
    observed = {_detailed_error_code(row) for row in rows}
    observed.discard(None)
    ordered = [code for code in DETAIL_ERROR_ORDER if code in observed]
    ordered.extend(sorted(code for code in observed if code not in set(DETAIL_ERROR_ORDER)))
    return ordered


def _shade(value: float, values: list[float]) -> int:
    positives = sorted(v for v in values if v > 0)
    if value <= 0 or not positives:
        return 0
    rank = sum(v <= value for v in positives) / len(positives)
    return [8, 16, 24, 32, 40][min(4, max(0, math.ceil(rank * 5) - 1))]


def _err_cell(count: int, denom: int, column_props: list[float]) -> str:
    if denom == 0:
        return ""
    prop = count / denom
    text = f"{100 * prop:.0f}"
    shade = _shade(prop, column_props)
    return rf"\colorbox{{red!{shade}}}{{\makebox[2.2em][c]{{\strut {text}}}}}" if shade else text


def _write_error_distribution(rows: list[dict[str, Any]], out_dir: Path) -> None:
    counts = _error_counts(rows)
    present = _present_models(rows)
    model_ids = [model for provider in PROVIDER_ORDER for model in present[provider]]
    colspec = "l" + "cc" * len(model_ids)
    group_header = " & ".join(rf"\multicolumn{{2}}{{c}}{{{MODEL_LABELS[m]}}}" for m in model_ids)
    cmidrules = " ".join(rf"\cmidrule(lr){{{2 + 2 * i}-{3 + 2 * i}}}" for i in range(len(model_ids)))
    subheader = "Error Type " + "".join(" & UFR & DIR" for _ in model_ids) + r" \\"
    denominators = {(b, m): sum(counts[(b, m)].values()) for b in BENCHMARKS for m in model_ids}
    column_props = {
        (b, m): [counts[(b, m)][g] / denominators[(b, m)] for g in ERROR_GROUPS]
        if denominators[(b, m)]
        else []
        for b in BENCHMARKS
        for m in model_ids
    }
    body_lines = []
    for group in ERROR_GROUPS:
        cells = [group]
        for model in model_ids:
            for benchmark in BENCHMARKS:
                cells.append(_err_cell(counts[(benchmark, model)][group], denominators[(benchmark, model)], column_props[(benchmark, model)]))
        body_lines.append(" & ".join(cells) + r" \\")
    body = "\n".join(body_lines)
    (out_dir / "error_distribution_table.tex").write_text(
        rf"""\begin{{table*}}[t]
\centering
\setlength{{\tabcolsep}}{{2pt}}
\resizebox{{\textwidth}}{{!}}{{%
\begin{{tabular}}{{{colspec}}}
\toprule
 & {group_header} \\
{cmidrules}
{subheader}
\midrule
{body}
\bottomrule
\end{{tabular}}}}
\caption{{Error distribution by model and domain for the d=3 setting. Entries are percentages of each model-domain's errors; darker cells indicate higher within-column quintiles.}}
\label{{tab:error_analysis}}
\vspace{{-4mm}}
\end{{table*}}
""",
        encoding="utf-8",
    )


def _write_raw_error_counts(rows: list[dict[str, Any]], out_dir: Path) -> None:
    counts = _error_counts(rows)
    present = _present_models(rows)
    model_ids = [model for provider in PROVIDER_ORDER for model in present[provider]]
    colspec = "l" + "cc" * len(model_ids)
    group_header = " & ".join(rf"\multicolumn{{2}}{{c}}{{{MODEL_LABELS[m]}}}" for m in model_ids)
    cmidrules = " ".join(rf"\cmidrule(lr){{{2 + 2 * i}-{3 + 2 * i}}}" for i in range(len(model_ids)))
    subheader = "Error Type " + "".join(" & UFR & DIR" for _ in model_ids) + r" \\"
    body = "\n".join(
        " & ".join([group] + [str(counts[(benchmark, model)][group]) for model in model_ids for benchmark in BENCHMARKS]) + r" \\"
        for group in ERROR_GROUPS
    )
    (out_dir / "error_counts_table.tex").write_text(
        rf"""\begin{{table*}}[t]
\centering
\setlength{{\tabcolsep}}{{2pt}}
\resizebox{{\textwidth}}{{!}}{{%
\begin{{tabular}}{{{colspec}}}
\toprule
 & {group_header} \\
{cmidrules}
{subheader}
\midrule
{body}
\bottomrule
\end{{tabular}}}}
\caption{{Raw error counts by model and domain for the d=3 setting.}}
\label{{tab:error_counts_raw}}
\vspace{{-4mm}}
\end{{table*}}
""",
        encoding="utf-8",
    )


def _write_detailed_error_codes(rows: list[dict[str, Any]], out_dir: Path) -> None:
    counts = _detailed_error_counts(rows)
    codes = _observed_detailed_error_codes(rows)
    present = _present_models(rows)
    model_ids = [model for provider in PROVIDER_ORDER for model in present[provider]]
    colspec = "l" + "cc" * len(model_ids)
    group_header = " & ".join(rf"\multicolumn{{2}}{{c}}{{{MODEL_LABELS[m]}}}" for m in model_ids)
    cmidrules = " ".join(rf"\cmidrule(lr){{{2 + 2 * i}-{3 + 2 * i}}}" for i in range(len(model_ids)))
    subheader = "Detailed Error " + "".join(" & UFR & DIR" for _ in model_ids) + r" \\"
    body = "\n".join(
        " & ".join(
            [_latex_escape(code)]
            + [str(counts[(benchmark, model)][code]) for model in model_ids for benchmark in BENCHMARKS]
        )
        + r" \\"
        for code in codes
    )
    (out_dir / "detailed_error_codes_table.tex").write_text(
        rf"""\begin{{table*}}[t]
\centering
\setlength{{\tabcolsep}}{{2pt}}
\resizebox{{\textwidth}}{{!}}{{%
\begin{{tabular}}{{{colspec}}}
\toprule
 & {group_header} \\
{cmidrules}
{subheader}
\midrule
{body}
\bottomrule
\end{{tabular}}}}
\caption{{Detailed error counts by model and domain for the d=3 setting. Executor failures retain their \texttt{{E\_*}} code; executed wrong answers are reported as \texttt{{M\_*}} mismatch kinds.}}
\label{{tab:detailed_error_codes}}
\vspace{{-4mm}}
\end{{table*}}
""",
        encoding="utf-8",
    )


def _write_top_detailed_errors(rows: list[dict[str, Any]], out_dir: Path) -> None:
    counts = _detailed_error_counts(rows)
    present = _present_models(rows)
    model_ids = [model for provider in PROVIDER_ORDER for model in present[provider]]

    body_lines: list[str] = []
    for model in model_ids:
        for idx, benchmark in enumerate(BENCHMARKS):
            total = sum(counts[(benchmark, model)].values())
            top = counts[(benchmark, model)].most_common(3)
            cells = [
                rf"\multirow{{2}}{{*}}{{{MODEL_LABELS[model]}}}" if idx == 0 else "",
                BENCH_LABELS[benchmark],
            ]
            for code, count in top:
                prop = count / total if total else 0.0
                cells.append(rf"\texttt{{{_latex_escape(code)}}} {count} ({100 * prop:.1f}\%)")
            cells.extend([""] * (5 - len(cells)))
            body_lines.append(" & ".join(cells) + r" \\")
        body_lines.append(r"\addlinespace[2pt]")
    if body_lines:
        body_lines.pop()
    body = "\n".join(body_lines)

    (out_dir / "top_detailed_errors_table.tex").write_text(
        rf"""\begin{{table}}[t]
\centering
\footnotesize
\setlength{{\tabcolsep}}{{4pt}}
\resizebox{{\linewidth}}{{!}}{{%
\begin{{tabular}}{{llccc}}
\toprule
\textbf{{Model}} & \textbf{{Domain}} & \textbf{{Top error 1}} & \textbf{{Top error 2}} & \textbf{{Top error 3}} \\
\midrule
{body}
\bottomrule
\end{{tabular}}}}
\caption{{Top detailed error codes by model and domain. Each cell reports count and percentage of that model-domain's total errors.}}
\label{{tab:top_detailed_errors}}
\vspace{{-3mm}}
\end{{table}}
""",
        encoding="utf-8",
    )


def _write_actions(out_dir: Path) -> None:
    lines: list[str] = []
    current = None
    for group, action, desc, inputs, output, ufr, dir_ in ACTION_ROWS:
        if group != current:
            if current is not None:
                lines.append(r"\midrule")
            lines.append(rf"\multicolumn{{6}}{{l}}{{\textit{{{group}}}}} \\")
            current = group
        lines.append(
            f"{action} & {desc} & {_latex_escape(inputs)} & {_latex_escape(output)} & "
            f"{'\\checkmark' if ufr else ''} & {'\\checkmark' if dir_ else ''} \\\\"
        )
    body = "\n".join(lines)
    (out_dir / "action_descriptions_table.tex").write_text(
        rf"""\begin{{table*}}[t]
\centering
\footnotesize
\setlength{{\tabcolsep}}{{4pt}}
\resizebox{{\textwidth}}{{!}}{{%
\begin{{tabular}}{{p{{2.4cm}} p{{6.4cm}} p{{2.8cm}} p{{2.6cm}} cc}}
\toprule
\textbf{{Action}} & \textbf{{Description}} & \textbf{{Input}} & \textbf{{Output}} & \textbf{{UFR}} & \textbf{{DIR}} \\
\midrule
{body}
\bottomrule
\end{{tabular}}}}
\caption{{Atomic actions used in TRACE reasoning DAGs. Actions are typed and domain-scoped; applicability to TRACE-UFR and TRACE-DIR is indicated.}}
\label{{tab:actions}}
\vspace{{-4mm}}
\end{{table*}}
""",
        encoding="utf-8",
    )


def _write_error_descriptions(out_dir: Path) -> None:
    body = "\n".join(
        f"{group} & {short} & {desc} \\\\"
        for group, (short, desc) in ERROR_DESCRIPTIONS.items()
    )
    (out_dir / "error_descriptions_table.tex").write_text(
        rf"""\begin{{wraptable}}{{r}}{{0.62\linewidth}}
\centering
\footnotesize
\setlength{{\tabcolsep}}{{4pt}}
\vspace{{-3mm}}
\begin{{tabular}}{{p{{1.7cm}} p{{2.6cm}} p{{5.4cm}}}}
\toprule
\textbf{{Group}} & \textbf{{Name}} & \textbf{{Description}} \\
\midrule
{body}
\bottomrule
\end{{tabular}}
\caption{{Error groups used in the TRACE error analysis.}}
\label{{tab:error_groups}}
\vspace{{-2mm}}
\end{{wraptable}}
""",
        encoding="utf-8",
    )


def _write_detailed_error_descriptions(rows: list[dict[str, Any]], out_dir: Path) -> None:
    observed = set(_observed_detailed_error_codes(rows))
    described = set(DETAIL_ERROR_DESCRIPTIONS)
    grouped_codes = {code for _, codes in DETAIL_ERROR_GROUPS for code in codes}
    body_lines: list[str] = []
    first_group = True
    for group, codes in DETAIL_ERROR_GROUPS:
        present_codes = [code for code in codes if code in described or code in observed]
        if not present_codes:
            continue
        if not first_group:
            body_lines.append(r"\midrule")
        first_group = False
        body_lines.append(rf"\multicolumn{{2}}{{l}}{{\textit{{{group}}}}} \\")
        for code in present_codes:
            desc = DETAIL_ERROR_DESCRIPTIONS.get(
                code, "Observed error code without a hand-authored description."
            )
            body_lines.append(
                f"\\texttt{{{_latex_escape(code)}}} & {desc} \\\\"
            )
    extra_codes = sorted((observed | described) - grouped_codes)
    if extra_codes:
        if body_lines:
            body_lines.append(r"\midrule")
        body_lines.append(r"\multicolumn{2}{l}{\textit{Other}} \\")
        for code in extra_codes:
            desc = DETAIL_ERROR_DESCRIPTIONS.get(
                code, "Observed error code without a hand-authored description."
            )
            body_lines.append(
                f"\\texttt{{{_latex_escape(code)}}} & {desc} \\\\"
            )
    body = "\n".join(body_lines)
    (out_dir / "full_error_descriptions_table.tex").write_text(
        rf"""\begin{{table*}}[t]
\centering
\footnotesize
\setlength{{\tabcolsep}}{{4pt}}
\renewcommand{{\arraystretch}}{{1.1}}
\begin{{tabular}}{{p{{3.1cm}} p{{11.0cm}}}}
\toprule
\textbf{{Code}} & \textbf{{Description}} \\
\midrule
{body}
\bottomrule
\end{{tabular}}
\caption{{Full set of TRACE executor and output-mismatch error codes used in the error analysis. Codes are grouped by failure class. Executor failures use \texttt{{E\_*}} codes; executed but incorrect outputs use \texttt{{M\_*}} mismatch codes.}}
\label{{tab:full_error_descriptions}}
\vspace{{-3mm}}
\end{{table*}}
""",
        encoding="utf-8",
    )


def _write_family_summary(exp_dir: Path, out_dir: Path) -> None:
    profiles = {b: _profile(exp_dir, b) for b in BENCHMARKS}
    families = sorted(set(profiles["trace_ufr"]["per_family"]) | set(profiles["trace_dir"]["per_family"]))
    lines = []
    for family in families:
        cells = [family]
        for benchmark in BENCHMARKS:
            stats = profiles[benchmark]["per_family"].get(family)
            if stats is None:
                cells.extend(["", "", "", ""])
            else:
                cells.extend([
                    str(stats["queries"]),
                    f"{stats['avg_snippets_per_query']:.2f}",
                    f"{stats['avg_fact_bindings_per_query']:.2f}",
                    f"{stats['action_count']['mean']:.2f}",
                ])
        lines.append(" & ".join(cells) + r" \\")
    body = "\n".join(lines)
    (out_dir / "family_summary_table.tex").write_text(
        rf"""\begin{{table}}[t]
\centering
\footnotesize
\setlength{{\tabcolsep}}{{4pt}}
\resizebox{{\linewidth}}{{!}}{{%
\begin{{tabular}}{{lcccccccc}}
\toprule
 & \multicolumn{{4}}{{c}}{{TRACE-UFR}} & \multicolumn{{4}}{{c}}{{TRACE-DIR}} \\
\cmidrule(lr){{2-5}} \cmidrule(lr){{6-9}}
Family & Queries & Snip./Q & Facts/Q & Actions/Q & Queries & Snip./Q & Facts/Q & Actions/Q \\
\midrule
{body}
\bottomrule
\end{{tabular}}}}
\caption{{Dataset composition by reasoning family.}}
\label{{tab:family_summary}}
\vspace{{-3mm}}
\end{{table}}
""",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("experiment_dir", type=Path)
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args()

    exp_dir = args.experiment_dir
    out_dir = args.out_dir or exp_dir / "tex_tables"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = _read_rows(exp_dir)
    _write_dataset_summary(exp_dir, out_dir)
    _write_performance(rows, out_dir)
    _write_answer_accuracy_by_question_type(rows, out_dir)
    _write_error_distribution(rows, out_dir)
    _write_raw_error_counts(rows, out_dir)
    _write_detailed_error_codes(rows, out_dir)
    _write_top_detailed_errors(rows, out_dir)
    _write_actions(out_dir)
    _write_error_descriptions(out_dir)
    _write_detailed_error_descriptions(rows, out_dir)
    _write_family_summary(exp_dir, out_dir)

    print(f"Read {len(rows)} rows from {len(_results_paths(exp_dir))} result file(s).")
    print(f"Wrote LaTeX tables to {out_dir}")
    for path in sorted(out_dir.glob("*.tex")):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
