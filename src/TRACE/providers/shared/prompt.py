from __future__ import annotations

import json
from typing import Any, Dict

from TRACE.core.actions import build_registry_for_benchmark
from TRACE.core.benchmarks.loader import load_benchmark


def _planner_operator_block(benchmark_def) -> str:
    registry = build_registry_for_benchmark(benchmark_def)
    ordered_ops = (
        "MODEL_FACT",
        "MAKE_SET",
        "SET_UNION",
        "SET_INTERSECT",
        "SET_DIFF",
        "SET_SIZE",
        "SET_CONTAINS",
        "CONVERT_SCALE",
        "FX_LOOKUP",
        "CPI_LOOKUP",
        "CONST",
        "ADD",
        "MUL",
        "DIV",
        "GT",
        "LT",
        "EQ",
        "AND",
        "OR",
    )

    lines = []
    for op in ordered_ops:
        if op not in benchmark_def.allowed_actions:
            continue
        action = registry.require(op)
        lines.append(action.prompt_doc())
    return "\n".join(lines)


def build_planner_prompt(capsule: Dict[str, Any], *, benchmark_def=None) -> str:
    if benchmark_def is None:
        benchmark_def = load_benchmark("trace_ufr")

    if benchmark_def.build_planner_prompt is not None:
        return benchmark_def.build_planner_prompt(capsule, benchmark_def)

    ctx = "\n\n".join(
        [f"[{s['snippet_id']}]\n{s['text']}" for s in capsule["context"]["snippets"]]
    )

    allowed_ops_block = _planner_operator_block(benchmark_def)
    allowed_labels = benchmark_def.load_allowed_labels(benchmark_def.schemas_dir)
    include_fx = "FX_LOOKUP" in benchmark_def.allowed_actions
    include_cpi = "CPI_LOOKUP" in benchmark_def.allowed_actions
    include_scale = "CONVERT_SCALE" in benchmark_def.allowed_actions
    include_add = "ADD" in benchmark_def.allowed_actions

    compatibility_lines = []
    if include_add:
        compatibility_lines.append(
            "- ADD / GT / LT / EQ require both inputs to match type, unit, and scale."
        )
    if include_scale:
        compatibility_lines.append(
            "- If scales differ, use CONVERT_SCALE before ADD/GT/LT/EQ."
        )
    compatibility_lines.extend(
        benchmark_def.prompt_guidance.planner_compatibility_rules
    )
    if include_fx:
        compatibility_lines.append(
            "- If currencies differ, convert using FX_LOOKUP + MUL."
        )

    fx_cpi_lines = ["WHEN TO USE FX vs CPI:"]
    if include_fx:
        fx_cpi_lines.extend(
            [
                "- FX: currency conversion using a single specified year's exchange rate.",
                "- Currency codes must be exactly one of: CHF, EUR, GBP, JPY, KRW, RMB, TWD, USD",
                "- FX_LOOKUP.series_id MUST be: FX_<BASE>_<QUOTE>",
            ]
        )
    if include_cpi:
        fx_cpi_lines.extend(
            [
                "- CPI: inflation adjustment of money between years (real terms / price-level adjustment).",
                "- Only use CPI when explicitly requested.",
                "- Money must be in USD for CPI adjustment."
                "- use CPI_US_CPIU series id for CPI_LOOKUP",
            ]
        )

    default_ordering = ["1) Extract required facts with MODEL_FACT nodes"]
    if include_scale:
        default_ordering.append(
            "2) If a specific output scale is requested, CONVERT_SCALE to that target scale"
        )
    if include_fx:
        default_ordering.append(
            "3) If currency conversion is needed, FX_LOOKUP(year) then MUL(money, fx_rate)"
        )
    if include_cpi:
        default_ordering.append(
            "4) If inflation adjustment is needed, CPI_LOOKUP(from_year,to_year) then MUL(money, cpi_rate)"
        )
    if include_add:
        default_ordering.append("5) Combine/compare: ADD or GT/LT/EQ")
    default_ordering.extend(benchmark_def.prompt_guidance.planner_default_ordering)

    grounding_lines = [
        "- All required facts MUST come from CONTEXT via MODEL_FACT nodes.",
        "- Each MODEL_FACT node must assert exactly one atomic extracted fact.",
        "- Each MODEL_FACT must copy one directly stated fact from the referenced snippet.",
        "- Do NOT invent values.",
        "- Do NOT compute values inside MODEL_FACT nodes.",
        "- If a question needs multiple facts, use multiple MODEL_FACT nodes and combine them with the appropriate operator.",
    ]
    grounding_lines.extend(benchmark_def.prompt_guidance.planner_grounding_rules)

    minimality_lines = [
        "- Do not add unused nodes.",
        "- Do not do conversions unless required by the question or operator compatibility.",
    ]
    minimality_lines.extend(benchmark_def.prompt_guidance.planner_minimality_rules)

    model_fact_schema_path = benchmark_def.schemas_dir / "model_fact.json"
    if model_fact_schema_path.exists():
        model_fact_schema = json.loads(model_fact_schema_path.read_text(encoding="utf-8"))
    else:
        model_fact_schema = {
            "snippet_id": "<one of the snippet ids in CONTEXT>",
            "label": "<one of ALLOWED LABELS>",
        }
    prompt_supplement = ""
    if benchmark_def.build_planner_prompt_supplement is not None:
        prompt_supplement = benchmark_def.build_planner_prompt_supplement(
            capsule,
            benchmark_def,
        ).strip()

    return (
        "Return ONLY a JSON object. No markdown. No extra keys.\n"
        'Top-level JSON MUST be exactly: {"dag": {"nodes": [...], "output": "ref:<node_id>"}}\n\n'
        "DAG / NODE FORMAT:\n"
        "- dag.nodes is an ordered list of nodes.\n"
        "- Each node MUST have exactly keys: id, op, args.\n"
        "- Node ids must be unique and look like n1, n2, n3, ...\n"
        '- References to prior nodes MUST be strings like "ref:n7".\n'
        "- dag.output MUST be a ref to the final node.\n\n"
        "GROUNDING (non-negotiable):\n" + "\n".join(grounding_lines) + "\n\n"
        "COMPATIBILITY RULES:\n"
        + "\n".join(compatibility_lines)
        + "\n\n"
        + "\n".join(fx_cpi_lines)
        + "\n\n"
        + "DEFAULT ORDERING (use when needed):\n"
        + "\n".join(default_ordering)
        + "\n\n"
        + "ALLOWED OPERATORS (args schema):\n"
        + allowed_ops_block
        + "\n\n"
        + "MODEL_FACT SCHEMA:\n"
        + json.dumps(model_fact_schema, indent=2, ensure_ascii=False)
        + "\n\n"
        + "ALLOWED LABELS:\n"
        + json.dumps(allowed_labels, ensure_ascii=False)
        + "\n\n"
        + ((prompt_supplement + "\n\n") if prompt_supplement else "")
        + "MINIMALITY:\n"
        + "\n".join(minimality_lines)
        + "\n\n"
        + f"QUESTION:\n{capsule['question']}\n\n"
        + f"CONTEXT:\n{ctx}\n"
    )
