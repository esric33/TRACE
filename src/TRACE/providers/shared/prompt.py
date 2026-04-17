from __future__ import annotations

from typing import Any, Dict

from TRACE.core.actions import build_registry_for_benchmark
from TRACE.core.benchmarks.loader import load_benchmark


def build_lookup_prompt(
    query: str, snippet_text: str, allowed_labels: list[str]
) -> str:
    labels_block = ", ".join(allowed_labels)
    return f"""
Return ONLY one JSON object. No markdown. No code fences.

REQUIRED JSON SHAPE:
{{
  "snippet_id": "<one of the snippet ids in TEXT>",
  "label": "<one of ALLOWED LABELS>",
  "period": {{
    "period": "FY" | "Q" | "ASOF",
    "value": <see PERIOD NORMALIZATION>
  }},
  "quantity": {{
    "value": <number>,
    "unit": "USD" | "EUR" | "JPY" | "TWD" | "GBP" | "KRW" | "RMB" | "CHF" | "percent" | "items" | "people",
    "scale": <number like 1, 1000, 1000000, 1000000000>,
    "type": "money" | "rate" | "per_share" | "count"
  }}
}}

CORE RULES:
- Output must be valid JSON and match the shape above exactly (no extra keys).
- snippet_id MUST be copied exactly from one of the snippet blocks in TEXT.
- The extracted value MUST appear verbatim in that same snippet block.

LABEL RULES:
- label MUST be exactly one of ALLOWED LABELS below.
- Do NOT paraphrase or invent labels.
- Choose the MOST SPECIFIC applicable label.

PERIOD NORMALIZATION (critical):
- period MUST be one of: FY | Q | ASOF
- FY -> integer year (e.g. FY24 -> 2024)
- Q  -> "Qn YYYY" (e.g. "Q4 2024")
- ASOF -> "YYYY-MM-DD"

QUANTITY RULES (critical):
- Use unit codes only (no symbols, no words).
- Percentages MUST use unit="percent".
- Put thousands/millions/billions into quantity.scale.
- quantity.value must be the stated numeric value (no extra math, no rounding).
- quantity.type must be consistent with the fact (money / rate / per_share / count).

TASK:
Extract ONE directly stated fact from TEXT that answers QUERY.
If the exact answer is not stated, return the closest directly stated fact.

ALLOWED LABELS:
[{labels_block}]

QUERY:
{query}

TEXT:
{snippet_text}
""".strip()


def _planner_operator_block(benchmark_def) -> str:
    registry = build_registry_for_benchmark(benchmark_def)
    ordered_ops = (
        "TEXT_LOOKUP",
        "GET_QUANTITY",
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
    docs = {
        "TEXT_LOOKUP": '- TEXT_LOOKUP:  {"query": string}',
        "GET_QUANTITY": '- GET_QUANTITY: {"fact": "ref:<id>"}',
        "CONVERT_SCALE": '- CONVERT_SCALE: {"q": "ref:<id>", "target_scale": number}',
        "FX_LOOKUP": '- FX_LOOKUP: {"series_id": "FX_<BASE>_<QUOTE>", "year": number}',
        "CPI_LOOKUP": '- CPI_LOOKUP: {"series_id": "CPI_US_CPIU", "from_year": number, "to_year": number}',
        "CONST": '- CONST: {"value": number}',
        "ADD": '- ADD: {"a": "ref:<id>", "b": "ref:<id>"}',
        "MUL": '- MUL: {"a": "ref:<id>", "b": "ref:<id>"}',
        "DIV": '- DIV: {"a": "ref:<id>", "b": "ref:<id>"}',
        "GT": '- GT: {"a": "ref:<id>", "b": "ref:<id>"}',
        "LT": '- LT: {"a": "ref:<id>", "b": "ref:<id>"}',
        "EQ": '- EQ: {"a": "ref:<id>", "b": "ref:<id>"}',
        "AND": '- AND: {"a": "ref:<id>", "b": "ref:<id>"}',
        "OR": '- OR: {"a": "ref:<id>", "b": "ref:<id>"}',
    }

    lines = []
    for op in ordered_ops:
        if op not in benchmark_def.allowed_actions:
            continue
        if op in docs:
            lines.append(docs[op])
            continue
        action = registry.require(op)
        fields = ", ".join(action.arg_keys)
        lines.append(f"- {op}: {{{fields}}}")
    return "\n".join(lines)


def build_planner_prompt(capsule: Dict[str, Any], *, benchmark_def=None) -> str:
    if benchmark_def is None:
        benchmark_def = load_benchmark("trace_ufr")

    ctx = "\n\n".join(
        [f"[{s['snippet_id']}]\n{s['text']}" for s in capsule["context"]["snippets"]]
    )

    allowed_ops_block = _planner_operator_block(benchmark_def)
    include_fx = "FX_LOOKUP" in benchmark_def.allowed_actions
    include_cpi = "CPI_LOOKUP" in benchmark_def.allowed_actions

    compatibility_lines = [
        "- ADD / GT / LT / EQ require both inputs to match type, unit, and scale.",
        "- If scales differ, use CONVERT_SCALE before ADD/GT/LT/EQ.",
    ]
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
                "- Money must be in USD for CPI adjustment.",
            ]
        )

    default_ordering = [
        "1) Retrieve facts: TEXT_LOOKUP -> GET_QUANTITY",
        "2) If a specific output scale is requested, CONVERT_SCALE to that target scale",
    ]
    if include_fx:
        default_ordering.append(
            "3) If currency conversion is needed, FX_LOOKUP(year) then MUL(money, fx_rate)"
        )
    if include_cpi:
        default_ordering.append(
            "4) If inflation adjustment is needed, CPI_LOOKUP(from_year,to_year) then MUL(money, cpi_rate)"
        )
    default_ordering.append("5) Combine/compare: ADD or GT/LT/EQ")

    return (
        "Return ONLY a JSON object. No markdown. No extra keys.\n"
        'Top-level JSON MUST be exactly: {"dag": {"nodes": [...], "output": "ref:<node_id>"}}\n\n'
        "DAG / NODE FORMAT:\n"
        "- dag.nodes is an ordered list of nodes.\n"
        "- Each node MUST have exactly keys: id, op, args.\n"
        "- Node ids must be unique and look like n1, n2, n3, ...\n"
        '- References to prior nodes MUST be strings like "ref:n7".\n'
        "- dag.output MUST be a ref to the final node.\n\n"
        "GROUNDING (non-negotiable):\n"
        "- All required facts MUST come from CONTEXT via TEXT_LOOKUP, then GET_QUANTITY.\n"
        "- Do NOT invent values.\n"
        "- Do NOT compute values inside TEXT_LOOKUP queries.\n"
        "- TEXT_LOOKUP.query should specify what to extract (label/metric + company + period).\n\n"
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
        + "MINIMALITY:\n"
        + "- Do not add unused nodes.\n"
        + "- Do not do conversions unless required by the question or operator compatibility.\n\n"
        + f"QUESTION:\n{capsule['question']}\n\n"
        + f"CONTEXT:\n{ctx}\n"
    )
