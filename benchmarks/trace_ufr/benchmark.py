from __future__ import annotations

from pathlib import Path

from TRACE.core.benchmarks.types import PromptGuidance
from TRACE.generation.generation_types import ExtractRecord
from TRACE.shared.io import read_json


BENCHMARK_ID = "trace_ufr"
ASSET_ROOT = Path(__file__).resolve().parent
SNIPPETS_DIR = ASSET_ROOT / "snippets"
EXTRACTS_DIR = ASSET_ROOT / "extracts"
SCHEMAS_DIR = ASSET_ROOT / "schemas"
TABLES_DIR = ASSET_ROOT / "tables"
TEMPLATES_MODULE = "benchmarks.trace_ufr.templates.registry"
ALLOWED_ACTIONS = {
    "TEXT_LOOKUP",
    "GET_QUANTITY",
    "CONVERT_SCALE",
    "CONST",
    "ADD",
    "MUL",
    "DIV",
    "GT",
    "LT",
    "EQ",
    "FX_LOOKUP",
    "CPI_LOOKUP",
}
PROMPT_GUIDANCE = PromptGuidance(
    lookup_rules=(
        "- Focus QUERY on the requested label/metric together with company and period.",
    ),
    planner_grounding_rules=(
        "- TEXT_LOOKUP.query should specify what to extract (label/metric + company + period).",
    ),
)


def _year_from_period(kind: str, value: object) -> int | None:
    kind = str(kind).upper()
    if kind == "FY":
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.strip().isdigit():
            return int(value.strip())
        return None
    if kind == "Q" and isinstance(value, str):
        parts = value.strip().split()
        if len(parts) == 2 and parts[0].upper().startswith("Q") and parts[1].isdigit():
            return int(parts[1])
    if kind == "ASOF" and isinstance(value, str):
        parts = value.strip().split("-", 2)
        if len(parts) == 3 and parts[0].isdigit():
            return int(parts[0])
    return None


def _canon_period_value(record: ExtractRecord) -> object:
    if str(record.period_kind).upper() != "FY":
        return record.period_value
    year = _year_from_period(record.period_kind, record.period_value)
    return year if year is not None else record.period_value


def DERIVE_SLOTS(record: ExtractRecord) -> dict[str, object]:
    return {
        "company": record.company,
        "metric_key": record.metric_key,
        "metric_role": record.metric_role,
        "label": record.label,
        "period": (record.period_kind, record.period_value),
        "unit": record.unit,
        "scale": record.scale,
        "qtype": record.qtype,
    }


def LOAD_EXTRACTS(extracts_dir: Path) -> list[ExtractRecord]:
    records: list[ExtractRecord] = []
    for path in sorted(extracts_dir.glob("*.json")):
        if not path.is_file():
            continue
        record = ExtractRecord.from_dict(read_json(path))
        records.append(record.with_slots(DERIVE_SLOTS(record)))
    return records


def BUILD_EXISTS_KEY(record: ExtractRecord) -> tuple[object, ...] | None:
    company = record.slot("company")
    metric_key = record.slot("metric_key")
    if not company or not metric_key:
        return None
    return (
        ("company", company),
        ("metric_key", metric_key),
        ("period_kind", str(record.period_kind).upper()),
        ("period_value", _canon_period_value(record)),
    )


def VALIDATE_PLANNER_DAG(dag: dict[str, object]) -> None:
    nodes = dag.get("nodes")
    if not isinstance(nodes, list):
        raise ValueError("trace_ufr dag.nodes must be a list")
    if not any(isinstance(node, dict) and node.get("op") == "TEXT_LOOKUP" for node in nodes):
        raise ValueError("trace_ufr plans must include at least one TEXT_LOOKUP node")


def LIST_MAINTENANCE_TOOLS() -> dict[str, str]:
    return {}


def REGISTER_ACTIONS(registry) -> None:
    from benchmarks.trace_ufr.actions import register_actions

    register_actions(registry)
