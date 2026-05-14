from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from TRACE.core.benchmarks.types import PromptGuidance
from TRACE.generation.generation_types import ExtractRecord
from TRACE.shared.io import read_json


BENCHMARK_ID = "trace_dir"
ASSET_ROOT = Path(__file__).resolve().parent
SNIPPETS_DIR = ASSET_ROOT / "snippets"
EXTRACTS_DIR = ASSET_ROOT / "extracts"
SCHEMAS_DIR = ASSET_ROOT / "schemas"
TABLES_DIR = None
TEMPLATES_MODULE = "benchmarks.trace_dir.templates.registry"
ALLOWED_ACTIONS = {
    "MODEL_FACT",
    "MAKE_SET",
    "SET_UNION",
    "SET_INTERSECT",
    "SET_DIFF",
    "SET_SIZE",
    "SET_CONTAINS",
    "CONST",
    "GT",
    "LT",
    "EQ",
}
PROMPT_GUIDANCE = PromptGuidance(
    planner_grounding_rules=(
        "- MODEL_FACT nodes should extract one directly stated medical relation from a provided snippet.",
        "- For TRACE-DIR, one MODEL_FACT corresponds to one relation: one subject, one label, and one object.",
        "- When constructing a relation set for a subject and label, extract ALL directly stated valid relations for that subject and label from the provided context snippets before using MAKE_SET.",
        "- Do not stop after a representative subset of relations; relation-set answers, SET_SIZE, SET_UNION, SET_INTERSECT, SET_DIFF, and SET_CONTAINS depend on the complete set of matching relations in context.",
        "- Relation facts use subject={type: drug, value: ...} and object={type: condition|drug|effect, value: ...}.",
    ),
    planner_compatibility_rules=(
        "- MAKE_SET requires all items to share the same relation label and object type.",
        "- SET_UNION / SET_INTERSECT / SET_DIFF require both sets to share the same relation label and object type.",
        "- SET_CONTAINS compares a relation set with a singleton relation fact by normalized object value.",
        "- SET_SIZE returns a scalar quantity that can be compared with GT / LT / EQ.",
    ),
    planner_default_ordering=(
        "1) Extract required relation facts with MODEL_FACT nodes",
        "2) Combine singleton facts with MAKE_SET",
        "3) Apply set operations or SET_CONTAINS / SET_SIZE",
    ),
)


def DERIVE_SLOTS(record: ExtractRecord) -> dict[str, object]:
    return {
        "label": record.label,
        "subject_type": record.subject.get("type"),
        "subject_value": record.subject.get("value"),
        "object_type": record.object.get("type"),
        "object_value": record.object.get("value"),
        "qtype": record.qtype,
        "snippet_id": record.snippet_id,
    }


def LOAD_EXTRACTS(extracts_dir: Path) -> list[ExtractRecord]:
    records: list[ExtractRecord] = []
    for path in sorted(extracts_dir.glob("*.json")):
        if not path.is_file():
            continue
        data = read_json(path)
        if "extraction_id" not in data:
            continue
        record = ExtractRecord.from_dict(data)
        records.append(record.with_slots(DERIVE_SLOTS(record)))
    return records


def BUILD_EXISTS_KEY(record: ExtractRecord) -> tuple[object, ...] | None:
    subject_value = record.slot("subject_value")
    object_value = record.slot("object_value")
    if not subject_value or not object_value:
        return None
    return (
        ("label", record.label),
        ("subject_type", record.slot("subject_type")),
        ("subject_value", subject_value),
        ("object_type", record.slot("object_type")),
        ("object_value", object_value),
    )


def VALIDATE_PLANNER_DAG(dag: dict[str, object]) -> None:
    nodes = dag.get("nodes")
    if not isinstance(nodes, list):
        raise ValueError("trace_dir dag.nodes must be a list")
    if not any(isinstance(node, dict) and node.get("op") == "MODEL_FACT" for node in nodes):
        raise ValueError("trace_dir plans must include at least one MODEL_FACT node")


def BUILD_PLANNER_PROMPT_SUPPLEMENT(
    capsule: dict[str, Any],
    benchmark_def,
) -> str:
    context_snippet_ids = {
        snippet.get("snippet_id")
        for snippet in capsule.get("context", {}).get("snippets", [])
        if isinstance(snippet, dict)
    }
    by_label: dict[str, set[str]] = {}
    for record in benchmark_def.load_extracts(benchmark_def.extracts_dir):
        if record.snippet_id not in context_snippet_ids:
            continue
        object_value = record.object.get("value")
        if not isinstance(object_value, str) or not object_value.strip():
            continue
        by_label.setdefault(record.label, set()).add(object_value.strip())

    allowed_labels = benchmark_def.load_allowed_labels(benchmark_def.schemas_dir)
    payload = {
        label: sorted(by_label.get(label, set()), key=str.casefold)
        for label in allowed_labels
        if by_label.get(label)
    }
    return (
        "VALID OBJECT VALUES IN THIS CONTEXT:\n"
        + json.dumps(payload, indent=2, ensure_ascii=False)
        + "\nUse these object values when a MODEL_FACT object matches one of them; do not invent alternate object wording."
    )


def LIST_MAINTENANCE_TOOLS() -> dict[str, str]:
    return {
        "prepare_relation_extracts": "benchmarks.trace_dir.tools.prepare_relation_extracts",
    }


def REGISTER_ACTIONS(registry) -> None:
    from benchmarks.trace_dir.actions import register_actions

    register_actions(registry)
