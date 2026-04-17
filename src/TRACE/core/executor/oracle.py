from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from TRACE.generation.generation_types import Bindings


@dataclass(frozen=True)
class OracleContext:
    lookup_records: dict[str, dict[str, Any]]
    extracts_by_snippet: dict[str, list[dict[str, Any]]]


def _extract_to_model_fact(extraction: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "snippet_id": extraction["snippet_id"],
        "label": extraction["label"],
        "period": extraction["period"],
        "quantity": extraction["quantity"],
    }


def make_oracle_context(bindings: Bindings, lookup_map: Dict[str, str]) -> OracleContext:
    extraction_by_id = {
        binding.extraction_id: {
            "extraction_id": binding.extraction_id,
            "snippet_id": binding.snippet_id,
            "label": binding.label,
            "period": binding.period,
            "quantity": binding.quantity,
        }
        for binding in bindings.values()
    }

    lookup_records: dict[str, dict[str, Any]] = {}
    extracts_by_snippet: dict[str, list[dict[str, Any]]] = {}
    for node_id, extraction_id in lookup_map.items():
        extraction = extraction_by_id[extraction_id]
        lookup_records[node_id] = _extract_to_model_fact(extraction)
        extracts_by_snippet.setdefault(extraction["snippet_id"], []).append(extraction)

    return OracleContext(
        lookup_records=lookup_records,
        extracts_by_snippet=extracts_by_snippet,
    )

