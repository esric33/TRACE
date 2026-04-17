from __future__ import annotations
from typing import Any, Dict, List

from TRACE.execute.executor import ExecError


def offline_lookup_fn(node_id, _, capsule, extracts_by_snippet):
    lookup_map = capsule["gold"]["lookup_map"]
    ex_id = lookup_map[node_id]

    for exs in extracts_by_snippet.values():
        for ex in exs:
            if ex["extraction_id"] == ex_id:
                return {
                    "snippet_id": ex["snippet_id"],
                    "label": ex["label"],
                    "period": ex["period"],
                    "quantity": ex["quantity"],
                }

    raise ExecError(
        "E_lookup_failed",
        "gold extraction not found",
        {"node": node_id, "extraction_id": ex_id},
    )
