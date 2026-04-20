from __future__ import annotations
from typing import Any, Dict, List

from TRACE.core.executor.support import ExecError, ExecErrorCode, exec_error_data


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
        ExecErrorCode.LOOKUP_FAILED,
        "gold extraction not found",
        exec_error_data(
            phase="lookup", provider="offline", node_id=node_id, extraction_id=ex_id
        ),
    )
