from __future__ import annotations

from pathlib import Path


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


def REGISTER_ACTIONS(registry) -> None:
    from benchmarks.trace_ufr.actions import register_actions

    register_actions(registry)

