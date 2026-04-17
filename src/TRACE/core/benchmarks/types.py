from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class BenchmarkDef:
    benchmark_id: str
    asset_root: Path
    snippets_dir: Path
    extracts_dir: Path
    schemas_dir: Path
    tables_dir: Path | None
    templates_module: str
    allowed_actions: set[str]
    register_actions: Callable[[object], None]

