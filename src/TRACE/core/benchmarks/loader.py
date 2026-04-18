from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path

from TRACE.core.benchmarks.types import BenchmarkDef, PromptGuidance
from TRACE.generation.generation_types import ExtractRecord, load_extracts as load_records


def _default_load_extracts(extracts_dir: Path) -> list[ExtractRecord]:
    return load_records(extracts_dir)


def _default_derive_slots(record: ExtractRecord) -> dict[str, object]:
    return dict(record.slots)


def _default_load_allowed_labels(schemas_dir: Path) -> list[str]:
    path = schemas_dir / "label_enum.json"
    labels = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(labels, list) or not all(isinstance(x, str) for x in labels):
        raise ValueError(f"label_enum.json must be a list of strings: {path}")
    return labels


def _module_name_for(ref: str) -> str:
    if ref in {"trace_ufr", "TRACE-UFR"}:
        return "benchmarks.trace_ufr.benchmark"

    path = Path(ref)
    if path.exists():
        bench_py = path / "benchmark.py"
        if not bench_py.exists():
            raise FileNotFoundError(f"benchmark.py not found under {path}")
        return ".".join(bench_py.with_suffix("").parts)

    if ref.endswith(".benchmark"):
        return ref

    return f"benchmarks.{ref}.benchmark"


def load_benchmark(ref: str) -> BenchmarkDef:
    mod = import_module(_module_name_for(ref))
    return BenchmarkDef(
        benchmark_id=mod.BENCHMARK_ID,
        asset_root=Path(mod.ASSET_ROOT),
        snippets_dir=Path(mod.SNIPPETS_DIR),
        extracts_dir=Path(mod.EXTRACTS_DIR),
        schemas_dir=Path(mod.SCHEMAS_DIR),
        tables_dir=(Path(mod.TABLES_DIR) if getattr(mod, "TABLES_DIR", None) else None),
        templates_module=str(mod.TEMPLATES_MODULE),
        allowed_actions=set(mod.ALLOWED_ACTIONS),
        register_actions=mod.REGISTER_ACTIONS,
        load_extracts=getattr(mod, "LOAD_EXTRACTS", _default_load_extracts),
        load_allowed_labels=getattr(mod, "LOAD_ALLOWED_LABELS", _default_load_allowed_labels),
        derive_slots=getattr(mod, "DERIVE_SLOTS", _default_derive_slots),
        build_exists_key=getattr(mod, "BUILD_EXISTS_KEY", None),
        sampler_constraint_vars=getattr(mod, "SAMPLER_CONSTRAINT_VARS", None),
        sampler_constraint_ok=getattr(mod, "SAMPLER_CONSTRAINT_OK", None),
        prompt_guidance=getattr(mod, "PROMPT_GUIDANCE", PromptGuidance()),
        validate_planner_dag=getattr(mod, "VALIDATE_PLANNER_DAG", None),
        list_maintenance_tools=getattr(mod, "LIST_MAINTENANCE_TOOLS", None),
    )
