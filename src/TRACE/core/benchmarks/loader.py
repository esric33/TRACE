from __future__ import annotations

from importlib import import_module
from pathlib import Path

from TRACE.core.benchmarks.types import BenchmarkDef


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
    )

