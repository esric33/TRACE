from __future__ import annotations

import os
from typing import Dict, List, Optional

from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.core.executor.runtime import execute_dag as execute_dag_runtime
from TRACE.core.executor.support import (
    ExecError,
    ModelFact,
    Period,
    Quantity,
    _attach_period,
    _get_q_period,
    _is_rate,
    _is_scalar,
    _q_norm,
    _rate_from,
    _rate_to,
    canonical_period,
    convert_scale,
    load_extract_store,
    period_equal,
    quantity_equal,
    resolve_fact_for_tagging,
)


__all__ = [
    "ExecError",
    "ModelFact",
    "Period",
    "Quantity",
    "_attach_period",
    "_get_q_period",
    "_is_rate",
    "_is_scalar",
    "_q_norm",
    "_rate_from",
    "_rate_to",
    "canonical_period",
    "convert_scale",
    "execute_dag_strict",
    "load_extract_store",
    "period_equal",
    "quantity_equal",
    "resolve_fact_for_tagging",
]


def execute_dag_strict(
    dag: Dict[str, object],
    capsule: Dict[str, object],
    extracts_by_snippet: Dict[str, List[Dict[str, object]]],
    *,
    cache: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    if cache is None:
        cache = {}

    benchmark_def = load_benchmark(os.environ.get("TRACE_BENCHMARK_ID", "trace_ufr"))
    return execute_dag_runtime(
        dag=dag,
        benchmark_def=benchmark_def,
        capsule=capsule,
        cache=cache,
    )
