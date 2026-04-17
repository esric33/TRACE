from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List, Optional

from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.core.executor.runtime import execute_dag as execute_dag_runtime
from TRACE.core.executor.support import (
    ExecError,
    LookupFn,
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
    "LookupFn",
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


@dataclass(frozen=True)
class _ProviderContext:
    lookup_fn: LookupFn
    extracts_by_snippet: Dict[str, List[Dict[str, object]]]


def execute_dag_strict(
    dag: Dict[str, object],
    capsule: Dict[str, object],
    extracts_by_snippet: Dict[str, List[Dict[str, object]]],
    *,
    cache: Optional[Dict[str, object]] = None,
    lookup_fn: Optional[LookupFn] = None,
) -> Dict[str, object]:
    if cache is None:
        cache = {}

    if lookup_fn is None:
        raise ExecError(
            "E_bad_args",
            "execute_dag_strict requires lookup_fn for TEXT_LOOKUP (offline mode)",
        )

    benchmark_def = load_benchmark(os.environ.get("TRACE_BENCHMARK_ID", "trace_ufr"))
    return execute_dag_runtime(
        dag=dag,
        benchmark_def=benchmark_def,
        mode="provider",
        provider_ctx=_ProviderContext(
            lookup_fn=lookup_fn,
            extracts_by_snippet=extracts_by_snippet,
        ),
        oracle_ctx=None,
        capsule=capsule,
        cache=cache,
    )
