from __future__ import annotations

import random
from pathlib import Path
from typing import Any, Dict, Optional

from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.core.executor.oracle import make_oracle_context
from TRACE.core.executor.runtime import execute_dag
from TRACE.shared.io import read_json
from TRACE.generation.expr import (
    Add,
    Const,
    ConvertScale,
    ConvertScaleTo,
    CpiLookup,
    Div,
    Eq,
    FxLookup,
    FxLookupAt,
    FxLookupTo,
    Gt,
    LookupQty,
    Lt,
    Mul,
)
from TRACE.generation.generation_types import Bindings, CompiledPlan, Spec, load_snippets


def _lookup_query_for(record) -> str:
    pk = record.period_kind
    pv = record.period_value
    return (
        f"Extract the fact for: company={record.company} label={record.label}; period={pk} {pv}. "
        f"Return a ModelFact with snippet_id, label, period, quantity."
    )


def _canon_year(x: Any) -> int:
    if isinstance(x, int):
        return x
    s = str(x).strip()
    if s.isdigit():
        return int(s)
    raise TypeError(f"Expected year int (or numeric string), got {x!r}")


def _fx_quotes_for_base(base: str, *, benchmark_def) -> list[str]:
    tables_dir = benchmark_def.tables_dir
    if tables_dir is None:
        return []
    fx_dir = Path(tables_dir) / "fx"
    if not fx_dir.exists():
        return []

    quotes: set[str] = set()
    for path in fx_dir.glob("*.json"):
        quote: str | None = None
        try:
            table = read_json(path)
            series_id = str(table.get("series_id", ""))
            if series_id.startswith("FX_"):
                parts = series_id.split("_", 2)
                if len(parts) == 3 and parts[1] == base and parts[2] != base:
                    quote = parts[2]
        except Exception:
            quote = None

        if quote is None:
            stem = path.stem.upper()
            if stem.startswith("FX_"):
                parts = stem.split("_", 2)
                if len(parts) == 3 and parts[1] == base and parts[2] != base:
                    quote = parts[2]

        if quote:
            quotes.add(quote)

    return sorted(quotes)


def compile_spec(
    spec: Spec,
    bindings: Bindings,
    benchmark_def=None,
    *,
    seed: Optional[int] = None,
) -> CompiledPlan:
    if benchmark_def is None:
        benchmark_def = load_benchmark("trace_ufr")

    rng = random.Random(seed)

    nodes: list[dict[str, Any]] = []
    lookup_map: dict[str, str] = {}
    snippet_ids: list[str] = []
    id_counter = 1

    chosen_target_scale: dict[int, float] = {}
    chosen_fx_pair: dict[int, tuple[str, str]] = {}

    def new_id() -> str:
        nonlocal id_counter
        nid = f"n{id_counter}"
        id_counter += 1
        return nid

    def emit(op: str, **args: Any) -> str:
        nid = new_id()
        nodes.append({"id": nid, "op": op, "args": args})
        return f"ref:{nid}"

    def _require_fy_int(rec, *, who: str) -> int:
        if rec.period_kind != "FY" or not isinstance(rec.period_value, int):
            raise TypeError(
                f"{who} expects FY int year, got {rec.period_kind} {rec.period_value!r}"
            )
        return rec.period_value

    def _fx_series_id(base: str, quote: str) -> str:
        if not isinstance(base, str) or not base:
            raise TypeError(f"FX base currency must be non-empty string, got {base!r}")
        if not isinstance(quote, str) or not quote:
            raise TypeError(
                f"FX quote currency must be non-empty string, got {quote!r}"
            )
        if base == quote:
            raise ValueError("FX would be a no-op currency conversion")
        return f"FX_{base}_{quote}"

    def _choose_fx_quote(
        base: str, *, expr_quote: Optional[str], expr_quote_in: Any
    ) -> str:
        if expr_quote is not None:
            return expr_quote
        if expr_quote_in is not None:
            candidates = [q for q in expr_quote_in if q != base]
        else:
            candidates = _fx_quotes_for_base(base, benchmark_def=benchmark_def)
        if not candidates:
            raise ValueError(f"No FX quote candidates for base={base!r}")
        return rng.choice(candidates)

    def _compile_fx_lookup_common(*, expr_key: int, year: int, base: str, quote: str) -> str:
        chosen_fx_pair[expr_key] = (base, quote)
        series_id = _fx_series_id(base, quote)
        return emit("FX_LOOKUP", series_id=series_id, year=year)

    def _emit_convert_scale(inner_ref: str, target_scale: float) -> str:
        return emit("CONVERT_SCALE", q=inner_ref, target_scale=float(target_scale))

    def compile_expr(expr) -> str:
        if isinstance(expr, LookupQty):
            rec = bindings[expr.var_name]

            n_lookup = new_id()
            nodes.append(
                {
                    "id": n_lookup,
                    "op": "TEXT_LOOKUP",
                    "args": {"query": _lookup_query_for(rec)},
                }
            )
            n_getq = new_id()
            nodes.append(
                {
                    "id": n_getq,
                    "op": "GET_QUANTITY",
                    "args": {"fact": f"ref:{n_lookup}"},
                }
            )

            lookup_map[n_lookup] = rec.extraction_id
            snippet_ids.append(rec.snippet_id)
            return f"ref:{n_getq}"

        if isinstance(expr, ConvertScale):
            inner_ref = compile_expr(expr.expr)

            allow_noop = bool(spec.compile_opts.get("t1_allow_noop", True))
            target_scales = [float(x) for x in expr.target_scale_in]

            if not allow_noop:
                if not isinstance(expr.expr, LookupQty):
                    raise TypeError(
                        "t1_allow_noop=False only supported for ConvertScale(LookupQty(...))"
                    )
                src_scale = float(bindings[expr.expr.var_name].scale)
                target_scales = [s for s in target_scales if s != src_scale]
                if not target_scales:
                    raise ValueError(
                        f"No non-noop target scale available (src_scale={src_scale})"
                    )

            target_scale = float(rng.choice(target_scales))
            chosen_target_scale[id(expr)] = target_scale
            return _emit_convert_scale(inner_ref, target_scale)

        if isinstance(expr, ConvertScaleTo):
            inner_ref = compile_expr(expr.expr)
            if expr.to_var not in bindings:
                raise KeyError(f"ConvertScaleTo to_var not in bindings: {expr.to_var!r}")
            target_scale = float(bindings[expr.to_var].scale)
            return _emit_convert_scale(inner_ref, target_scale)

        if isinstance(expr, Add):
            return emit("ADD", a=compile_expr(expr.left), b=compile_expr(expr.right))

        if isinstance(expr, Const):
            return emit("CONST", value=float(expr.value))

        if isinstance(expr, Div):
            return emit("DIV", a=compile_expr(expr.left), b=compile_expr(expr.right))

        if isinstance(expr, Mul):
            return emit("MUL", a=compile_expr(expr.left), b=compile_expr(expr.right))

        if isinstance(expr, CpiLookup):
            rec_from = bindings[expr.from_var]
            rec_to = bindings[expr.to_var]
            y_from = _require_fy_int(rec_from, who="CPI_LOOKUP(from_var)")
            y_to = _require_fy_int(rec_to, who="CPI_LOOKUP(to_var)")
            return emit(
                "CPI_LOOKUP", series_id=expr.series_id, from_year=y_from, to_year=y_to
            )

        if isinstance(expr, FxLookup):
            rec = bindings[expr.var_name]
            year = _require_fy_int(rec, who="FX_LOOKUP(var_name)")
            base = expr.base or rec.unit
            if not isinstance(base, str) or not base:
                raise TypeError("FX_LOOKUP base currency must be a non-empty string")
            quote = _choose_fx_quote(
                base, expr_quote=expr.quote, expr_quote_in=expr.quote_in
            )
            return _compile_fx_lookup_common(
                expr_key=id(expr), year=year, base=base, quote=quote
            )

        if isinstance(expr, FxLookupTo):
            rec_from = bindings[expr.from_var]
            rec_to = bindings[expr.to_var]
            y_from = _require_fy_int(rec_from, who="FX_LOOKUP_TO(from_var)")
            y_to = _require_fy_int(rec_to, who="FX_LOOKUP_TO(to_var)")
            if y_from != y_to:
                raise TypeError(
                    f"FX_LOOKUP_TO requires same FY year: from={y_from}, to={y_to}"
                )
            base = expr.base if expr.base is not None else rec_from.unit
            quote = rec_to.unit
            return _compile_fx_lookup_common(
                expr_key=id(expr), year=y_from, base=base, quote=quote
            )

        if isinstance(expr, FxLookupAt):
            rec_base = bindings[expr.base_var]
            rec_quote = bindings[expr.quote_var]
            rec_at = bindings[expr.at_var]

            year = _require_fy_int(rec_at, who="FX_LOOKUP_AT(at_var)")
            base = expr.base if expr.base is not None else rec_base.unit
            quote = expr.quote if expr.quote is not None else rec_quote.unit
            return _compile_fx_lookup_common(
                expr_key=id(expr), year=year, base=base, quote=quote
            )

        if isinstance(expr, Gt):
            return emit("GT", a=compile_expr(expr.left), b=compile_expr(expr.right))

        if isinstance(expr, Lt):
            return emit("LT", a=compile_expr(expr.left), b=compile_expr(expr.right))

        if isinstance(expr, Eq):
            return emit("EQ", a=compile_expr(expr.left), b=compile_expr(expr.right))

        raise TypeError(f"Unknown expr type: {type(expr)}")

    output_ref = compile_expr(spec.ast)
    dag = {"nodes": nodes, "output": output_ref}

    snippets_by_id = load_snippets(Path(benchmark_def.snippets_dir))
    capsule_context = []
    seen = set()
    for sid in snippet_ids:
        if sid in seen:
            continue
        seen.add(sid)
        snippet = snippets_by_id[sid]
        capsule_context.append(
            {
                "snippet_id": snippet["snippet_id"],
                "text": snippet["text"],
                **({"source": snippet["source"]} if "source" in snippet else {}),
            }
        )

    oracle_ctx = make_oracle_context(bindings, lookup_map)
    capsule = {
        "qid": f"{spec.template_id}|compile",
        "context": {"snippets": capsule_context},
    }
    result = execute_dag(
        dag,
        benchmark_def,
        "oracle",
        provider_ctx=None,
        oracle_ctx=oracle_ctx,
        capsule=capsule,
        cache={},
    )
    answer = result["output"]
    operators = [n["op"] for n in nodes]

    return CompiledPlan(
        dag=dag,
        lookup_map=lookup_map,
        answer=answer,
        snippet_ids=snippet_ids,
        operators=operators,
        meta={"benchmark_id": benchmark_def.benchmark_id},
    )
