from __future__ import annotations

import random
from pathlib import Path
from typing import Any, Optional, Dict
from TRACE.generation.expr import (
    LookupQty,
    ConvertScale,
    ConvertScaleTo,
    Add,
    CpiLookup,
    FxLookup,
    FxLookupTo,
    FxLookupAt,  # <-- add
    Div,
    Mul,
    Const,
    Gt,
    Lt,
    Eq,
)


from TRACE.shared.io import read_json
from TRACE.generation.generation_types import (
    ExtractRecord,
    Bindings,
    Spec,
    CompiledPlan,
)


def _lookup_query_for(record: ExtractRecord) -> str:
    pk = record.period_kind
    pv = record.period_value
    return (
        f"Extract the fact for: company={record.company} label={record.label}; period={pk} {pv}. "
        f"Return a ModelFact with snippet_id, label, period, quantity."
    )


def _float_eq(a: float, b: float, *, tol: float = 1e-20) -> bool:
    return abs(float(a) - float(b)) <= tol


def _canon_year(x: Any) -> int:
    if isinstance(x, int):
        return x
    s = str(x).strip()
    if s.isdigit():
        return int(s)
    raise TypeError(f"Expected year int (or numeric string), got {x!r}")


def compile_spec(
    spec: Spec, bindings: Bindings, *, seed: Optional[int] = None
) -> CompiledPlan:
    rng = random.Random(seed)

    nodes: list[dict[str, Any]] = []
    lookup_map: dict[str, str] = {}
    snippet_ids: list[str] = []
    id_counter = 1

    # compile-time choices for gold eval
    chosen_target_scale: dict[int, float] = {}
    chosen_fx_pair: dict[int, tuple[str, str]] = {}

    table_cache: dict[str, dict[str, Any]] = {}

    # ---------------- IO helpers ----------------

    def _load_cpi_table(series_id: str) -> dict[str, Any]:
        if series_id in table_cache:
            return table_cache[series_id]
        if series_id == "CPI_US_CPIU":
            p = Path("data") / "tables" / "cpi_us_cpiu.json"
        else:
            raise ValueError(f"Unknown CPI series_id: {series_id}")
        tbl = read_json(p)
        if tbl.get("series_id") != series_id:
            raise ValueError("CPI table series_id mismatch")
        table_cache[series_id] = tbl
        return tbl

    def _load_fx_table(series_id: str) -> dict[str, Any]:
        if series_id in table_cache:
            return table_cache[series_id]
        p = Path("data") / "tables" / "fx" / f"{series_id}.json"
        tbl = read_json(p)
        if tbl.get("series_id") != series_id:
            raise ValueError("FX table series_id mismatch")
        table_cache[series_id] = tbl
        return tbl

    # ---------------- DAG building helpers ----------------

    def new_id() -> str:
        nonlocal id_counter
        nid = f"n{id_counter}"
        id_counter += 1
        return nid

    def emit(op: str, **args: Any) -> str:
        """Append a node and return its ref string."""
        nid = new_id()
        nodes.append({"id": nid, "op": op, "args": args})
        return f"ref:{nid}"

    # ---------------- FX helpers (compile-time) ----------------

    def _require_fy_int(rec: ExtractRecord, *, who: str) -> int:
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
        """Implements your current FxLookup quote selection rules."""
        if expr_quote is not None:
            return expr_quote

        if expr_quote_in is not None:
            candidates = [q for q in expr_quote_in if q != base]
        else:
            from TRACE.generation.specs.common import fx_quotes_for_base

            candidates = list(fx_quotes_for_base(base))

        if not candidates:
            raise ValueError(f"No FX quote candidates for base={base!r}")
        return rng.choice(candidates)

    def _compile_fx_lookup_common(
        *,
        expr_key: int,
        year: int,
        base: str,
        quote: str,
    ) -> str:
        """Emit FX_LOOKUP node and record chosen pair for gold eval."""
        chosen_fx_pair[expr_key] = (base, quote)
        series_id = _fx_series_id(base, quote)
        return emit("FX_LOOKUP", series_id=series_id, year=year)

    # ---------------- Scale helpers ----------------

    def _emit_convert_scale(inner_ref: str, target_scale: float) -> str:
        return emit("CONVERT_SCALE", q=inner_ref, target_scale=float(target_scale))

    def _apply_scale(q: Dict[str, Any], target_scale: float) -> Dict[str, Any]:
        target_scale = float(target_scale)
        if float(q.get("scale")) == target_scale:
            return q
        v = float(q["value"])
        src = float(q["scale"])
        new_v = (v * src) / target_scale
        return {**q, "value": new_v, "scale": target_scale}

    # ---------------- compile AST -> DAG ----------------

    def compile_expr(expr) -> str:
        if isinstance(expr, LookupQty):
            rec = bindings[expr.var_name]

            # explicit two-node expansion preserved
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
                raise KeyError(
                    f"ConvertScaleTo to_var not in bindings: {expr.to_var!r}"
                )
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
                expr_key=id(expr),
                year=year,
                base=base,
                quote=quote,
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
                expr_key=id(expr),
                year=y_from,
                base=base,
                quote=quote,
            )

        if isinstance(expr, FxLookupAt):
            rec_base = bindings[expr.base_var]
            rec_quote = bindings[expr.quote_var]
            rec_at = bindings[expr.at_var]

            year = _require_fy_int(rec_at, who="FX_LOOKUP_AT(at_var)")

            base = expr.base if expr.base is not None else rec_base.unit
            quote = expr.quote if expr.quote is not None else rec_quote.unit
            return _compile_fx_lookup_common(
                expr_key=id(expr),
                year=year,
                base=base,
                quote=quote,
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

    # ---------------- gold eval (Quantity dicts) ----------------

    def q_scalar(v: float) -> Dict[str, Any]:
        return {"value": float(v), "unit": "", "scale": 1, "type": "scalar"}

    def is_scalar(q: Dict[str, Any]) -> bool:
        return q.get("type") == "scalar" and q.get("unit") == "" and q.get("scale") == 1

    def is_money(q: Dict[str, Any]) -> bool:
        return (
            q.get("type") == "money"
            and isinstance(q.get("unit"), str)
            and isinstance(q.get("scale"), (int, float))
        )

    def attach_period(q: Dict[str, Any], pk: str, pv: Any) -> Dict[str, Any]:
        return {**q, "_period_kind": pk, "_period_value": pv}

    def get_period(q: Dict[str, Any]) -> Optional[tuple[str, Any]]:
        pk = q.get("_period_kind")
        pv = q.get("_period_value")
        if pk is None or pv is None:
            return None
        return str(pk), pv

    def fx_rate_quantity(series_id: str, year: int) -> Dict[str, Any]:
        """Single source of truth for FX rate quantity shape."""
        tbl = _load_fx_table(series_id)
        rate = tbl.get("rate_by_year", {}).get(str(year))
        if rate is None:
            raise ValueError(f"FX year not found: {year} for {series_id}")

        frm = tbl.get("from")
        to = tbl.get("to")
        if not isinstance(frm, str) or not isinstance(to, str):
            raise ValueError("FX table missing from/to currency strings")

        return {
            "value": float(rate),
            "unit": "fx_rate",
            "scale": 1,
            "type": "rate",
            "from": {"currency": frm},
            "to": {"currency": to},
            "at": {"year": year},
            "series_id": series_id,
        }

    def eval_expr(expr) -> Dict[str, Any]:
        if isinstance(expr, LookupQty):
            r = bindings[expr.var_name]
            q = {
                "value": float(r.value),
                "unit": r.unit,
                "scale": float(r.scale),
                "type": r.qtype,
            }
            return attach_period(q, r.period_kind, r.period_value)

        if isinstance(expr, ConvertScale):
            q = eval_expr(expr.expr)
            target_scale = float(chosen_target_scale[id(expr)])
            return _apply_scale(q, target_scale)

        if isinstance(expr, ConvertScaleTo):
            q = eval_expr(expr.expr)
            target_scale = float(bindings[expr.to_var].scale)
            return _apply_scale(q, target_scale)

        if isinstance(expr, Add):
            a = eval_expr(expr.left)
            b = eval_expr(expr.right)
            if a.get("type") != b.get("type"):
                raise ValueError("ADD type mismatch")
            if a.get("unit") != b.get("unit"):
                raise ValueError("ADD unit mismatch")
            if a.get("scale") != b.get("scale"):
                raise ValueError("ADD scale mismatch")
            return {
                "value": float(a["value"]) + float(b["value"]),
                "unit": a["unit"],
                "scale": a["scale"],
                "type": a["type"],
            }

        if isinstance(expr, Const):
            return q_scalar(float(expr.value))

        if isinstance(expr, Div):
            a = eval_expr(expr.left)
            b = eval_expr(expr.right)
            denom = float(b["value"])
            if denom == 0.0:
                raise ValueError("DIV by zero")

            if (
                a.get("type") == "rate"
                and a.get("unit") == "percent"
                and a.get("scale") == 1
                and is_scalar(b)
            ):
                return q_scalar(float(a["value"]) / denom)

            if is_scalar(a) and is_scalar(b):
                return q_scalar(float(a["value"]) / denom)

            if is_money(a) and is_money(b):
                if a.get("unit") != b.get("unit"):
                    raise ValueError("DIV money currency mismatch")
                if a.get("scale") != b.get("scale"):
                    raise ValueError("DIV money scale mismatch")
                return q_scalar(float(a["value"]) / denom)

            raise ValueError("Unsupported DIV types")

        if isinstance(expr, CpiLookup):
            rec_from = bindings[expr.from_var]
            rec_to = bindings[expr.to_var]
            if rec_from.period_kind != "FY" or rec_to.period_kind != "FY":
                raise TypeError("CpiLookup expects FY on both bound vars")

            y_from = _canon_year(rec_from.period_value)
            y_to = _canon_year(rec_to.period_value)

            tbl = _load_cpi_table(expr.series_id)
            idx = tbl.get("index_by_year", {})
            a = idx.get(str(y_to))
            b = idx.get(str(y_from))
            if a is None or b is None:
                raise ValueError("CPI year not found")
            b = float(b)
            if b == 0.0:
                raise ValueError("CPI denom zero")

            return {
                "value": float(a) / b,
                "unit": "cpi_rate",
                "scale": 1,
                "type": "rate",
                "from": {"year": y_from},
                "to": {"year": y_to},
                "series_id": expr.series_id,
            }

        if isinstance(expr, FxLookup):
            rec = bindings[expr.var_name]
            year = _canon_year(rec.period_value)
            base, quote = chosen_fx_pair.get(id(expr), (expr.base, expr.quote))
            if base is None:
                base = rec.unit
            if quote is None:
                raise RuntimeError("FxLookup quote was not chosen during compile_expr")
            series_id = _fx_series_id(base, quote)
            return fx_rate_quantity(series_id, year)

        if isinstance(expr, FxLookupAt):
            rec_at = bindings[expr.at_var]
            year = _canon_year(rec_at.period_value)
            rec_base = bindings[expr.base_var]
            rec_quote = bindings[expr.quote_var]
            base = expr.base if expr.base is not None else rec_base.unit
            quote = expr.quote if expr.quote is not None else rec_quote.unit
            series_id = _fx_series_id(base, quote)
            return fx_rate_quantity(series_id, year)

        if isinstance(expr, FxLookupTo):
            rec_from = bindings[expr.from_var]
            rec_to = bindings[expr.to_var]
            y_from = _canon_year(rec_from.period_value)
            y_to = _canon_year(rec_to.period_value)
            if y_from != y_to:
                raise ValueError("FxLookupTo requires same FY year on both vars")
            base = expr.base if expr.base is not None else rec_from.unit
            quote = rec_to.unit
            series_id = _fx_series_id(base, quote)
            return fx_rate_quantity(series_id, y_from)

        if isinstance(expr, Mul):
            a = eval_expr(expr.left)
            b = eval_expr(expr.right)

            if is_scalar(a) and is_scalar(b):
                return q_scalar(float(a["value"]) * float(b["value"]))

            if is_money(a) and is_scalar(b):
                return {**a, "value": float(a["value"]) * float(b["value"])}
            if is_scalar(a) and is_money(b):
                return {**b, "value": float(a["value"]) * float(b["value"])}

            if (
                is_money(a)
                and b.get("type") == "rate"
                and b.get("unit") == "fx_rate"
                and b.get("scale") == 1
            ):
                frm = (b.get("from") or {}).get("currency")
                to = (b.get("to") or {}).get("currency")
                if a.get("unit") != frm:
                    raise ValueError("FX rate from.currency must match money unit")
                return {**a, "value": float(a["value"]) * float(b["value"]), "unit": to}

            if (
                is_money(a)
                and b.get("type") == "rate"
                and b.get("unit") == "cpi_rate"
                and b.get("scale") == 1
            ):
                fy = (b.get("from") or {}).get("year")
                ap = get_period(a)
                if ap is None:
                    raise ValueError(
                        "CPI adjustment requires money to carry FY provenance"
                    )
                pk, pv = ap
                if pk != "FY" or not isinstance(pv, int):
                    raise ValueError(
                        "CPI adjustment requires FY int provenance on money"
                    )
                if pv != fy:
                    raise ValueError("CPI from_year must match money FY year")
                return {**a, "value": float(a["value"]) * float(b["value"])}

            raise ValueError("Unsupported MUL types")

        if isinstance(expr, Gt):
            a = eval_expr(expr.left)
            b = eval_expr(expr.right)
            if (a.get("unit"), a.get("scale"), a.get("type")) != (
                b.get("unit"),
                b.get("scale"),
                b.get("type"),
            ):
                raise ValueError("GT type/unit/scale mismatch")
            return {
                "value": float(a["value"]) > float(b["value"]),
                "unit": "bool",
                "scale": 1,
                "type": "bool",
            }

        if isinstance(expr, Lt):
            a = eval_expr(expr.left)
            b = eval_expr(expr.right)
            if (a.get("unit"), a.get("scale"), a.get("type")) != (
                b.get("unit"),
                b.get("scale"),
                b.get("type"),
            ):
                raise ValueError("LT type/unit/scale mismatch")
            return {
                "value": float(a["value"]) < float(b["value"]),
                "unit": "bool",
                "scale": 1,
                "type": "bool",
            }

        if isinstance(expr, Eq):
            a = eval_expr(expr.left)
            b = eval_expr(expr.right)
            if (a.get("unit"), a.get("scale"), a.get("type")) != (
                b.get("unit"),
                b.get("scale"),
                b.get("type"),
            ):
                raise ValueError("EQ type/unit/scale mismatch")

            av = a.get("value")
            bv = b.get("value")
            if isinstance(av, (int, float)) and isinstance(bv, (int, float)):
                return {
                    "value": _float_eq(float(av), float(bv)),
                    "unit": "bool",
                    "scale": 1,
                    "type": "bool",
                }
            return {"value": av == bv, "unit": "bool", "scale": 1, "type": "bool"}

        raise TypeError(f"Unknown expr type: {type(expr)}")

    answer = eval_expr(spec.ast)
    operators = [n["op"] for n in nodes]

    return CompiledPlan(
        dag=dag,
        lookup_map=lookup_map,
        answer=answer,
        snippet_ids=snippet_ids,
        operators=operators,
        meta={},
    )
