# reason_bench/generation/specs/b0_bool.py
from __future__ import annotations

from typing import List, Optional

from TRACE.generation.expr import (
    Gt,
    Lt,
    Eq,
    LookupQty,
    Mul,
    ConvertScale,
    CpiLookup,
    FxLookupTo,
    FxLookupAt,
)
from TRACE.generation.generation_types import (
    Bindings,
    CompiledPlan,
    Constraint,
    DifferentCompany,
    DifferentExtraction,
    DifferentPeriod,
    SameCompany,
    SameLabel,
    SamePeriod,
    SameScale,
    SameUnit,
    Spec,
    VarSpec,
    DifferentUnit,
)
from benchmarks.trace_ufr.templates.common import (
    with_instr,
    label_renderer,
    period_renderer,
    scale_renderer,
    dag_arg_single,
    DEFAULT_SCALES,
)

B0_GT = "B0_GT"
B0_LT = "B0_LT"
B0_EQ = "B0_EQ"

B0_CMP_SCALE = "B0_CMP_SCALE"
B0_CMP_FX = "B0_CMP_FX"
B0_CMP_CPI = "B0_CMP_CPI"
B0_CMP_FX_CPI = "B0_CMP_FX_CPI"


def _numeric_vs(period_kind_in: Optional[List[str]] = None) -> VarSpec:
    # keep broad: allow money/count/rate etc if you want
    return VarSpec(period_kind_in=period_kind_in)


def b0_cmp_spec(
    *,
    template_id: str,
    ast,
    render_question,
    constraints: List[Constraint],
    a_name: str = "A",
    b_name: str = "B",
    period_kind_in: Optional[List[str]] = None,
    vs_a: Optional[VarSpec] = None,
    vs_b: Optional[VarSpec] = None,
) -> Spec:
    if vs_a is None:
        vs_a = _numeric_vs(period_kind_in=period_kind_in)
    if vs_b is None:
        vs_b = _numeric_vs(period_kind_in=period_kind_in)

    return Spec(
        template_id=template_id,
        vars={a_name: vs_a, b_name: vs_b},
        ast=ast,
        render_question=render_question,
        constraints=constraints,
        compile_opts={},
    )


def _render_cmp(bindings: Bindings, _: CompiledPlan, *, op_word: str) -> str:
    a, b = bindings["A"], bindings["B"]
    return with_instr(
        f"Is {label_renderer(a.label)} for {a.company} in {period_renderer(a)} {op_word} "
        f"{label_renderer(b.label)} for {b.company} in {period_renderer(b)}?"
    )


# -----------------------------------------------------------------------------
# Base (your original)
# -----------------------------------------------------------------------------

COMMON_CONSTRAINTS: List[Constraint] = [
    SameCompany("A", "B"),
    SameLabel("A", "B"),
    SameUnit("A", "B"),
    SameScale("A", "B"),
    DifferentPeriod("A", "B"),
    DifferentExtraction("A", "B"),
]

B0_GT__SAME_COMPANY_SAME_LABEL_DIFF_PERIOD = b0_cmp_spec(
    template_id=f"{B0_GT}__SAME_COMPANY_SAME_LABEL_DIFF_PERIOD",
    ast=Gt(LookupQty("A"), LookupQty("B")),
    render_question=lambda b, c: _render_cmp(b, c, op_word="greater than"),
    constraints=COMMON_CONSTRAINTS,
)

B0_LT__SAME_COMPANY_SAME_LABEL_DIFF_PERIOD = b0_cmp_spec(
    template_id=f"{B0_LT}__SAME_COMPANY_SAME_LABEL_DIFF_PERIOD",
    ast=Lt(LookupQty("A"), LookupQty("B")),
    render_question=lambda b, c: _render_cmp(b, c, op_word="less than"),
    constraints=COMMON_CONSTRAINTS,
)

B0_EQ__SAME_COMPANY_SAME_LABEL_DIFF_PERIOD = b0_cmp_spec(
    template_id=f"{B0_EQ}__SAME_COMPANY_SAME_LABEL_DIFF_PERIOD",
    ast=Eq(LookupQty("A"), LookupQty("B")),
    render_question=lambda b, c: _render_cmp(b, c, op_word="equal to"),
    constraints=COMMON_CONSTRAINTS,
)

# -----------------------------------------------------------------------------
# SCALE: convert BOTH A and B to the same (compiler-chosen) target scale, then compare
# This avoids “which side do we convert?” and matches your L0 ConvertScale pattern.
# -----------------------------------------------------------------------------


def _render_cmp_scale(
    bindings: Bindings, compiled: CompiledPlan, *, op_word: str
) -> str:
    a, b = bindings["A"], bindings["B"]
    ts = dag_arg_single(compiled, op="CONVERT_SCALE", arg="target_scale")
    ts_txt = scale_renderer(ts)
    mid = f"{ts_txt} " if ts_txt else ""
    return with_instr(
        f"Is {label_renderer(a.label)} for {a.company} in {period_renderer(a)} {op_word} "
        f"{label_renderer(b.label)} for {b.company} in {period_renderer(b)}, "
        f"after expressing both values in {mid}{a.unit}?"
    )


B0_SCALE_CONSTRAINTS: List[Constraint] = [
    DifferentCompany("A", "B"),
    SameLabel("A", "B"),
    SameUnit("A", "B"),
    SamePeriod("A", "B"),
    # DifferentExtraction("A", "B"),
    # NOTE: we *want* different scale often, but don't require it—compiler may choose target anyway.
]

B0_GT__SCALE__BOTH_TO_TARGET_THEN_CMP = b0_cmp_spec(
    template_id=f"{B0_GT}__{B0_CMP_SCALE}__BOTH_TO_TARGET_THEN_CMP",
    ast=Gt(
        ConvertScale(
            expr=LookupQty("A"), target_scale_in=tuple(float(x) for x in DEFAULT_SCALES)
        ),
        ConvertScale(
            expr=LookupQty("B"), target_scale_in=tuple(float(x) for x in DEFAULT_SCALES)
        ),
    ),
    render_question=lambda b, c: _render_cmp_scale(b, c, op_word="greater than"),
    constraints=B0_SCALE_CONSTRAINTS,
)

B0_LT__SCALE__BOTH_TO_TARGET_THEN_CMP = b0_cmp_spec(
    template_id=f"{B0_LT}__{B0_CMP_SCALE}__BOTH_TO_TARGET_THEN_CMP",
    ast=Lt(
        ConvertScale(
            expr=LookupQty("A"), target_scale_in=tuple(float(x) for x in DEFAULT_SCALES)
        ),
        ConvertScale(
            expr=LookupQty("B"), target_scale_in=tuple(float(x) for x in DEFAULT_SCALES)
        ),
    ),
    render_question=lambda b, c: _render_cmp_scale(b, c, op_word="less than"),
    constraints=B0_SCALE_CONSTRAINTS,
)

B0_EQ__SCALE__BOTH_TO_TARGET_THEN_CMP = b0_cmp_spec(
    template_id=f"{B0_EQ}__{B0_CMP_SCALE}__BOTH_TO_TARGET_THEN_CMP",
    ast=Eq(
        ConvertScale(
            expr=LookupQty("A"), target_scale_in=tuple(float(x) for x in DEFAULT_SCALES)
        ),
        ConvertScale(
            expr=LookupQty("B"), target_scale_in=tuple(float(x) for x in DEFAULT_SCALES)
        ),
    ),
    render_question=lambda b, c: _render_cmp_scale(b, c, op_word="equal to"),
    constraints=B0_SCALE_CONSTRAINTS,
)

# -----------------------------------------------------------------------------
# FX: convert A -> B currency, then compare (FY only)
# -----------------------------------------------------------------------------


def _render_cmp_fx(bindings: Bindings, _: CompiledPlan, *, op_word: str) -> str:
    a, b = bindings["A"], bindings["B"]
    return with_instr(
        f"Is {label_renderer(a.label)} for {a.company} in {period_renderer(a)} {op_word} "
        f"{label_renderer(b.label)} for {b.company} in {period_renderer(b)}, "
        f"after converting A from {a.unit} to {b.unit} using the FY exchange rate?"
    )


B0_FX_CONSTRAINTS: List[Constraint] = [
    DifferentCompany("A", "B"),
    SameLabel("A", "B"),
    SameScale("A", "B"),
    SamePeriod("A", "B"),
    DifferentUnit("A", "B"),
    DifferentExtraction("A", "B"),
]

B0_GT__FX__A_TO_B_THEN_CMP = b0_cmp_spec(
    template_id=f"{B0_GT}__{B0_CMP_FX}__A_TO_B_THEN_CMP",
    ast=Gt(Mul(LookupQty("A"), FxLookupTo(from_var="A", to_var="B")), LookupQty("B")),
    render_question=lambda b, c: _render_cmp_fx(b, c, op_word="greater than"),
    constraints=B0_FX_CONSTRAINTS,
    period_kind_in=["FY"],
)

B0_LT__FX__A_TO_B_THEN_CMP = b0_cmp_spec(
    template_id=f"{B0_LT}__{B0_CMP_FX}__A_TO_B_THEN_CMP",
    ast=Lt(Mul(LookupQty("A"), FxLookupTo(from_var="A", to_var="B")), LookupQty("B")),
    render_question=lambda b, c: _render_cmp_fx(b, c, op_word="less than"),
    constraints=B0_FX_CONSTRAINTS,
    period_kind_in=["FY"],
)

B0_EQ__FX__A_TO_B_THEN_CMP = b0_cmp_spec(
    template_id=f"{B0_EQ}__{B0_CMP_FX}__A_TO_B_THEN_CMP",
    ast=Eq(Mul(LookupQty("A"), FxLookupTo(from_var="A", to_var="B")), LookupQty("B")),
    render_question=lambda b, c: _render_cmp_fx(b, c, op_word="equal to"),
    constraints=B0_FX_CONSTRAINTS,
    period_kind_in=["FY"],
)

# -----------------------------------------------------------------------------
# CPI: CPI-adjust A to B period, then compare (USD-only)
# -----------------------------------------------------------------------------


def _render_cmp_cpi(bindings: Bindings, _: CompiledPlan, *, op_word: str) -> str:
    a, b = bindings["A"], bindings["B"]
    return with_instr(
        f"Is {label_renderer(a.label)} for {a.company} in {period_renderer(a)} {op_word} "
        f"{label_renderer(b.label)} for {b.company} in {period_renderer(b)}, "
        f"after inflation-adjusting A to the price level of {period_renderer(b)} using CPI-U?"
    )


B0_CPI_CONSTRAINTS: List[Constraint] = [
    SameCompany("A", "B"),
    SameLabel("A", "B"),
    SameUnit("A", "B"),
    SameScale("A", "B"),
    DifferentPeriod("A", "B"),
    DifferentExtraction("A", "B"),
]

USD_MONEY_FY = VarSpec(qtype_in=["money"], unit_in=["USD"], period_kind_in=["FY"])

B0_GT__CPI__A_TO_B_THEN_CMP = b0_cmp_spec(
    template_id=f"{B0_GT}__{B0_CMP_CPI}__A_TO_B_THEN_CMP",
    ast=Gt(
        Mul(
            LookupQty("A"), CpiLookup(from_var="A", to_var="B", series_id="CPI_US_CPIU")
        ),
        LookupQty("B"),
    ),
    render_question=lambda b, c: _render_cmp_cpi(b, c, op_word="greater than"),
    constraints=B0_CPI_CONSTRAINTS,
    vs_a=USD_MONEY_FY,
    vs_b=USD_MONEY_FY,
)

B0_LT__CPI__A_TO_B_THEN_CMP = b0_cmp_spec(
    template_id=f"{B0_LT}__{B0_CMP_CPI}__A_TO_B_THEN_CMP",
    ast=Lt(
        Mul(
            LookupQty("A"), CpiLookup(from_var="A", to_var="B", series_id="CPI_US_CPIU")
        ),
        LookupQty("B"),
    ),
    render_question=lambda b, c: _render_cmp_cpi(b, c, op_word="less than"),
    constraints=B0_CPI_CONSTRAINTS,
    vs_a=USD_MONEY_FY,
    vs_b=USD_MONEY_FY,
)

B0_EQ__CPI__A_TO_B_THEN_CMP = b0_cmp_spec(
    template_id=f"{B0_EQ}__{B0_CMP_CPI}__A_TO_B_THEN_CMP",
    ast=Eq(
        Mul(
            LookupQty("A"), CpiLookup(from_var="A", to_var="B", series_id="CPI_US_CPIU")
        ),
        LookupQty("B"),
    ),
    render_question=lambda b, c: _render_cmp_cpi(b, c, op_word="equal to"),
    constraints=B0_CPI_CONSTRAINTS,
    vs_a=USD_MONEY_FY,
    vs_b=USD_MONEY_FY,
)

# -----------------------------------------------------------------------------
# FX + CPI: convert A at B year to USD (B forced USD), CPI-adjust to B, then compare
# -----------------------------------------------------------------------------


def _render_cmp_fx_cpi(bindings: Bindings, _: CompiledPlan, *, op_word: str) -> str:
    a, b = bindings["A"], bindings["B"]
    return with_instr(
        f"Is {label_renderer(a.label)} for {a.company} in {period_renderer(a)} {op_word} "
        f"{label_renderer(b.label)} for {b.company} in {period_renderer(b)}, "
        f"after converting A to USD using the FY exchange rate at {period_renderer(b)} and "
        f"inflation-adjusting to {period_renderer(b)} using CPI-U?"
    )


B0_FX_CPI_CONSTRAINTS: List[Constraint] = [
    DifferentCompany("A", "B"),
    SameLabel("A", "B"),
    SameScale("A", "B"),
    DifferentUnit("A", "B"),
    DifferentPeriod("A", "B"),
    DifferentExtraction("A", "B"),
]

B_ANY_MONEY_FY_USD = VarSpec(qtype_in=["money"], unit_in=["USD"], period_kind_in=["FY"])
A_ANY_MONEY_FY = VarSpec(qtype_in=["money"], period_kind_in=["FY"])

A_fx_at_b_then_cpi = Mul(
    Mul(LookupQty("A"), FxLookupAt(base_var="A", at_var="B", quote_var="B")),
    CpiLookup(from_var="A", to_var="B", series_id="CPI_US_CPIU"),
)

B0_GT__FX_CPI__A_TO_USD_THEN_CPI_THEN_CMP = b0_cmp_spec(
    template_id=f"{B0_GT}__{B0_CMP_FX_CPI}__A_TO_USD_THEN_CPI_THEN_CMP",
    ast=Gt(A_fx_at_b_then_cpi, LookupQty("B")),
    render_question=lambda b, c: _render_cmp_fx_cpi(b, c, op_word="greater than"),
    constraints=B0_FX_CPI_CONSTRAINTS,
    vs_a=A_ANY_MONEY_FY,
    vs_b=B_ANY_MONEY_FY_USD,
)

B0_LT__FX_CPI__A_TO_USD_THEN_CPI_THEN_CMP = b0_cmp_spec(
    template_id=f"{B0_LT}__{B0_CMP_FX_CPI}__A_TO_USD_THEN_CPI_THEN_CMP",
    ast=Lt(A_fx_at_b_then_cpi, LookupQty("B")),
    render_question=lambda b, c: _render_cmp_fx_cpi(b, c, op_word="less than"),
    constraints=B0_FX_CPI_CONSTRAINTS,
    vs_a=A_ANY_MONEY_FY,
    vs_b=B_ANY_MONEY_FY_USD,
)

B0_EQ__FX_CPI__A_TO_USD_THEN_CPI_THEN_CMP = b0_cmp_spec(
    template_id=f"{B0_EQ}__{B0_CMP_FX_CPI}__A_TO_USD_THEN_CPI_THEN_CMP",
    ast=Eq(A_fx_at_b_then_cpi, LookupQty("B")),
    render_question=lambda b, c: _render_cmp_fx_cpi(b, c, op_word="equal to"),
    constraints=B0_FX_CPI_CONSTRAINTS,
    vs_a=A_ANY_MONEY_FY,
    vs_b=B_ANY_MONEY_FY_USD,
)

# -----------------------------------------------------------------------------
# Final registry
# -----------------------------------------------------------------------------

SPECS: list[Spec] = [
    # original
    B0_GT__SAME_COMPANY_SAME_LABEL_DIFF_PERIOD,
    B0_LT__SAME_COMPANY_SAME_LABEL_DIFF_PERIOD,
    B0_EQ__SAME_COMPANY_SAME_LABEL_DIFF_PERIOD,
    # scale
    B0_GT__SCALE__BOTH_TO_TARGET_THEN_CMP,
    B0_LT__SCALE__BOTH_TO_TARGET_THEN_CMP,
    B0_EQ__SCALE__BOTH_TO_TARGET_THEN_CMP,
    # fx
    B0_GT__FX__A_TO_B_THEN_CMP,
    B0_LT__FX__A_TO_B_THEN_CMP,
    B0_EQ__FX__A_TO_B_THEN_CMP,
    # cpi
    B0_GT__CPI__A_TO_B_THEN_CMP,
    B0_LT__CPI__A_TO_B_THEN_CMP,
    B0_EQ__CPI__A_TO_B_THEN_CMP,
    # fx + cpi
    B0_GT__FX_CPI__A_TO_USD_THEN_CPI_THEN_CMP,
    B0_LT__FX_CPI__A_TO_USD_THEN_CPI_THEN_CMP,
    B0_EQ__FX_CPI__A_TO_USD_THEN_CPI_THEN_CMP,
]
