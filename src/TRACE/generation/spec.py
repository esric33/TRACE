from __future__ import annotations

from typing import Callable, List, Optional, Sequence

from TRACE.generation.expr import (
    Add,
    ConvertScale,
    CpiLookup,
    LookupQty,
    Mul,
    Const,
    Div,
)
from TRACE.generation.generation_types import (
    Bindings,
    CompiledPlan,
    Constraint,
    DifferentCompany,
    DifferentExtraction,
    DifferentLabel,
    DifferentPeriod,
    ExtractRecord,
    NotInExtracts,
    SameCompany,
    SameLabel,
    SameMetricKey,
    SamePeriod,
    SameScale,
    SameUnit,
    Spec,
    VarSpec,
)

# -----------------------------------------------------------------------------
# Shared render helpers
# -----------------------------------------------------------------------------

RenderFn = Callable[[Bindings, CompiledPlan], str]

DEFAULT_SCALES: Sequence[float] = (
    1.0,
    1_000.0,
    1_000_000.0,
    1_000_000_000.0,
    1_000_000_000_000.0,
)


def label_renderer(label: str) -> str:
    return label.replace("_", " ").lower()


def period_renderer(r: ExtractRecord) -> str:
    return f"{r.period_kind} {r.period_value}"


def scale_renderer(scale: float) -> str:
    match float(scale):
        case 1.0:
            return ""  # base units (no prefix)
        case 1_000.0:
            return "thousands"
        case 1_000_000.0:
            return "millions"
        case 1_000_000_000.0:
            return "billions"
        case 1_000_000_000_000.0:
            return "trillions"
        case _:
            raise ValueError(f"Unknown scale: {scale}")


def _dag_arg_single(compiled: CompiledPlan, *, op: str, arg: str) -> float:
    """
    Helper for compile-time chosen args that live in the compiled DAG
    (e.g., CONVERT_SCALE.target_scale).
    """
    for n in compiled.dag["nodes"]:
        if n["op"] == op:
            return float(n["args"][arg])
    raise KeyError(f"Missing op={op} arg={arg} in compiled DAG")


# -----------------------------------------------------------------------------
# T0 — single lookup
# -----------------------------------------------------------------------------

T0_BASE_ID = "T0_LOOKUP"


def t0_base_spec(
    *,
    template_id: str,
    render_question: RenderFn,
    var_name: str = "A",
    qtype_in: Optional[List[str]] = None,
    unit_in: Optional[List[str]] = None,
    label_in: Optional[List[str]] = None,
    period_kind_in: Optional[List[str]] = None,
) -> Spec:
    vs = VarSpec(
        qtype_in=qtype_in,
        unit_in=unit_in,
        label_in=label_in,
        period_kind_in=period_kind_in,
    )
    return Spec(
        template_id=template_id,
        vars={var_name: vs},
        ast=LookupQty(var_name=var_name),
        render_question=render_question,
        constraints=[],
        compile_opts={},
    )


def _t0_render_default(bindings: Bindings, _: CompiledPlan) -> str:
    r = bindings["A"]
    return (
        f"What was {label_renderer(r.label)} for {r.company} in {period_renderer(r)}?"
    )


T0_LOOKUP = t0_base_spec(
    template_id=T0_BASE_ID,
    render_question=_t0_render_default,
)

# -----------------------------------------------------------------------------
# T1 — convert scale (compile-time choice of target scale)
# -----------------------------------------------------------------------------

T1_BASE_ID = "T1_CONVERT_SCALE"


def t1_base_spec(
    *,
    template_id: str,
    render_question: RenderFn,
    var_name: str = "A",
    target_scales: Sequence[float] = DEFAULT_SCALES,
    qtype_in: Optional[List[str]] = None,
    unit_in: Optional[List[str]] = None,
    label_in: Optional[List[str]] = None,
    period_kind_in: Optional[List[str]] = None,
    allow_noop: bool = True,
) -> Spec:
    if qtype_in is None:
        qtype_in = ["money"]

    vs = VarSpec(
        qtype_in=qtype_in,
        unit_in=unit_in,
        label_in=label_in,
        period_kind_in=period_kind_in,
    )

    return Spec(
        template_id=template_id,
        vars={var_name: vs},
        ast=ConvertScale(
            expr=LookupQty(var_name=var_name),
            target_scale_in=tuple(float(x) for x in target_scales),
        ),
        render_question=render_question,
        constraints=[],
        # compile-time policy: whether choosing target_scale == src_scale is allowed
        compile_opts={"t1_allow_noop": bool(allow_noop)},
    )


def _t1_render_default(bindings: Bindings, compiled: CompiledPlan) -> str:
    r = bindings["A"]
    company = r.company or r.snippet_id
    unit = r.unit

    ts = _dag_arg_single(compiled, op="CONVERT_SCALE", arg="target_scale")
    ts_txt = scale_renderer(ts)

    # "expressed in millions USD" (or "expressed in USD" if scale is 1.0)
    mid = f"{ts_txt} " if ts_txt else ""
    return (
        f"What was {label_renderer(r.label)} for {company} in {period_renderer(r)} "
        f"expressed in {mid}{unit}?"
    )


T1_CONVERT_SCALE_ALLOW_NOOP = t1_base_spec(
    template_id=f"{T1_BASE_ID}__ALLOW_NOOP",
    render_question=_t1_render_default,
    allow_noop=True,
)

T1_CONVERT_SCALE_FORCE_NON_NOOP = t1_base_spec(
    template_id=f"{T1_BASE_ID}__FORCE_NON_NOOP",
    render_question=_t1_render_default,
    allow_noop=False,
)

# -----------------------------------------------------------------------------
# T2 — add two quantities (same solving DAG; variants change constraints + phrasing)
# -----------------------------------------------------------------------------

T2_BASE_ID = "T2_ADD"


def t2_base_spec(
    *,
    template_id: str,
    constraints: List[Constraint],
    render_question: RenderFn,
    a_name: str = "A",
    b_name: str = "B",
    qtype_in: Optional[List[str]] = None,
    unit_in: Optional[List[str]] = None,
    label_in: Optional[List[str]] = None,
    period_kind_in: Optional[List[str]] = None,
) -> Spec:
    if qtype_in is None:
        qtype_in = ["money"]

    vs = VarSpec(
        qtype_in=qtype_in,
        unit_in=unit_in,
        label_in=label_in,
        period_kind_in=period_kind_in,
    )

    return Spec(
        template_id=template_id,
        vars={a_name: vs, b_name: vs},
        ast=Add(LookupQty(a_name), LookupQty(b_name)),
        render_question=render_question,
        constraints=constraints,
        compile_opts={},
    )


def _t2_render_same_company_diff_label(bindings: Bindings, _: CompiledPlan) -> str:
    a, b = bindings["A"], bindings["B"]
    return (
        f"What is the sum of {label_renderer(a.label)} and {label_renderer(b.label)} "
        f"for {a.company} in {period_renderer(a)}?"
    )


T2_ADD__SAME_COMPANY_DIFF_LABEL = t2_base_spec(
    template_id=f"{T2_BASE_ID}__SAME_COMPANY_DIFF_LABEL",
    constraints=[
        SameCompany("A", "B"),
        SamePeriod("A", "B"),
        SameUnit("A", "B"),
        SameScale("A", "B"),
        DifferentLabel("A", "B"),
        DifferentExtraction("A", "B"),
    ],
    render_question=_t2_render_same_company_diff_label,
)


def _t2_render_diff_company_same_label(bindings: Bindings, _: CompiledPlan) -> str:
    a, b = bindings["A"], bindings["B"]
    return (
        f"What is the sum of {label_renderer(a.label)} for {a.company} and "
        f"{label_renderer(b.label)} for {b.company} in {period_renderer(a)}?"
    )


T2_ADD__DIFF_COMPANY_SAME_LABEL = t2_base_spec(
    template_id=f"{T2_BASE_ID}__DIFF_COMPANY_SAME_LABEL",
    constraints=[
        DifferentCompany("A", "B"),
        SamePeriod("A", "B"),
        SameLabel("A", "B"),
        SameUnit("A", "B"),
        SameScale("A", "B"),
        DifferentExtraction("A", "B"),
    ],
    render_question=_t2_render_diff_company_same_label,
)


# -----------------------------------------------------------------------------
# T3 — project next FY value from amount * (1 + growth_rate)
# -----------------------------------------------------------------------------

T3_BASE_ID = "T3_PROJECT_NEXT_FY"


def t3_base_spec(
    *,
    template_id: str,
    render_question: RenderFn,
    a_name: str = "A",  # amount (e.g., revenue)
    g_name: str = "G",  # growth rate (percent)
    period_kind_in: Optional[List[str]] = None,
) -> Spec:
    # Restrict to FY by default, because NotInExtracts is FY-only for now.
    if period_kind_in is None:
        period_kind_in = ["FY"]

    vs_amount = VarSpec(
        period_kind_in=period_kind_in,
        metric_role_in=["amount"],
        # optional: qtype_in=["money"] if you only want money-only templates
    )

    vs_rate = VarSpec(
        period_kind_in=period_kind_in,
        metric_role_in=["rate"],
        unit_in=["percent"],
        qtype_in=["rate"],  # your extracts use quantity.type == "rate"
    )

    # A * (1 + (G / 100))
    ast = Mul(
        LookupQty(a_name),
        Add(
            Const(1.0),
            Div(LookupQty(g_name), Const(100.0)),
        ),
    )

    constraints: List[Constraint] = [
        SameCompany(a_name, g_name),
        SameMetricKey(a_name, g_name),
        SamePeriod(a_name, g_name),
        DifferentExtraction(a_name, g_name),
        # Target FY (base + 1) for same company+metric_key must NOT be present.
        NotInExtracts(
            company_from=a_name,
            metric_key_from=a_name,
            period_kind="FY",
            period_value_from=a_name,
            delta_years=+1,
        ),
    ]

    return Spec(
        template_id=template_id,
        vars={a_name: vs_amount, g_name: vs_rate},
        ast=ast,
        render_question=render_question,
        constraints=constraints,
        compile_opts={},
    )


def _t3_render_project_next_fy(bindings: Bindings, _: CompiledPlan) -> str:
    a = bindings["A"]
    g = bindings["G"]

    # FY assumed to be an int year in your dataset
    base_year = int(a.period_value)
    target_year = base_year + 1

    # Note: we DO NOT include the numeric values; the DAG must look them up.
    return (
        f"For {a.company}, if {label_renderer(g.label)} was the same in FY {base_year} "
        f"as reported, what would you expect {label_renderer(a.label)} to be in FY {target_year} "
        f"based on {label_renderer(a.label)} in FY {base_year}?"
    )


T3_PROJECT_NEXT_FY__A_TIMES_1_PLUS_G = t3_base_spec(
    template_id=f"{T3_BASE_ID}__A_TIMES_1_PLUS_G",
    render_question=_t3_render_project_next_fy,
)


# -----------------------------------------------------------------------------
# T4 — CPI-adjusted cross-year add: (A@year1 adjusted to year2) + (B@year2)
# -----------------------------------------------------------------------------

T4_BASE_ID = "T4_ADD_CPI_ADJUSTED"


def t4_base_spec(
    *,
    template_id: str,
    render_question: RenderFn,
    a_name: str = "A",
    b_name: str = "B",
    period_kind_in: Optional[List[str]] = None,
) -> Spec:
    if period_kind_in is None:
        period_kind_in = ["FY"]

    vs = VarSpec(
        qtype_in=["money"],
        period_kind_in=period_kind_in,
        metric_role_in=["amount"],
    )

    ast = Add(
        LookupQty(b_name),
        Mul(
            LookupQty(a_name),
            Div(
                CpiLookup(b_name, series_id="CPI_US_CPIU"),
                CpiLookup(a_name, series_id="CPI_US_CPIU"),
            ),
        ),
    )

    constraints: List[Constraint] = [
        SameCompany(a_name, b_name),
        SameLabel(a_name, b_name),
        SameUnit(a_name, b_name),
        SameScale(a_name, b_name),
        DifferentPeriod(a_name, b_name),
        DifferentExtraction(a_name, b_name),
    ]

    return Spec(
        template_id=template_id,
        vars={a_name: vs, b_name: vs},
        ast=ast,
        render_question=render_question,
        constraints=constraints,
        compile_opts={},
    )


def _t4_render_add_cpi_adjusted(bindings: Bindings, _: CompiledPlan) -> str:
    a, b = bindings["A"], bindings["B"]
    return (
        f"What is the sum of {label_renderer(a.label)} for {a.company} in {period_renderer(a)} "
        f"(adjusted for inflation to {period_renderer(b)} dollars using CPI-U) and "
        f"{label_renderer(b.label)} for {b.company} in {period_renderer(b)}?"
    )


T4_ADD_CPI_ADJUSTED__SAME_COMPANY_SAME_LABEL = t4_base_spec(
    template_id=f"{T4_BASE_ID}__SAME_COMPANY_SAME_LABEL",
    render_question=_t4_render_add_cpi_adjusted,
)


# -----------------------------------------------------------------------------
# Convenience registry (optional)
# -----------------------------------------------------------------------------

SPECS_BY_ID: dict[str, Spec] = {
    template.template_id: template
    for template in [
        T0_LOOKUP,
        T1_CONVERT_SCALE_ALLOW_NOOP,
        T1_CONVERT_SCALE_FORCE_NON_NOOP,
        T2_ADD__SAME_COMPANY_DIFF_LABEL,
        T2_ADD__DIFF_COMPANY_SAME_LABEL,
        T3_PROJECT_NEXT_FY__A_TIMES_1_PLUS_G,
        T4_ADD_CPI_ADJUSTED__SAME_COMPANY_SAME_LABEL,
    ]
}

SPECS_BY_FAMILY: dict[str, List[Spec]] = {
    "T0": [T0_LOOKUP],
    "T1": [T1_CONVERT_SCALE_ALLOW_NOOP, T1_CONVERT_SCALE_FORCE_NON_NOOP],
    "T2": [T2_ADD__SAME_COMPANY_DIFF_LABEL, T2_ADD__DIFF_COMPANY_SAME_LABEL],
    "T3": [T3_PROJECT_NEXT_FY__A_TIMES_1_PLUS_G],
    "T4": [T4_ADD_CPI_ADJUSTED__SAME_COMPANY_SAME_LABEL],
}
