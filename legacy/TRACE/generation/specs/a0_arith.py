# reason_bench/generation/specs/a0_arith.py
from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

from TRACE.generation.expr import (
    Add,
    FxLookupAt,
    Mul,
    Div,
    Const,
    LookupQty,
    CpiLookup,
    ConvertScale,
    ConvertScaleTo,
    FxLookup,
    FxLookupTo,
)
from TRACE.generation.generation_types import (
    Bindings,
    CompiledPlan,
    Constraint,
    DifferentCompany,
    DifferentExtraction,
    DifferentLabel,
    DifferentScale,
    DifferentPeriod,
    SameCompany,
    SameLabel,
    SameMetricKey,
    SamePeriod,
    SameScale,
    SameUnit,
    DifferentUnit,
    NotInExtracts,
    Spec,
    VarSpec,
)

from TRACE.generation.specs.common import (
    dag_arg_single,
    scale_renderer,
    with_instr,
    label_renderer,
    period_renderer,
    available_fx_pairs,
    dag_arg_fx_series,  # <-- add this
    DEFAULT_SCALES,
)

# -----------------------------------------------------------------------------
# A0 — arithmetic family
# -----------------------------------------------------------------------------

A0_ADD = "A0_ADD"
A0_PROJECT = "A0_PROJECT_NEXT_FY"
A0_ADD_CPI = "A0_ADD_CPI_ADJUSTED"

A0_FX_CONVERT = "A0_FX_CONVERT"
A0_ADD_FX = "A0_ADD_FX"
A0_ADD_FX_CPI = "A0_ADD_FX_CPI"
A0_ADD_FX_TO_THIRD = "A0_ADD_FX_TO_THIRD"

A0_ADD4 = "A0_ADD4"
A0_ADD4_NORM = "A0_ADD4_NORM_TO_A"

# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------


def _money_vs(
    period_kind_in: Optional[List[str]] = None,
    unit_in: Optional[List[str]] = None,
) -> VarSpec:
    return VarSpec(
        qtype_in=["money"],
        period_kind_in=period_kind_in,
        unit_in=unit_in,
        metric_role_in=["amount"],
    )


def _rate_vs(
    period_kind_in: Optional[List[str]] = None,
) -> VarSpec:
    return VarSpec(
        qtype_in=["rate"],
        period_kind_in=period_kind_in,
        metric_role_in=["rate"],
        unit_in=["percent"],
    )


# -----------------------------------------------------------------------------
# A0_ADD variants
# -----------------------------------------------------------------------------


def a0_add_spec(
    *,
    template_id: str,
    constraints: List[Constraint],
    render_question,
    a_name: str = "A",
    b_name: str = "B",
    period_kind_in: Optional[List[str]] = None,
) -> Spec:
    vs = _money_vs(period_kind_in=period_kind_in)
    return Spec(
        template_id=template_id,
        vars={a_name: vs, b_name: vs},
        ast=Add(LookupQty(a_name), LookupQty(b_name)),
        render_question=render_question,
        constraints=constraints,
        compile_opts={},
    )


def _render_add_same_company_diff_label(bindings: Bindings, _: CompiledPlan) -> str:
    a, b = bindings["A"], bindings["B"]
    return with_instr(
        f"What is the sum of {label_renderer(a.label)} and {label_renderer(b.label)} "
        f"for {a.company} in {period_renderer(a)}?"
    )


A0_ADD__SAME_COMPANY_DIFF_LABEL = a0_add_spec(
    template_id=f"{A0_ADD}__SAME_COMPANY_DIFF_LABEL",
    constraints=[
        SameCompany("A", "B"),
        SamePeriod("A", "B"),
        SameUnit("A", "B"),
        SameScale("A", "B"),
        DifferentLabel("A", "B"),
        DifferentExtraction("A", "B"),
    ],
    render_question=_render_add_same_company_diff_label,
)


def _render_add_diff_company_same_label(bindings: Bindings, _: CompiledPlan) -> str:
    a, b = bindings["A"], bindings["B"]
    return with_instr(
        f"What is the sum of {label_renderer(a.label)} for {a.company} and "
        f"{label_renderer(b.label)} for {b.company} in {period_renderer(a)}?"
    )


A0_ADD__DIFF_COMPANY_SAME_LABEL = a0_add_spec(
    template_id=f"{A0_ADD}__DIFF_COMPANY_SAME_LABEL",
    constraints=[
        DifferentCompany("A", "B"),
        SamePeriod("A", "B"),
        SameLabel("A", "B"),
        SameUnit("A", "B"),
        SameScale("A", "B"),
        DifferentExtraction("A", "B"),
    ],
    render_question=_render_add_diff_company_same_label,
)

# -----------------------------------------------------------------------------
# A0_PROJECT_NEXT_FY
# -----------------------------------------------------------------------------


def a0_project_next_fy_spec(
    *,
    template_id: str,
    render_question,
    a_name: str = "A",
    g_name: str = "G",
    period_kind_in: Optional[List[str]] = None,
) -> Spec:
    if period_kind_in is None:
        period_kind_in = ["FY"]

    vs_amount = _money_vs(period_kind_in=period_kind_in)
    vs_rate = _rate_vs(period_kind_in=period_kind_in)

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


def _render_project_next_fy(bindings: Bindings, _: CompiledPlan) -> str:
    a = bindings["A"]
    g = bindings["G"]
    base_year = int(a.period_value)
    target_year = base_year + 1
    return with_instr(
        f"For {a.company}, use FY {base_year} {label_renderer(a.label)} and FY {base_year} "
        f"{label_renderer(g.label)} to project FY {target_year} {label_renderer(a.label)}."
    )


A0_PROJECT_NEXT_FY__A_TIMES_1_PLUS_G = a0_project_next_fy_spec(
    template_id=f"{A0_PROJECT}__A_TIMES_1_PLUS_G",
    render_question=_render_project_next_fy,
)

# -----------------------------------------------------------------------------
# A0_ADD_CPI_ADJUSTED  (USD-only)
# -----------------------------------------------------------------------------


def a0_add_cpi_adjusted_spec(
    *,
    template_id: str,
    render_question,
    a_name: str = "A",
    b_name: str = "B",
    period_kind_in: Optional[List[str]] = None,
) -> Spec:
    if period_kind_in is None:
        period_kind_in = ["FY"]

    # CPI_US_CPIU => force USD to avoid nonsense
    vs_usd = _money_vs(period_kind_in=period_kind_in, unit_in=["USD"])

    ast = Add(
        LookupQty(b_name),
        Mul(
            LookupQty(a_name),
            CpiLookup(from_var=a_name, to_var=b_name, series_id="CPI_US_CPIU"),
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
        vars={a_name: vs_usd, b_name: vs_usd},
        ast=ast,
        render_question=render_question,
        constraints=constraints,
        compile_opts={},
    )


def _render_add_cpi_adjusted(bindings: Bindings, _: CompiledPlan) -> str:
    a, b = bindings["A"], bindings["B"]
    return with_instr(
        f"What is the sum of {label_renderer(a.label)} for {a.company} in {period_renderer(a)} "
        f"(inflation-adjusted to the price level of {period_renderer(b)} using CPI-U) and "
        f"{label_renderer(b.label)} for {b.company} in {period_renderer(b)}?"
    )


A0_ADD_CPI_ADJUSTED__SAME_COMPANY_SAME_LABEL = a0_add_cpi_adjusted_spec(
    template_id=f"{A0_ADD_CPI}__SAME_COMPANY_SAME_LABEL",
    render_question=_render_add_cpi_adjusted,
)

# -----------------------------------------------------------------------------
# FX specs
# -----------------------------------------------------------------------------

# ---- FX convert (unary) using quote_in (scale-like) ----
#
# This picks a quote currency at compile time from the domain,
# based on available FX tables for any base. We compute the set of all quote
# currencies present in fx tables and let the compiler pick one.
#
# Note: This does not guarantee “use B currency” — use FxLookupTo for that.

_ALL_FX_QUOTES: Tuple[str, ...] = tuple(sorted({q for _, q in available_fx_pairs()}))


def _render_fx_convert_quote_in(bindings: Bindings, compiled: CompiledPlan) -> str:
    a = bindings["A"]
    base, quote = dag_arg_fx_series(compiled)
    return with_instr(
        f"For {a.company}, convert FY {int(a.period_value)} {label_renderer(a.label)} "
        f"from {base} to {quote} using the FY exchange rate."
    )


A0_FX_CONVERT__QUOTE_IN = Spec(
    template_id=f"{A0_FX_CONVERT}__QUOTE_IN",
    vars={"A": _money_vs(period_kind_in=["FY"])},
    ast=Mul(
        LookupQty("A"),
        FxLookup(var_name="A", quote_in=_ALL_FX_QUOTES),
    ),
    render_question=_render_fx_convert_quote_in,
    constraints=[],
    compile_opts={},
)

# ---- FX convert A -> B currency (deterministic) ----


def _render_fx_convert_to_b(bindings: Bindings, _: CompiledPlan) -> str:
    a, b = bindings["A"], bindings["B"]
    return with_instr(
        f"For {a.company}, convert FY {int(a.period_value)} {label_renderer(a.label)} "
        f"from {a.unit} to {b.unit} using the FY exchange rate."
    )


A0_FX_CONVERT__TO_B_CURRENCY = Spec(
    template_id=f"{A0_FX_CONVERT}__TO_B_CURRENCY",
    vars={
        "A": _money_vs(period_kind_in=["FY"]),
        "B": _money_vs(period_kind_in=["FY"]),
    },
    ast=Mul(
        LookupQty("A"),
        FxLookupTo(from_var="A", to_var="B"),
    ),
    render_question=_render_fx_convert_to_b,
    constraints=[
        DifferentCompany("A", "B"),
        SamePeriod("A", "B"),
        DifferentUnit("A", "B"),
        DifferentExtraction("A", "B"),
    ],
    compile_opts={},
)

# ---- ADD after converting A -> B currency (deterministic) ----


def _render_add_fx_to_b(bindings: Bindings, _: CompiledPlan) -> str:
    a, b = bindings["A"], bindings["B"]
    return with_instr(
        f"What is the sum of {label_renderer(a.label)} for {a.company} in {period_renderer(a)}, "
        f"after converting it from {a.unit} to {b.unit} using the FY exchange rate, and "
        f"{label_renderer(b.label)} for {b.company} in {period_renderer(b)}?"
    )


A0_ADD_FX__A_TO_B_THEN_ADD = Spec(
    template_id=f"{A0_ADD_FX}__A_TO_B_THEN_ADD",
    vars={
        "A": _money_vs(period_kind_in=["FY"]),
        "B": _money_vs(period_kind_in=["FY"]),
    },
    ast=Add(
        Mul(LookupQty("A"), FxLookupTo(from_var="A", to_var="B")),
        LookupQty("B"),
    ),
    render_question=_render_add_fx_to_b,
    constraints=[
        DifferentCompany("A", "B"),
        SamePeriod("A", "B"),
        SameLabel("A", "B"),
        SameScale("A", "B"),
        DifferentUnit("A", "B"),
        DifferentExtraction("A", "B"),
    ],
    compile_opts={},
)

# ---- FX + CPI: convert A -> USD (via B), CPI adjust to B year, then add B (USD-only B) ----
#
# NOTE: economically coherent only because B is forced USD and CPI series is CPI_US_CPIU.


def _render_add_fx_cpi(bindings: Bindings, _: CompiledPlan) -> str:
    a, b = bindings["A"], bindings["B"]
    return with_instr(
        f"What is the sum of {label_renderer(a.label)} for {a.company} in {period_renderer(a)} "
        f"(converted from {a.unit} to USD using the FY exchange rate, then inflation-adjusted to "
        f"the price level of {period_renderer(b)} using CPI) and {label_renderer(b.label)} for "
        f"{b.company} in {period_renderer(b)}?"
    )


A0_ADD_FX_CPI__A_TO_USD_THEN_CPI_THEN_ADD = Spec(
    template_id=f"{A0_ADD_FX_CPI}__A_TO_USD_THEN_CPI_THEN_ADD",
    vars={
        "A": _money_vs(period_kind_in=["FY"]),
        "B": _money_vs(
            period_kind_in=["FY"], unit_in=["USD"]
        ),  # force USD anchor for CPI_U
    },
    ast=Add(
        LookupQty("B"),
        Mul(
            Mul(LookupQty("A"), FxLookupAt(base_var="A", at_var="A", quote_var="B")),
            CpiLookup(from_var="A", to_var="B"),
        ),
    ),
    render_question=_render_add_fx_cpi,
    constraints=[
        DifferentCompany("A", "B"),
        SameLabel("A", "B"),
        SameScale("A", "B"),
        DifferentUnit("A", "B"),
        DifferentPeriod("A", "B"),
        DifferentExtraction("A", "B"),
    ],
    compile_opts={},
)


def _render_add_fx_to_target(bindings: Bindings, compiled: CompiledPlan) -> str:
    a, b = bindings["A"], bindings["B"]
    base, quote = dag_arg_fx_series(
        compiled
    )  # reads first FX_LOOKUP; should be A->target
    # NOTE: we also convert B to the same quote; if you want to print quote once, that's fine.
    return with_instr(
        f"For {a.company} in {period_renderer(a)}, convert {label_renderer(a.label)} "
        f"(from {a.unit}) and {label_renderer(b.label)} (from {b.unit}) to {quote} using the FY "
        f"exchange rate, then add them."
    )


_USD_CONVERTIBLE_BASES: list[str] = list(
    sorted(
        base for base, quote in available_fx_pairs() if quote == "USD" and base != "USD"
    )
)

A0_ADD_FX_TO_THIRD__A_AND_B_TO_TARGET_THEN_ADD = Spec(
    template_id=f"{A0_ADD_FX_TO_THIRD}__A_AND_B_TO_TARGET_THEN_ADD",
    vars={
        "A": _money_vs(period_kind_in=["FY"], unit_in=_USD_CONVERTIBLE_BASES),
        "B": _money_vs(period_kind_in=["FY"], unit_in=_USD_CONVERTIBLE_BASES),
    },
    ast=Add(
        Mul(LookupQty("A"), FxLookup(var_name="A", quote_in=("USD",))),
        Mul(LookupQty("B"), FxLookup(var_name="B", quote_in=("USD",))),
    ),
    render_question=_render_add_fx_to_target,
    constraints=[
        DifferentCompany("A", "B"),
        SamePeriod("A", "B"),
        SameScale("A", "B"),
        DifferentUnit("A", "B"),
        DifferentLabel("A", "B"),
        DifferentExtraction("A", "B"),
    ],
    compile_opts={},
)


def a0_add4_spec(
    *,
    template_id: str,
    constraints: List[Constraint],
    render_question,
    var_names: List[str] = ["A", "B", "C", "D"],
    period_kind_in: Optional[List[str]] = None,
) -> Spec:
    vs = _money_vs(period_kind_in=period_kind_in)

    ast = Add(
        Add(
            Add(LookupQty(var_names[0]), LookupQty(var_names[1])),
            LookupQty(var_names[2]),
        ),
        LookupQty(var_names[3]),
    )

    return Spec(
        template_id=template_id,
        vars={vn: vs for vn in var_names},
        ast=ast,
        render_question=render_question,
        constraints=constraints,
        compile_opts={},
    )


def _render_add4(bindings: Bindings, _: CompiledPlan) -> str:
    a, b, c, d = bindings["A"], bindings["B"], bindings["C"], bindings["D"]
    return with_instr(
        f"What is the sum of {label_renderer(a.label)}, {label_renderer(b.label)}, "
        f"{label_renderer(c.label)}, and {label_renderer(d.label)} for {a.company} "
        f"in {period_renderer(a)}?"
    )


# Variant 1: same company/period/unit/scale; all labels different
A0_ADD4__SAME_COMPANY_DIFF_LABELS = a0_add4_spec(
    template_id=f"{A0_ADD4}__SAME_COMPANY_DIFF_LABELS",
    constraints=[
        SameCompany("A", "B"),
        SameCompany("A", "C"),
        SameCompany("A", "D"),
        SamePeriod("A", "B"),
        SamePeriod("A", "C"),
        SamePeriod("A", "D"),
        SameUnit("A", "B"),
        SameUnit("A", "C"),
        SameUnit("A", "D"),
        SameScale("A", "B"),
        SameScale("A", "C"),
        SameScale("A", "D"),
        DifferentLabel("A", "B"),
        DifferentLabel("A", "C"),
        DifferentLabel("A", "D"),
        DifferentLabel("B", "C"),
        DifferentLabel("B", "D"),
        DifferentLabel("C", "D"),
    ],
    render_question=_render_add4,
)


def _render_add4_same_label(bindings: Bindings, _: CompiledPlan) -> str:
    a, b, c, d = bindings["A"], bindings["B"], bindings["C"], bindings["D"]
    return with_instr(
        f"What is the sum of four {label_renderer(a.label)} values for "
        f"{a.company}, {b.company}, {c.company}, and {d.company} in {period_renderer(a)}?"
    )


A0_ADD4__SAME_COMPANY_SAME_LABEL_4X = a0_add4_spec(
    template_id=f"{A0_ADD4}__SAME_COMPANY_SAME_LABEL_4X",
    constraints=[
        DifferentCompany("A", "B"),
        DifferentCompany("A", "C"),
        DifferentCompany("A", "D"),
        SamePeriod("A", "B"),
        SamePeriod("A", "C"),
        SamePeriod("A", "D"),
        SameLabel("A", "B"),
        SameLabel("A", "C"),
        SameLabel("A", "D"),
        SameUnit("A", "B"),
        SameUnit("A", "C"),
        SameUnit("A", "D"),
        SameScale("A", "B"),
        SameScale("A", "C"),
        SameScale("A", "D"),
    ],
    render_question=_render_add4_same_label,
)


def _render_add4_norm_to_a(bindings: Bindings, compiled: CompiledPlan) -> str:
    a, b, c, d = bindings["A"], bindings["B"], bindings["C"], bindings["D"]

    ts = dag_arg_single(compiled, op="CONVERT_SCALE", arg="target_scale")
    ts_txt = scale_renderer(ts)
    scale_mid = f"{ts_txt} " if ts_txt else ""

    return with_instr(
        f"Convert the following to {scale_mid}{a.unit} at the price level of "
        f"{period_renderer(a)} (use FY exchange rates and CPI-U), then add them:\n"
        f"- {label_renderer(a.label)} for {a.company} in {period_renderer(a)}\n"
        f"- {label_renderer(b.label)} for {b.company} in {period_renderer(b)}\n"
        f"- {label_renderer(c.label)} for {c.company} in {period_renderer(c)}\n"
        f"- {label_renderer(d.label)} for {d.company} in {period_renderer(d)}"
    )


def _norm_to_a(var: str):
    return Mul(
        Mul(
            ConvertScaleTo(expr=LookupQty(var), to_var="A"),
            FxLookupAt(base_var=var, at_var=var, quote_var="A"),
        ),
        CpiLookup(from_var=var, to_var="A", series_id="CPI_US_CPIU"),
    )


A0_ADD4__NORM_TO_A_THEN_ADD = Spec(
    template_id=f"{A0_ADD4_NORM}__B_C_D_TO_A_THEN_ADD",
    vars={
        "A": _money_vs(period_kind_in=["FY"], unit_in=["USD"]),
        "B": _money_vs(period_kind_in=["FY"]),
        "C": _money_vs(period_kind_in=["FY"]),
        "D": _money_vs(period_kind_in=["FY"]),
    },
    ast=Add(
        Add(
            Add(
                LookupQty("A"),
                _norm_to_a("B"),
            ),
            _norm_to_a("C"),
        ),
        _norm_to_a("D"),
    ),
    render_question=_render_add4_norm_to_a,
    constraints=[
        # make it genuinely heterogeneous
        SameLabel("A", "B"),
        SameLabel("A", "C"),
        SameLabel("A", "D"),
        DifferentCompany("A", "B"),
        DifferentCompany("A", "C"),
        DifferentCompany("A", "D"),
        DifferentPeriod("A", "B"),
        DifferentPeriod("A", "C"),
        DifferentPeriod("A", "D"),
        DifferentUnit("A", "B"),
        DifferentUnit("A", "C"),
        DifferentUnit("A", "D"),
        DifferentScale("A", "B"),
        DifferentScale("A", "C"),
        DifferentScale("A", "D"),
    ],
    compile_opts={},
)

# -----------------------------------------------------------------------------
# Final registry
# -----------------------------------------------------------------------------

SPECS: list[Spec] = [
    A0_ADD__SAME_COMPANY_DIFF_LABEL,
    A0_ADD__DIFF_COMPANY_SAME_LABEL,
    A0_PROJECT_NEXT_FY__A_TIMES_1_PLUS_G,
    A0_ADD_CPI_ADJUSTED__SAME_COMPANY_SAME_LABEL,
    # FX
    A0_FX_CONVERT__QUOTE_IN,
    A0_FX_CONVERT__TO_B_CURRENCY,
    A0_ADD_FX__A_TO_B_THEN_ADD,
    A0_ADD_FX_CPI__A_TO_USD_THEN_CPI_THEN_ADD,
    A0_ADD_FX_TO_THIRD__A_AND_B_TO_TARGET_THEN_ADD,
    # ADD 4
    A0_ADD4__SAME_COMPANY_DIFF_LABELS,
    A0_ADD4__SAME_COMPANY_SAME_LABEL_4X,
    A0_ADD4__NORM_TO_A_THEN_ADD,
]
