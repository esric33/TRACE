# reason_bench/generation/specs/l0_lookup.py
from __future__ import annotations

from typing import List, Optional, Sequence

from TRACE.generation.expr import LookupQty, ConvertScale
from TRACE.generation.generation_types import (
    Bindings,
    CompiledPlan,
    Spec,
    VarSpec,
)
from benchmarks.trace_ufr.templates.common import (
    RenderFn,
    DEFAULT_SCALES,
    with_instr,
    label_renderer,
    period_renderer,
    scale_renderer,
    dag_arg_single,
)

# -----------------------------------------------------------------------------
# L0 — lookup family
# -----------------------------------------------------------------------------

L0_BASE = "L0_LOOKUP"


def l0_lookup_plain_spec(
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


def _render_l0_plain(bindings: Bindings, _: CompiledPlan) -> str:
    r = bindings["A"]
    return with_instr(
        f"What was {label_renderer(r.label)} for {r.company} in {period_renderer(r)}?"
    )


L0_LOOKUP__PLAIN = l0_lookup_plain_spec(
    template_id=f"{L0_BASE}__PLAIN",
    render_question=_render_l0_plain,
)

# ---- scale variants ----


def l0_lookup_scale_spec(
    *,
    template_id: str,
    render_question: RenderFn,
    var_name: str = "A",
    target_scales: Sequence[float] = DEFAULT_SCALES,
    allow_noop: bool = True,
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
        vars={var_name: vs},
        ast=ConvertScale(
            expr=LookupQty(var_name=var_name),
            target_scale_in=tuple(float(x) for x in target_scales),
        ),
        render_question=render_question,
        constraints=[],
        compile_opts={"t1_allow_noop": bool(allow_noop)},
    )


def _render_l0_scale(bindings: Bindings, compiled: CompiledPlan) -> str:
    r = bindings["A"]
    ts = dag_arg_single(compiled, op="CONVERT_SCALE", arg="target_scale")
    ts_txt = scale_renderer(ts)
    mid = f"{ts_txt} " if ts_txt else ""
    return with_instr(
        f"What was {label_renderer(r.label)} for {r.company} in {period_renderer(r)} "
        f"expressed in {mid}{r.unit}?"
    )


L0_LOOKUP__SCALE__ALLOW_NOOP = l0_lookup_scale_spec(
    template_id=f"{L0_BASE}__SCALE__ALLOW_NOOP",
    render_question=_render_l0_scale,
    allow_noop=True,
)

L0_LOOKUP__SCALE__FORCE_NON_NOOP = l0_lookup_scale_spec(
    template_id=f"{L0_BASE}__SCALE__FORCE_NON_NOOP",
    render_question=_render_l0_scale,
    allow_noop=False,
)

SPECS: list[Spec] = [
    L0_LOOKUP__PLAIN,
    L0_LOOKUP__SCALE__ALLOW_NOOP,
    L0_LOOKUP__SCALE__FORCE_NON_NOOP,
]
