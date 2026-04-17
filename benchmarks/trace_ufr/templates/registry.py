from __future__ import annotations

from typing import Dict, List

from TRACE.generation.generation_types import Spec

from benchmarks.trace_ufr.templates.a0_arith import SPECS as A0_SPECS
from benchmarks.trace_ufr.templates.b0_bool import SPECS as B0_SPECS
from benchmarks.trace_ufr.templates.l0_lookup import SPECS as L0_SPECS


ALL_SPECS: List[Spec] = [
    *L0_SPECS,
    *A0_SPECS,
    *B0_SPECS,
]


def _family_of(template_id: str) -> str:
    return template_id.split("_", 1)[0]


SPECS_BY_ID: Dict[str, Spec] = {s.template_id: s for s in ALL_SPECS}

SPECS_BY_FAMILY: Dict[str, List[Spec]] = {}
for spec in ALL_SPECS:
    fam = _family_of(spec.template_id)
    SPECS_BY_FAMILY.setdefault(fam, []).append(spec)

FAMILIES: List[str] = sorted(SPECS_BY_FAMILY.keys())

