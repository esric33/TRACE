# reason_bench/generation/specs/registry.py
from __future__ import annotations

from typing import Dict, List

from TRACE.generation.generation_types import Spec

from TRACE.generation.specs.l0_lookup import SPECS as L0_SPECS
from TRACE.generation.specs.a0_arith import SPECS as A0_SPECS
from TRACE.generation.specs.b0_bool import SPECS as B0_SPECS

# later:
# from reason_bench.generation.specs.c0_comp import SPECS as C0_SPECS

ALL_SPECS: List[Spec] = [
    *L0_SPECS,
    *A0_SPECS,
    *B0_SPECS,
    # *C0_SPECS,
]


def _family_of(template_id: str) -> str:
    # "L0_LOOKUP__PLAIN" -> "L0"
    return template_id.split("_", 1)[0]


SPECS_BY_ID: Dict[str, Spec] = {s.template_id: s for s in ALL_SPECS}

SPECS_BY_FAMILY: Dict[str, List[Spec]] = {}
for s in ALL_SPECS:
    fam = _family_of(s.template_id)
    SPECS_BY_FAMILY.setdefault(fam, []).append(s)

FAMILIES: List[str] = sorted(SPECS_BY_FAMILY.keys())
