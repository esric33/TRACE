from __future__ import annotations

from collections import defaultdict

from benchmarks.trace_dir.templates.relations import ALL_SPECS


FAMILIES = ["L0", "A0", "B0"]

SPECS_BY_FAMILY = {family: [] for family in FAMILIES}
for spec in ALL_SPECS:
    family = spec.template_id.split("_", 1)[0]
    SPECS_BY_FAMILY.setdefault(family, []).append(spec)

SPECS_BY_ID = {spec.template_id: spec for spec in ALL_SPECS}

_unused = defaultdict(list)
