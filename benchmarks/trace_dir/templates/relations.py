from __future__ import annotations

from TRACE.generation.expr import (
    Gt,
    LookupQty,
    RelationSetFor,
    SetContains,
    SetDiff,
    SetIntersect,
    SetSize,
    SetUnion,
)
from TRACE.generation.generation_types import Different, Same, Spec

from benchmarks.trace_dir.templates.common import (
    relation_vs,
    render_contains,
    render_difference,
    render_direct,
    render_intersection,
    render_size_gt,
    render_union,
)


REL = relation_vs()

L0_RELATION_SET = Spec(
    template_id="L0_RELATION_SET__SAME_DRUG_SAME_LABEL_2X",
    vars={"A": REL, "B": REL},
    constraints=[
        Same("subject_value", "A", "B"),
        Same("label", "A", "B"),
        Same("object_type", "A", "B"),
        Different("object_value", "A", "B"),
    ],
    ast=RelationSetFor("A"),
    render_question=render_direct,
)

A0_SET_UNION = Spec(
    template_id="A0_SET_UNION__DIFF_DRUG_SAME_LABEL",
    vars={"A": REL, "B": REL},
    constraints=[
        Different("subject_value", "A", "B"),
        Same("label", "A", "B"),
        Same("object_type", "A", "B"),
    ],
    ast=SetUnion(
        RelationSetFor("A"),
        RelationSetFor("B"),
    ),
    render_question=render_union,
)

A0_SET_INTERSECT = Spec(
    template_id="A0_SET_INTERSECT__DIFF_DRUG_SHARED_OBJECT",
    vars={"A": REL, "B": REL},
    constraints=[
        Different("subject_value", "A", "B"),
        Same("label", "A", "B"),
        Same("object_type", "A", "B"),
        Same("object_value", "A", "B"),
    ],
    ast=SetIntersect(
        RelationSetFor("A"),
        RelationSetFor("B"),
    ),
    render_question=render_intersection,
)

A0_SET_DIFF = Spec(
    template_id="A0_SET_DIFF__DIFF_DRUG_SAME_LABEL",
    vars={"A": REL, "B": REL},
    constraints=[
        Different("subject_value", "A", "B"),
        Same("label", "A", "B"),
        Same("object_type", "A", "B"),
        Different("object_value", "A", "B"),
    ],
    ast=SetDiff(
        RelationSetFor("A"),
        RelationSetFor("B"),
    ),
    render_question=render_difference,
)

B0_SET_CONTAINS = Spec(
    template_id="B0_SET_CONTAINS__SAME_DRUG_SAME_LABEL",
    vars={"A": REL, "B": REL},
    constraints=[
        Same("subject_value", "A", "B"),
        Same("label", "A", "B"),
        Same("object_type", "A", "B"),
        Different("object_value", "A", "B"),
    ],
    ast=SetContains(
        RelationSetFor("A"),
        LookupQty("A"),
    ),
    render_question=render_contains,
)

B0_SET_SIZE_GT = Spec(
    template_id="B0_SET_SIZE_GT__TWO_VS_ONE",
    vars={"A": REL, "B": REL, "C": REL},
    constraints=[
        Same("subject_value", "A", "B"),
        Different("subject_value", "A", "C"),
        Same("label", "A", "B"),
        Same("label", "A", "C"),
        Same("object_type", "A", "B"),
        Same("object_type", "A", "C"),
        Different("object_value", "A", "B"),
    ],
    ast=Gt(
        SetSize(RelationSetFor("A")),
        SetSize(RelationSetFor("C")),
    ),
    render_question=render_size_gt,
)


ALL_SPECS = [
    L0_RELATION_SET,
    A0_SET_UNION,
    A0_SET_INTERSECT,
    A0_SET_DIFF,
    B0_SET_CONTAINS,
    B0_SET_SIZE_GT,
]
