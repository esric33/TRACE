from __future__ import annotations

from TRACE.generation.generation_types import Bindings, CompiledPlan, ExtractRecord, VarSpec


LABEL_NOUN = {
    "treats_condition": "conditions it treats",
    "contraindicated_for": "contraindications",
    "interacts_with": "interacting drugs",
    "causes_side_effect": "side effects it causes",
}


def relation_vs(*, label_in: list[str] | None = None, object_type_in: list[str] | None = None) -> VarSpec:
    return VarSpec(
        qtype_in=["relation"],
        label_in=label_in,
        object_type_in=object_type_in,
    )


def drug(record: ExtractRecord) -> str:
    return str(record.subject.get("value") or "")


def obj(record: ExtractRecord) -> str:
    return str(record.object.get("value") or "")


def label_noun(record: ExtractRecord) -> str:
    return LABEL_NOUN.get(record.label, record.label.replace("_", " "))


def render_direct(bindings: Bindings, _: CompiledPlan) -> str:
    a = bindings["A"]
    return f"What {label_noun(a)} are stated for {drug(a)}?"


def render_union(bindings: Bindings, _: CompiledPlan) -> str:
    a = bindings["A"]
    b = bindings["B"]
    return f"What {label_noun(a)} are stated for either {drug(a)} or {drug(b)}?"


def render_intersection(bindings: Bindings, _: CompiledPlan) -> str:
    a = bindings["A"]
    b = bindings["B"]
    return f"What {label_noun(a)} are stated for both {drug(a)} and {drug(b)}?"


def render_difference(bindings: Bindings, _: CompiledPlan) -> str:
    a = bindings["A"]
    b = bindings["B"]
    return f"What {label_noun(a)} are stated for {drug(a)} but not for {drug(b)}?"


def render_size_gt(bindings: Bindings, _: CompiledPlan) -> str:
    a = bindings["A"]
    b = bindings["B"]
    return f"Does {drug(a)} have more stated {label_noun(a)} than {drug(b)}?"


def render_contains(bindings: Bindings, _: CompiledPlan) -> str:
    a = bindings["A"]
    return f"Is {obj(a)} among the stated {label_noun(a)} for {drug(a)}?"
