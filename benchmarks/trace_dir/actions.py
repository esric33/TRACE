from __future__ import annotations

from typing import Any

from TRACE.core.actions.types import (
    ActionDef,
    ActionExecContext,
    ArgSpec,
    OutputSpec,
)
from TRACE.core.executor.support import ExecErrorCode, ExecPhase, exec_error
from TRACE.shared.text_norm import normalize_relation_text


def _norm(value: object) -> str:
    return normalize_relation_text(value)


def _require_relation_set(value: Any, *, op: str, arg: str) -> dict[str, Any]:
    if not (
        isinstance(value, dict)
        and value.get("type") == "relation_set"
        and isinstance(value.get("items"), list)
    ):
        raise exec_error(
            ExecErrorCode.TYPE_MISMATCH,
            f"{op} expected relation_set for {arg}",
            phase=ExecPhase.ACTION,
            op=op,
            arg=arg,
            expected="relation_set",
            got=value,
        )
    return value


def _relation_key(item: dict[str, Any]) -> str:
    return _norm(item.get("object", {}).get("value") if isinstance(item.get("object"), dict) else item.get("value"))


def _set_key(value: dict[str, Any]) -> tuple[str, str]:
    return (str(value.get("label") or ""), str(value.get("object_type") or ""))


def _require_compatible(a: dict[str, Any], b: dict[str, Any], *, op: str) -> None:
    if _set_key(a) != _set_key(b):
        raise exec_error(
            ExecErrorCode.TYPE_MISMATCH,
            f"{op} requires matching relation label and object_type",
            phase=ExecPhase.ACTION,
            op=op,
            a={"label": a.get("label"), "object_type": a.get("object_type")},
            b={"label": b.get("label"), "object_type": b.get("object_type")},
        )


def _merge_subjects(values: list[dict[str, Any]]) -> dict[str, Any]:
    subjects: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for value in values:
        subject = value.get("subject")
        if isinstance(subject, dict):
            key = (str(subject.get("type") or ""), str(subject.get("value") or ""))
            if key not in seen:
                seen.add(key)
                subjects.append(subject)
        for subject in value.get("subjects") or []:
            if isinstance(subject, dict):
                key = (str(subject.get("type") or ""), str(subject.get("value") or ""))
                if key not in seen:
                    seen.add(key)
                    subjects.append(subject)
    if len(subjects) == 1:
        return {"subject": subjects[0], "subjects": subjects}
    return {"subject": None, "subjects": subjects}


def _make_relation_set(
    *,
    label: str,
    object_type: str,
    subject: dict[str, Any] | None,
    items: list[dict[str, Any]],
    source_op: str,
) -> dict[str, Any]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        key = _relation_key(item)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return {
        "type": "relation_set",
        "label": label,
        "object_type": object_type,
        "subject": subject,
        "subjects": [subject] if subject else [],
        "items": deduped,
        "source_op": source_op,
    }


def _exec_model_fact(ctx: ActionExecContext, _: str, args: dict[str, Any]) -> dict[str, Any]:
    snippet_id = args["snippet_id"]
    label = args["label"]
    subject = args["subject"]
    obj = args["object"]

    context_ids = {
        s.get("snippet_id")
        for s in ctx.capsule.get("context", {}).get("snippets", [])
        if isinstance(s, dict)
    }
    if snippet_id not in context_ids:
        raise exec_error(
            ExecErrorCode.BAD_ARGS,
            "MODEL_FACT snippet_id must refer to a context snippet",
            phase=ExecPhase.ACTION,
            op="MODEL_FACT",
            arg="snippet_id",
            got=snippet_id,
        )

    allowed_labels = set(ctx.benchmark_def.load_allowed_labels(ctx.benchmark_def.schemas_dir))
    if label not in allowed_labels:
        raise exec_error(
            ExecErrorCode.BAD_ARGS,
            "MODEL_FACT label is not allowed for this benchmark",
            phase=ExecPhase.ACTION,
            op="MODEL_FACT",
            arg="label",
            got=label,
        )

    if not isinstance(subject, dict) or subject.get("type") != "drug" or not subject.get("value"):
        raise exec_error(
            ExecErrorCode.BAD_ARGS,
            "MODEL_FACT subject must be a drug object",
            phase=ExecPhase.ACTION,
            op="MODEL_FACT",
            arg="subject",
            got=subject,
        )
    if not isinstance(obj, dict) or not obj.get("type") or not obj.get("value"):
        raise exec_error(
            ExecErrorCode.BAD_ARGS,
            "MODEL_FACT object must have type and value",
            phase=ExecPhase.ACTION,
            op="MODEL_FACT",
            arg="object",
            got=obj,
        )

    item = {
        "label": label,
        "subject": subject,
        "object": obj,
        "value": obj["value"],
        "source": {
            "snippet_id": snippet_id,
            "label": label,
            "subject": subject,
            "object": obj,
        },
    }
    return _make_relation_set(
        label=label,
        object_type=str(obj["type"]),
        subject=subject,
        items=[item],
        source_op="MODEL_FACT",
    )


def _exec_make_set(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    raw_items = args["items"]
    if not isinstance(raw_items, list) or not raw_items:
        raise exec_error(
            ExecErrorCode.BAD_ARGS,
            "MAKE_SET requires at least one item",
            phase=ExecPhase.ACTION,
            op="MAKE_SET",
            arg="items",
            got=raw_items,
        )
    sets = [_require_relation_set(item, op="MAKE_SET", arg="items") for item in raw_items]
    first = sets[0]
    for value in sets[1:]:
        _require_compatible(first, value, op="MAKE_SET")
    subjects = _merge_subjects(sets)
    merged_items = [item for value in sets for item in value["items"]]
    return {
        **_make_relation_set(
            label=str(first.get("label")),
            object_type=str(first.get("object_type")),
            subject=subjects["subject"],
            items=merged_items,
            source_op="MAKE_SET",
        ),
        "subjects": subjects["subjects"],
    }


def _binary_set_op(args: dict[str, Any], *, op: str) -> tuple[dict[str, Any], dict[str, Any]]:
    a = _require_relation_set(args["a"], op=op, arg="a")
    b = _require_relation_set(args["b"], op=op, arg="b")
    _require_compatible(a, b, op=op)
    return a, b


def _exec_set_union(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a, b = _binary_set_op(args, op="SET_UNION")
    subjects = _merge_subjects([a, b])
    return {
        **_make_relation_set(
            label=str(a.get("label")),
            object_type=str(a.get("object_type")),
            subject=subjects["subject"],
            items=[*a["items"], *b["items"]],
            source_op="SET_UNION",
        ),
        "subjects": subjects["subjects"],
    }


def _exec_set_intersect(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a, b = _binary_set_op(args, op="SET_INTERSECT")
    b_keys = {_relation_key(item) for item in b["items"]}
    subjects = _merge_subjects([a, b])
    return {
        **_make_relation_set(
            label=str(a.get("label")),
            object_type=str(a.get("object_type")),
            subject=subjects["subject"],
            items=[item for item in a["items"] if _relation_key(item) in b_keys],
            source_op="SET_INTERSECT",
        ),
        "subjects": subjects["subjects"],
    }


def _exec_set_diff(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a, b = _binary_set_op(args, op="SET_DIFF")
    b_keys = {_relation_key(item) for item in b["items"]}
    return _make_relation_set(
        label=str(a.get("label")),
        object_type=str(a.get("object_type")),
        subject=a.get("subject") if isinstance(a.get("subject"), dict) else None,
        items=[item for item in a["items"] if _relation_key(item) not in b_keys],
        source_op="SET_DIFF",
    )


def _exec_set_size(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    value = _require_relation_set(args["set"], op="SET_SIZE", arg="set")
    return {"value": len(value["items"]), "unit": "", "scale": 1, "type": "scalar"}


def _exec_set_contains(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    haystack = _require_relation_set(args["set"], op="SET_CONTAINS", arg="set")
    needle = _require_relation_set(args["item"], op="SET_CONTAINS", arg="item")
    _require_compatible(haystack, needle, op="SET_CONTAINS")
    if len(needle["items"]) != 1:
        raise exec_error(
            ExecErrorCode.BAD_ARGS,
            "SET_CONTAINS item must be a singleton relation_set",
            phase=ExecPhase.ACTION,
            op="SET_CONTAINS",
            arg="item",
            got=needle,
        )
    haystack_keys = {_relation_key(item) for item in haystack["items"]}
    return {
        "value": _relation_key(needle["items"][0]) in haystack_keys,
        "unit": "bool",
        "scale": 1,
        "type": "bool",
    }


def register_actions(registry) -> None:
    for action in (
        ActionDef(
            name="MODEL_FACT",
            arg_specs=(
                ArgSpec("snippet_id", "string", non_empty=True),
                ArgSpec("label", "string", non_empty=True),
                ArgSpec("subject", "object"),
                ArgSpec("object", "object"),
            ),
            summary="Assert one directly stated medical relation fact extracted from context",
            output_spec=OutputSpec(
                category="relation_set",
                summary="Singleton relation_set with label, subject, object_type, and provenance",
            ),
            executor=_exec_model_fact,
        ),
        ActionDef(
            name="MAKE_SET",
            arg_specs=(ArgSpec("items", "refs"),),
            summary="Build a relation set from singleton relation facts with matching label and object type",
            output_spec=OutputSpec(
                category="relation_set",
                summary="Relation set with common label and object_type",
            ),
            executor=_exec_make_set,
        ),
        ActionDef(
            name="SET_UNION",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            summary="Union two compatible relation sets",
            output_spec=OutputSpec(category="relation_set", summary="Union relation set"),
            executor=_exec_set_union,
        ),
        ActionDef(
            name="SET_INTERSECT",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            summary="Intersect two compatible relation sets by object value",
            output_spec=OutputSpec(category="relation_set", summary="Intersection relation set"),
            executor=_exec_set_intersect,
        ),
        ActionDef(
            name="SET_DIFF",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            summary="Subtract compatible relation set b from a by object value",
            output_spec=OutputSpec(category="relation_set", summary="Difference relation set"),
            executor=_exec_set_diff,
        ),
        ActionDef(
            name="SET_SIZE",
            arg_specs=(ArgSpec("set", "ref"),),
            summary="Return the number of items in a relation set",
            output_spec=OutputSpec(
                category="quantity",
                summary="Scalar quantity",
                fixed_type="scalar",
                fixed_unit="",
                fixed_scale=1,
            ),
            executor=_exec_set_size,
        ),
        ActionDef(
            name="SET_CONTAINS",
            arg_specs=(ArgSpec("set", "ref"), ArgSpec("item", "ref")),
            summary="Return whether a compatible relation set contains a singleton item by object value",
            output_spec=OutputSpec(
                category="quantity",
                summary="Boolean quantity",
                fixed_type="bool",
                fixed_unit="bool",
                fixed_scale=1,
            ),
            executor=_exec_set_contains,
        ),
    ):
        registry.register(action, allow_override=(action.name == "MODEL_FACT"))
