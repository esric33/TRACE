# reason_bench/generation/capsule.py
from __future__ import annotations

import hashlib
import json
import random
from typing import Any, Dict, Optional, List

from TRACE.generation.generation_types import CompiledPlan, Bindings, Spec


def _stable_hash(obj: Any) -> str:
    b = json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b).hexdigest()[:16]


def _pick_distractor_ids(
    *,
    snippets_by_id: Dict[str, Dict[str, Any]],
    used_ids: List[str],
    k: int,
    seed: Optional[int],
    qid_payload: Dict[str, Any],
) -> List[str]:
    if k <= 0:
        return []

    used = set(used_ids)
    candidates = [sid for sid in snippets_by_id.keys() if sid not in used]
    if not candidates:
        return []

    # Deterministic shuffle: seed derived from (seed + stable hash of qid_payload)
    # so if you regenerate, you get the same distractors.
    h = int(_stable_hash(qid_payload), 16)
    s = (int(seed) if seed is not None else 0) ^ h
    rng = random.Random(s)
    rng.shuffle(candidates)
    return candidates[: min(k, len(candidates))]


def make_capsule(
    *,
    spec: Spec,
    bindings: Bindings,
    compiled: CompiledPlan,
    snippets_by_id: Dict[str, Dict[str, Any]],
    seed: Optional[int] = None,
    generator_version: str = "gen_v1",
    distractor_count: int = 0,
) -> Dict[str, Any]:
    if compiled.answer is None:
        raise ValueError("compiled.answer is required to build a capsule")

    # qid: spec + bound extraction ids + seed + distractor_count
    qid_payload = {
        "template_id": spec.template_id,
        "bindings": {k: v.extraction_id for k, v in bindings.items()},
        "seed": seed,
        "distractor_count": int(distractor_count),
    }
    qid = f"{spec.template_id}|{_stable_hash(qid_payload)}"

    # context snippets: include relevant snippets + distractors
    relevant_ids = list(compiled.snippet_ids)
    distractor_ids = _pick_distractor_ids(
        snippets_by_id=snippets_by_id,
        used_ids=relevant_ids,
        k=int(distractor_count),
        seed=seed,
        qid_payload=qid_payload,
    )

    snips = []
    snip_ids_in_capsule: set[str] = set()
    for sid in relevant_ids + distractor_ids:
        s = snippets_by_id.get(sid)
        if s is None:
            raise KeyError(f"Missing snippet_id={sid} in snippets store")
        if sid in snip_ids_in_capsule:
            continue
        snips.append(
            {
                "snippet_id": s["snippet_id"],
                "text": s["text"],
                **({"source": s["source"]} if "source" in s else {}),
            }
        )
        snip_ids_in_capsule.add(sid)

    capsule: Dict[str, Any] = {
        "qid": qid,
        "question": spec.render_question(bindings, compiled),
        "context": {"snippets": snips},
        "gold": {
            "lookup_map": compiled.lookup_map,
            "answer": compiled.answer,
            "dag": compiled.dag,
        },
        "meta": {
            "template_id": spec.template_id,
            "distractor_policy": spec.distractor_policy,
            "distractor_count": int(distractor_count),
            "distractor_snippet_ids": distractor_ids,
            "seed": seed,
            "generator_version": generator_version,
            "operators": compiled.operators,
            "snippet_ids": compiled.snippet_ids,
            "extraction_ids": [v.extraction_id for v in bindings.values()],
            "labels": [v.label for v in bindings.values()],
            "periods": [v.period for v in bindings.values()],
            "units": [v.unit for v in bindings.values()],
            "types": [v.qtype for v in bindings.values()],
            "scales": [v.scale for v in bindings.values()],
        },
    }

    return capsule
