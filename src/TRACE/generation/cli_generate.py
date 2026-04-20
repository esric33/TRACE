from __future__ import annotations

import argparse
from importlib import import_module
import json
from pathlib import Path
from typing import Dict, List, Tuple

from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.generation.generation_types import ExtractRecord, Spec
from TRACE.generation.sampler import sample_k_bindings_fast
from TRACE.generation.compiler import evaluate_compiled_plan_oracle, lower_spec
from TRACE.generation.capsule import make_capsule
from TRACE.generation.generation_types import (
    load_snippets,
)
from TRACE.generation.simplify import simplify_plan


def _counts_from_props(n: int, pairs: List[Tuple[str, float]]) -> List[Tuple[str, int]]:
    weights = [(name, max(0.0, float(p))) for name, p in pairs]
    total = sum(p for _, p in weights)
    if total <= 0:
        raise ValueError("At least one proportion must be > 0")

    norm = [(name, p / total) for name, p in weights]
    raw = [(name, n * p) for name, p in norm]
    base = [(name, int(x)) for name, x in raw]
    rem = n - sum(c for _, c in base)

    frac = sorted(
        [(name, x - int(x)) for name, x in raw],
        key=lambda t: t[1],
        reverse=True,
    )
    counts = {name: c for name, c in base}
    for i in range(rem):
        counts[frac[i % len(frac)][0]] += 1
    return [(name, counts[name]) for name, _ in pairs]


def _parse_csv_floats(csv: str) -> List[float]:
    parts = [p.strip() for p in csv.split(",") if p.strip()]
    if not parts:
        raise ValueError("Empty CSV")
    ws = [float(p) for p in parts]
    if any(w < 0 for w in ws):
        raise ValueError("Weights must be non-negative")
    if sum(ws) <= 0:
        raise ValueError("At least one weight must be > 0")
    return ws


def _parse_kv_floats(s: str) -> Dict[str, float]:
    """
    Parse "L0=0.4,A0=0.4,B0=0.2" into dict.
    """
    out: Dict[str, float] = {}
    if not s.strip():
        return out
    for item in s.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"Expected KEY=VAL, got: {item!r}")
        k, v = item.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            raise ValueError(f"Empty key in {item!r}")
        out[k] = float(v)
    return out


def _parse_family_weight_overrides(s: str) -> Dict[str, List[float]]:
    """
    Parse "L0=1,1,1;A0=1,0,0,0" into dict family->list[float]
    """
    out: Dict[str, List[float]] = {}
    if not s or not s.strip():
        return out
    chunks = [c.strip() for c in s.split(";") if c.strip()]
    for chunk in chunks:
        if "=" not in chunk:
            raise ValueError(f"Expected FAMILY=csv, got: {chunk!r}")
        fam, csv = chunk.split("=", 1)
        fam = fam.strip()
        csv = csv.strip()
        out[fam] = _parse_csv_floats(csv)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--benchmark", default="trace_ufr")
    ap.add_argument("--extracts", default=None)
    ap.add_argument("--snippets", default=None)
    ap.add_argument("--out", required=True)
    ap.add_argument("--n", type=int, default=10)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--max-compile-attempts", type=int, default=50)

    # New knobs:
    ap.add_argument(
        "--p",
        default="",
        help=(
            "Family proportions as KEY=VAL pairs, e.g. "
            '"L0=0.4,A0=0.4,B0=0.2". '
            "If omitted, defaults to uniform over all families in registry."
        ),
    )
    ap.add_argument(
        "--w",
        default="",
        help=(
            "Optional per-family variant weights. "
            'Format: "L0=1,1,1;A0=1,0,0,0". '
            "Order matches SPECS_BY_FAMILY[family]."
        ),
    )

    ap.add_argument(
        "--qid-prefix", default="", help="Optional prefix for all qids (default: empty)"
    )

    ap.add_argument("--distractor-count", type=int, default=0)

    args = ap.parse_args()

    benchmark_def = load_benchmark(args.benchmark)
    templates = import_module(benchmark_def.templates_module)
    SPECS_BY_FAMILY = templates.SPECS_BY_FAMILY
    FAMILIES = templates.FAMILIES

    families = list(FAMILIES)
    if not families:
        raise RuntimeError("No families registered (FAMILIES is empty)")

    # -------------------------
    # Parse family proportions
    # -------------------------
    p_map = _parse_kv_floats(args.p)

    # Validate families in p_map
    unknown = sorted(set(p_map.keys()) - set(families))
    if unknown:
        raise ValueError(f"Unknown families in --p: {unknown}. Known: {families}")

    # Default: uniform if --p omitted
    if not p_map:
        p_map = {fam: 1.0 for fam in families}

    # If user provided some families but not all, fill missing with 0.0
    for fam in families:
        p_map.setdefault(fam, 0.0)

    if all(float(p_map[f]) <= 0.0 for f in families):
        raise ValueError(f"--p has no positive proportions. Got: {p_map}")

    template_mix: dict[str, float] = {f"p_{fam}": float(p_map[fam]) for fam in families}

    # -------------------------
    # Parse variant weight overrides
    # -------------------------
    weight_overrides = _parse_family_weight_overrides(args.w)

    unknown_w = sorted(set(weight_overrides.keys()) - set(families))
    if unknown_w:
        raise ValueError(f"Unknown families in --w: {unknown_w}. Known: {families}")

    # Validate weight lengths
    for fam, ws in weight_overrides.items():
        expected = len(SPECS_BY_FAMILY.get(fam, []))
        if expected <= 0:
            raise ValueError(f"Family {fam} has no variants in registry")
        if len(ws) != expected:
            raise ValueError(
                f"--w for {fam} expected {expected} weights, got {len(ws)}: {ws}"
            )

    # -------------------------
    # Load data
    # -------------------------
    extracts_dir = Path(args.extracts) if args.extracts else benchmark_def.extracts_dir
    snippets_dir = Path(args.snippets) if args.snippets else benchmark_def.snippets_dir

    extracts = benchmark_def.load_extracts(extracts_dir)
    snippets_by_id = load_snippets(snippets_dir)

    # -------------------------
    # Plan: family -> counts
    # -------------------------
    fam_pairs = [(fam, float(p_map[fam])) for fam in families]
    family_plan = _counts_from_props(args.n, fam_pairs)

    # Expand to a concrete variant plan: (family, spec, count)
    variant_plan: List[Tuple[str, Spec, int]] = []
    for fam, fam_count in family_plan:
        variants = SPECS_BY_FAMILY.get(fam, [])
        if not variants or fam_count <= 0:
            continue

        ws = weight_overrides.get(fam, [1.0] * len(variants))
        pairs = [(v.template_id, w) for v, w in zip(variants, ws)]
        v_counts = _counts_from_props(fam_count, pairs)
        id_to_spec = {v.template_id: v for v in variants}

        for tid, c in v_counts:
            if c > 0:
                variant_plan.append((fam, id_to_spec[tid], c))

    # -------------------------
    # Build valid pools once
    # -------------------------
    # print("Building valid binding pools...")
    # valid_by_template_id: dict[str, list[dict[str, ExtractRecord]]] = {}
    # for fam, spec, _count in variant_plan:
    #    if spec.template_id in valid_by_template_id:
    #        continue
    #    #valid = build_valid_bindings(spec, extracts)
    #    #valid_by_template_id[spec.template_id] = valid
    #    #print(f"{fam} / {spec.template_id}: {len(valid)} valid bindings")
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # Generate
    # -------------------------
    i_global = 0
    for fam, spec, count in variant_plan:
        made = 0

        # optional: avoid duplicates within this (fam,spec) batch
        seen_sigs: set[tuple[str, ...]] = set()
        var_names = list(spec.vars.keys())

        while made < count:
            seed_i = args.seed + i_global
            i_global += 1

            ok = False
            last_err: Exception | None = None

            for _attempt in range(args.max_compile_attempts):
                # always get a fresh candidate binding (fast; no cartesian enumeration)
                # replace=True is fine here; we de-dupe ourselves with seen_sigs if desired.
                bindings = sample_k_bindings_fast(
                    spec,
                    extracts,
                    k=1,
                    benchmark_def=benchmark_def,
                    seed=seed_i,
                    replace=True,
                )[0]

                sig = tuple(bindings[v].extraction_id for v in var_names)
                if sig in seen_sigs:
                    seed_i = args.seed + i_global
                    i_global += 1
                    continue

                try:
                    compiled_raw = lower_spec(
                        spec,
                        bindings,
                        benchmark_def=benchmark_def,
                        seed=seed_i,
                    )
                    compiled_raw.answer = evaluate_compiled_plan_oracle(
                        compiled_raw,
                        bindings,
                        benchmark_def=benchmark_def,
                    )
                    ok = True
                    seen_sigs.add(sig)
                    break
                except Exception as e:
                    last_err = e
                    seed_i = args.seed + i_global
                    i_global += 1
                    continue

            if not ok:
                raise RuntimeError(
                    f"Failed to compile after {args.max_compile_attempts} attempts "
                    f"for {spec.template_id}. Last error: {last_err}"
                )

            dag_canonical, simp_meta = simplify_plan(spec, bindings, compiled_raw)
            compiled = compiled_raw
            compiled.dag = dag_canonical

            capsule = make_capsule(
                spec=spec,
                bindings=bindings,
                compiled=compiled,
                snippets_by_id=snippets_by_id,
                seed=seed_i,
                distractor_count=args.distractor_count,
            )

            capsule.setdefault("meta", {})
            capsule["meta"]["family"] = fam
            capsule["meta"]["template_id"] = spec.template_id
            capsule["meta"]["template_mix"] = template_mix

            capsule["meta"]["dag_raw"] = compiled_raw.dag

            if args.qid_prefix:
                capsule["qid"] = f"{args.qid_prefix}_{capsule['qid']}"
                capsule["meta"]["qid_prefix"] = args.qid_prefix

            capsule["meta"]["distractor"] = args.distractor_count

            capsule["meta"].update(compiled_raw.meta)
            capsule["meta"].update(simp_meta)

            p = out_dir / f"{capsule['qid']}.json"
            p.write_text(json.dumps(capsule, indent=2), encoding="utf-8")

            made += 1

    print(f"Wrote {args.n} capsules to {out_dir}")


if __name__ == "__main__":
    main()
