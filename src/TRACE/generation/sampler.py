from __future__ import annotations

import random
import re
from collections import defaultdict
from typing import DefaultDict, Dict, List, Optional, Set, Tuple

from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.core.benchmarks.types import BenchmarkDef, ExistsKey
from TRACE.generation.generation_types import (
    Bindings,
    Constraint,
    DifferentCompany,
    DifferentExtraction,
    DifferentLabel,
    DifferentPeriod,
    DifferentScale,
    DifferentSlot,
    DifferentUnit,
    ExtractRecord,
    NotExists,
    NotInExtracts,
    SameCompany,
    SameLabel,
    SameMetricKey,
    SamePeriod,
    SameScale,
    SameSlot,
    SameUnit,
    Spec,
    VarSpec,
)

_Q_RE = re.compile(r"^\s*Q([1-4])\s+(\d{4})\s*$", re.IGNORECASE)
_DATE_RE = re.compile(r"^\s*(\d{4})-(\d{2})-(\d{2})\s*$")


def _year_from_period(kind: str, value: object) -> int | None:
    kind = str(kind).upper()
    if kind == "FY":
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            s = value.strip()
            if s.isdigit():
                return int(s)
        return None
    if kind == "Q":
        if isinstance(value, str):
            m = _Q_RE.match(value)
            if m:
                return int(m.group(2))
        return None
    if kind == "ASOF":
        if isinstance(value, str):
            m = _DATE_RE.match(value)
            if m:
                return int(m.group(1))
        return None
    return None


def _match_varspec(r: ExtractRecord, vs: VarSpec) -> bool:
    if vs.qtype_in is not None and r.qtype not in vs.qtype_in:
        return False
    if vs.unit_in is not None and r.unit not in vs.unit_in:
        return False
    if vs.label_in is not None and r.label not in vs.label_in:
        return False
    if vs.period_kind_in is not None and r.period_kind not in vs.period_kind_in:
        return False
    if (
        getattr(vs, "metric_role_in", None) is not None
        and r.slot("metric_role") not in vs.metric_role_in
    ):
        return False
    if (
        getattr(vs, "metric_key_in", None) is not None
        and r.slot("metric_key") not in vs.metric_key_in
    ):
        return False
    return True


def _normalize_extracts(
    extracts: List[ExtractRecord], benchmark_def: BenchmarkDef
) -> List[ExtractRecord]:
    out: List[ExtractRecord] = []
    for record in extracts:
        derived_slots = dict(benchmark_def.derive_slots(record))
        out.append(record.with_slots(derived_slots) if derived_slots else record)
    return out


def _exists_index(
    extracts: List[ExtractRecord], benchmark_def: BenchmarkDef
) -> Set[ExistsKey]:
    if benchmark_def.build_exists_key is None:
        return set()

    out: Set[ExistsKey] = set()
    for record in extracts:
        key = benchmark_def.build_exists_key(record)
        if key is not None:
            out.add(key)
    return out


def _slot_name(c: Constraint) -> str | None:
    if isinstance(c, (SameSlot, DifferentSlot)):
        return c.slot
    if isinstance(c, (SameCompany, DifferentCompany)):
        return "company"
    if isinstance(c, (SamePeriod, DifferentPeriod)):
        return "period"
    if isinstance(c, (SameLabel, DifferentLabel)):
        return "label"
    if isinstance(c, (SameUnit, DifferentUnit)):
        return "unit"
    if isinstance(c, (SameScale, DifferentScale)):
        return "scale"
    if isinstance(c, SameMetricKey):
        return "metric_key"
    if isinstance(c, DifferentExtraction):
        return "extraction_id"
    return None


def _same_like(c: Constraint) -> bool:
    return isinstance(
        c,
        (
            SameSlot,
            SameCompany,
            SamePeriod,
            SameLabel,
            SameUnit,
            SameScale,
            SameMetricKey,
        ),
    )


def _different_like(c: Constraint) -> bool:
    return isinstance(
        c,
        (
            DifferentSlot,
            DifferentCompany,
            DifferentPeriod,
            DifferentLabel,
            DifferentUnit,
            DifferentScale,
            DifferentExtraction,
        ),
    )


def _slot_value(record: ExtractRecord, slot: str) -> object | None:
    value = record.slot(slot)
    if value == "":
        return None
    if slot == "scale" and value is not None:
        return float(value)
    return value


def _pair_ok(a: ExtractRecord, b: ExtractRecord, c: Constraint) -> bool:
    slot = _slot_name(c)
    if slot is None:
        raise TypeError(type(c))

    a_val = _slot_value(a, slot)
    b_val = _slot_value(b, slot)
    if a_val is None or b_val is None:
        return False
    if _same_like(c):
        return a_val == b_val
    if _different_like(c):
        return a_val != b_val
    raise TypeError(type(c))


def _not_exists_shape(
    c: NotExists | NotInExtracts,
) -> tuple[Dict[str, str], str, str, int]:
    if isinstance(c, NotExists):
        return dict(c.slot_refs), c.period_kind, c.period_value_from, int(c.delta_years)
    return (
        {
            "company": c.company_from,
            "metric_key": c.metric_key_from,
        },
        c.period_kind,
        c.period_value_from,
        int(c.delta_years),
    )


def _exists_key(
    slot_values: Dict[str, object], period_kind: str, period_value: object
) -> ExistsKey:
    items = sorted(slot_values.items())
    items.extend(
        [
            ("period_kind", str(period_kind).upper()),
            ("period_value", period_value),
        ]
    )
    return tuple(items)


def _not_exists_ok(
    bindings: Dict[str, ExtractRecord],
    c: NotExists | NotInExtracts,
    *,
    exists: Set[ExistsKey],
) -> bool:
    slot_refs, period_kind, period_value_from, delta_years = _not_exists_shape(c)

    base = bindings.get(period_value_from)
    if base is None:
        return True

    slot_values: Dict[str, object] = {}
    for slot_name, var_name in slot_refs.items():
        record = bindings.get(var_name)
        if record is None:
            return True
        slot_value = _slot_value(record, slot_name)
        if slot_value is None:
            return False
        slot_values[slot_name] = slot_value

    if str(period_kind).upper() != "FY":
        raise ValueError("NotExists currently supports period_kind='FY' only")

    base_year = _year_from_period(base.period_kind, base.period_value)
    if base_year is None:
        return False

    target_year = base_year + delta_years
    return _exists_key(slot_values, "FY", target_year) not in exists


def _constraint_var_names(
    c: Constraint, benchmark_def: BenchmarkDef
) -> tuple[str, ...]:
    if hasattr(c, "a") and hasattr(c, "b"):
        return (c.a, c.b)
    if isinstance(c, (NotExists, NotInExtracts)):
        slot_refs, _period_kind, period_value_from, _delta_years = _not_exists_shape(c)
        vars_for_constraint = list(slot_refs.values())
        vars_for_constraint.append(period_value_from)
        return tuple(dict.fromkeys(vars_for_constraint))
    if benchmark_def.sampler_constraint_vars is not None:
        vars_for_constraint = benchmark_def.sampler_constraint_vars(c)
        if vars_for_constraint:
            return tuple(dict.fromkeys(vars_for_constraint))
    raise TypeError(f"Unsupported constraint type: {type(c)!r}")


def _constraint_ok(
    bindings: Dict[str, ExtractRecord],
    constraint: Constraint,
    *,
    constraint_vars: tuple[str, ...],
    benchmark_def: BenchmarkDef,
    exists: Set[ExistsKey],
) -> bool:
    if _slot_name(constraint) is not None:
        left = bindings[constraint_vars[0]]
        right = bindings[constraint_vars[1]]
        return _pair_ok(left, right, constraint)

    if isinstance(constraint, (NotExists, NotInExtracts)):
        return _not_exists_ok(bindings, constraint, exists=exists)

    if benchmark_def.sampler_constraint_ok is not None:
        decision = benchmark_def.sampler_constraint_ok(bindings, constraint, exists)
        if decision is not None:
            return decision

    raise TypeError(f"Unsupported constraint type: {type(constraint)!r}")


def sample_k_bindings_fast(
    spec: Spec,
    extracts: List[ExtractRecord],
    k: int,
    *,
    benchmark_def: BenchmarkDef | None = None,
    seed: int | None = None,
    replace: bool = False,
    max_tries: int = 50_000,
) -> List[Dict[str, ExtractRecord]]:
    """
    Fast sampler: uses backtracking + pruning. Does NOT enumerate full cartesian product.
    - replace=False: tries to return up to k distinct bindings (by extraction_ids).
    - replace=True: returns k bindings (may repeat) by re-running search.
    """
    if benchmark_def is None:
        benchmark_def = load_benchmark("trace_ufr")

    rng = random.Random(seed)
    constraints = spec.constraints or []
    var_names = list(spec.vars.keys())
    if not var_names:
        return []

    extracts = _normalize_extracts(extracts, benchmark_def)
    exists = _exists_index(extracts, benchmark_def)

    cands: Dict[str, List[ExtractRecord]] = {}
    for vn, vs in spec.vars.items():
        xs = [r for r in extracts if _match_varspec(r, vs)]
        if not xs:
            raise ValueError(f"No candidates for var {vn} in {spec.template_id}")
        rng.shuffle(xs)
        cands[vn] = xs

    constraint_vars_by_id: Dict[int, tuple[str, ...]] = {
        id(c): _constraint_var_names(c, benchmark_def) for c in constraints
    }
    by_var: DefaultDict[str, List[Constraint]] = defaultdict(list)
    for constraint in constraints:
        for var_name in constraint_vars_by_id[id(constraint)]:
            by_var[var_name].append(constraint)

    seen: Set[Tuple[str, ...]] = set()
    results: List[Dict[str, ExtractRecord]] = []
    tries = 0

    def _signature(b: Dict[str, ExtractRecord]) -> Tuple[str, ...]:
        return tuple(b[v].extraction_id for v in var_names)

    def _is_partial_ok(bindings: Dict[str, ExtractRecord], newly_bound: str) -> bool:
        checked: Set[int] = set()
        for constraint in by_var.get(newly_bound, []):
            cid = id(constraint)
            if cid in checked:
                continue
            vars_for_constraint = constraint_vars_by_id[cid]
            if any(var_name not in bindings for var_name in vars_for_constraint):
                continue
            if not _constraint_ok(
                bindings,
                constraint,
                constraint_vars=vars_for_constraint,
                benchmark_def=benchmark_def,
                exists=exists,
            ):
                return False
            checked.add(cid)
        return True

    def _filtered_domain(
        v: str, bindings: Dict[str, ExtractRecord]
    ) -> List[ExtractRecord]:
        dom = cands[v]

        for constraint in by_var.get(v, []):
            if not _same_like(constraint):
                continue
            vars_for_constraint = constraint_vars_by_id[id(constraint)]
            if len(vars_for_constraint) != 2 or v not in vars_for_constraint:
                continue
            other = vars_for_constraint[0] if vars_for_constraint[1] == v else vars_for_constraint[1]
            other_record = bindings.get(other)
            if other_record is None:
                continue
            slot = _slot_name(constraint)
            if slot is None:
                continue
            other_value = _slot_value(other_record, slot)
            if other_value is None:
                return []
            dom = [record for record in dom if _slot_value(record, slot) == other_value]

        return dom

    def _choose_next_var(bindings: Dict[str, ExtractRecord]) -> str:
        best_v = None
        best_n = None
        for v in var_names:
            if v in bindings:
                continue
            dom = _filtered_domain(v, bindings)
            n = len(dom)
            if best_n is None or n < best_n:
                best_v, best_n = v, n
            if best_n == 0:
                break
        assert best_v is not None
        return best_v

    def _search_once() -> Optional[Dict[str, ExtractRecord]]:
        nonlocal tries
        bindings: Dict[str, ExtractRecord] = {}

        def rec() -> Optional[Dict[str, ExtractRecord]]:
            nonlocal tries
            if len(bindings) == len(var_names):
                return dict(bindings)

            v = _choose_next_var(bindings)
            dom = _filtered_domain(v, bindings)
            if not dom:
                return None

            sample_size = min(len(dom), 200)
            picks = rng.sample(dom, sample_size) if len(dom) > sample_size else dom

            for record in picks:
                tries += 1
                if tries > max_tries:
                    return None

                bindings[v] = record
                if _is_partial_ok(bindings, v):
                    out = rec()
                    if out is not None:
                        return out
                bindings.pop(v, None)

            return None

        return rec()

    if replace:
        for _ in range(k):
            binding = _search_once()
            if binding is None:
                raise ValueError(
                    f"Could not find {k} bindings for {spec.template_id} within max_tries={max_tries}"
                )
            results.append(binding)
        return results

    while len(results) < k:
        binding = _search_once()
        if binding is None:
            break
        sig = _signature(binding)
        if sig in seen:
            continue
        seen.add(sig)
        results.append(binding)

    if not results:
        raise ValueError(
            f"No valid bindings found for spec {spec.template_id} within max_tries={max_tries}"
        )

    return results
