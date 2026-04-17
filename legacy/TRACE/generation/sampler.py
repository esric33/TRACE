from __future__ import annotations

import random
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Iterable, Tuple, Set, DefaultDict
from collections import defaultdict

from TRACE.generation.generation_types import (
    Bindings,
    ExtractRecord,
    Spec,
    Constraint,
    VarSpec,
    SameCompany,
    SamePeriod,
    SameLabel,
    SameUnit,
    SameScale,
    DifferentExtraction,
    DifferentCompany,
    DifferentLabel,
    SameMetricKey,
    NotInExtracts,
    DifferentPeriod,
    DifferentScale,
    DifferentUnit,
)

_Q_RE = re.compile(r"^\s*Q([1-4])\s+(\d{4})\s*$", re.IGNORECASE)
_DATE_RE = re.compile(r"^\s*(\d{4})-(\d{2})-(\d{2})\s*$")

ExistsKey = tuple[
    str, str, str, object
]  # (company, metric_key, period_kind, canon_period_value)


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


def _canon_period_value(r: ExtractRecord) -> object:
    if str(r.period_kind).upper() == "FY":
        y = _year_from_period(r.period_kind, r.period_value)
        return y if y is not None else r.period_value
    return r.period_value


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
        and r.metric_role not in vs.metric_role_in
    ):
        return False
    if (
        getattr(vs, "metric_key_in", None) is not None
        and r.metric_key not in vs.metric_key_in
    ):
        return False
    return True


@dataclass(frozen=True)
class _Idx:
    by_company: Dict[str, List[ExtractRecord]]
    by_period: Dict[Tuple[str, object], List[ExtractRecord]]
    by_unit: Dict[str, List[ExtractRecord]]
    by_scale: Dict[float, List[ExtractRecord]]
    by_label: Dict[str, List[ExtractRecord]]
    by_metric_key: Dict[str, List[ExtractRecord]]


def _build_indices(extracts: List[ExtractRecord]) -> _Idx:
    by_company: DefaultDict[str, List[ExtractRecord]] = defaultdict(list)
    by_period: DefaultDict[Tuple[str, object], List[ExtractRecord]] = defaultdict(list)
    by_unit: DefaultDict[str, List[ExtractRecord]] = defaultdict(list)
    by_scale: DefaultDict[float, List[ExtractRecord]] = defaultdict(list)
    by_label: DefaultDict[str, List[ExtractRecord]] = defaultdict(list)
    by_metric_key: DefaultDict[str, List[ExtractRecord]] = defaultdict(list)

    for r in extracts:
        if r.company:
            by_company[r.company].append(r)
        by_period[(r.period_kind, r.period_value)].append(r)
        if r.unit:
            by_unit[r.unit].append(r)
        by_scale[float(r.scale)].append(r)
        if r.label:
            by_label[r.label].append(r)
        if r.metric_key:
            by_metric_key[r.metric_key].append(r)

    return _Idx(
        by_company=dict(by_company),
        by_period=dict(by_period),
        by_unit=dict(by_unit),
        by_scale=dict(by_scale),
        by_label=dict(by_label),
        by_metric_key=dict(by_metric_key),
    )


def _exists_index(extracts: List[ExtractRecord]) -> Set[ExistsKey]:
    ex: Set[ExistsKey] = set()
    for r in extracts:
        if r.company and r.metric_key:
            ex.add((r.company, r.metric_key, r.period_kind, _canon_period_value(r)))
    return ex


# ---- constraint helpers (pairwise pruning) ---------------------------------


def _pair_ok(a: ExtractRecord, b: ExtractRecord, c: Constraint) -> bool:
    if isinstance(c, SameCompany):
        return bool(a.company and b.company and a.company == b.company)
    if isinstance(c, SamePeriod):
        return (a.period_kind == b.period_kind) and (a.period_value == b.period_value)
    if isinstance(c, SameLabel):
        return a.label == b.label
    if isinstance(c, SameUnit):
        return a.unit == b.unit
    if isinstance(c, SameScale):
        return float(a.scale) == float(b.scale)
    if isinstance(c, DifferentExtraction):
        return a.extraction_id != b.extraction_id
    if isinstance(c, DifferentCompany):
        return bool(a.company and b.company and a.company != b.company)
    if isinstance(c, DifferentLabel):
        return a.label != b.label
    if isinstance(c, DifferentPeriod):
        return not (
            (a.period_kind == b.period_kind) and (a.period_value == b.period_value)
        )
    if isinstance(c, DifferentScale):
        return float(a.scale) != float(b.scale)
    if isinstance(c, DifferentUnit):
        return a.unit != b.unit
    if isinstance(c, SameMetricKey):
        return a.metric_key == b.metric_key
    raise TypeError(type(c))


def _not_in_extracts_ok(
    bindings: Dict[str, ExtractRecord],
    c: NotInExtracts,
    *,
    exists: Set[ExistsKey],
) -> bool:
    base = bindings.get(c.period_value_from)
    company_rec = bindings.get(c.company_from)
    metric_rec = bindings.get(c.metric_key_from)
    if base is None or company_rec is None or metric_rec is None:
        # can't evaluate yet -> don't prune
        return True

    company = company_rec.company
    metric_key = metric_rec.metric_key
    if not company or not metric_key:
        return False

    if str(c.period_kind).upper() != "FY":
        raise ValueError("NotInExtracts currently supports period_kind='FY' only")

    base_year = _year_from_period(base.period_kind, base.period_value)
    if base_year is None:
        return False

    target_year = base_year + int(c.delta_years)
    key: ExistsKey = (company, metric_key, "FY", target_year)
    return key not in exists


def _constraints_by_var(constraints: List[Constraint]) -> Dict[str, List[Constraint]]:
    out: DefaultDict[str, List[Constraint]] = defaultdict(list)
    for c in constraints:
        if isinstance(c, NotInExtracts):
            # touches up to 3 vars
            out[c.company_from].append(c)
            out[c.metric_key_from].append(c)
            out[c.period_value_from].append(c)
        else:
            out[c.a].append(c)
            out[c.b].append(c)
    return dict(out)


def _pair_constraints_for(
    v: str, u: str, constraints: List[Constraint]
) -> List[Constraint]:
    pcs: List[Constraint] = []
    for c in constraints:
        if isinstance(c, NotInExtracts):
            continue
        if (c.a == v and c.b == u) or (c.a == u and c.b == v):
            pcs.append(c)
    return pcs


# ---- main: fast k-sampling via backtracking ---------------------------------


def sample_k_bindings_fast(
    spec: Spec,
    extracts: List[ExtractRecord],
    k: int,
    *,
    seed: int | None = None,
    replace: bool = False,
    max_tries: int = 50_000,
) -> List[Dict[str, ExtractRecord]]:
    """
    Fast sampler: uses backtracking + pruning. Does NOT enumerate full cartesian product.
    - replace=False: tries to return up to k distinct bindings (by extraction_ids).
    - replace=True: returns k bindings (may repeat) by re-running search.
    """
    rng = random.Random(seed)
    constraints = spec.constraints or []
    var_names = list(spec.vars.keys())
    if not var_names:
        return []

    idx = _build_indices(extracts)
    exists = _exists_index(extracts)

    # unary candidate lists per var
    cands: Dict[str, List[ExtractRecord]] = {}
    for vn, vs in spec.vars.items():
        xs = [r for r in extracts if _match_varspec(r, vs)]
        if not xs:
            raise ValueError(f"No candidates for var {vn} in {spec.template_id}")
        rng.shuffle(xs)  # randomise exploration
        cands[vn] = xs

    by_var = _constraints_by_var(constraints)

    # cache pairwise constraint sets
    pair_cs: Dict[Tuple[str, str], List[Constraint]] = {}
    for i, v in enumerate(var_names):
        for u in var_names[i + 1 :]:
            pair_cs[(v, u)] = _pair_constraints_for(v, u, constraints)

    def _pair_list(v: str, u: str) -> List[Constraint]:
        return pair_cs[(v, u)] if (v, u) in pair_cs else pair_cs[(u, v)]

    # track uniqueness (optional)
    seen: Set[Tuple[str, ...]] = set()

    results: List[Dict[str, ExtractRecord]] = []
    tries = 0

    def _signature(b: Dict[str, ExtractRecord]) -> Tuple[str, ...]:
        # stable signature for de-dupe: ordered by var_names
        return tuple(b[v].extraction_id for v in var_names)

    def _is_partial_ok(bindings: Dict[str, ExtractRecord], newly_bound: str) -> bool:
        # check all pairwise constraints between newly_bound and already-bound vars
        a = bindings[newly_bound]
        for other, b in bindings.items():
            if other == newly_bound:
                continue
            for c in _pair_list(newly_bound, other):
                # normalise direction for _pair_ok: just apply directly on records
                if not _pair_ok(a, b, c):
                    return False

        # check any NotInExtracts that might now be evaluable
        for c in by_var.get(newly_bound, []):
            if isinstance(c, NotInExtracts):
                if not _not_in_extracts_ok(bindings, c, exists=exists):
                    return False
        return True

    def _filtered_domain(
        v: str, bindings: Dict[str, ExtractRecord]
    ) -> List[ExtractRecord]:
        # start from unary candidates, then aggressively filter by equalities where possible
        dom = cands[v]

        # quick equality-based narrowing using indices when v has SameX constraints to some bound var
        # (this makes a big difference for 4-var same company/period/unit/scale)
        for other, r_other in bindings.items():
            if other == v:
                continue
            for c in _pair_list(v, other):
                if isinstance(c, SameCompany) and r_other.company:
                    dom = [r for r in dom if r.company == r_other.company]
                elif isinstance(c, SamePeriod):
                    dom = [
                        r
                        for r in dom
                        if (
                            r.period_kind == r_other.period_kind
                            and r.period_value == r_other.period_value
                        )
                    ]
                elif isinstance(c, SameUnit) and r_other.unit:
                    dom = [r for r in dom if r.unit == r_other.unit]
                elif isinstance(c, SameScale):
                    s = float(r_other.scale)
                    dom = [r for r in dom if float(r.scale) == s]
                elif isinstance(c, SameLabel) and r_other.label:
                    dom = [r for r in dom if r.label == r_other.label]
                elif isinstance(c, SameMetricKey) and r_other.metric_key:
                    mk = r_other.metric_key
                    dom = [r for r in dom if r.metric_key == mk]

        return dom

    def _choose_next_var(bindings: Dict[str, ExtractRecord]) -> str:
        # MRV heuristic: choose unbound var with smallest filtered domain
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

        # recursion
        def rec() -> Optional[Dict[str, ExtractRecord]]:
            nonlocal tries
            if len(bindings) == len(var_names):
                return dict(bindings)

            v = _choose_next_var(bindings)
            dom = _filtered_domain(v, bindings)
            if not dom:
                return None

            # randomise a little but keep it bounded for speed
            # (sampling without replacement inside the domain)
            # try up to N candidates; N small keeps it fast on hard specs
            N = min(len(dom), 200)
            # pick N unique indices
            if len(dom) > N:
                picks = rng.sample(dom, N)
            else:
                picks = dom

            for r in picks:
                tries += 1
                if tries > max_tries:
                    return None

                bindings[v] = r
                if _is_partial_ok(bindings, v):
                    out = rec()
                    if out is not None:
                        return out
                bindings.pop(v, None)

            return None

        return rec()

    if replace:
        for _ in range(k):
            b = _search_once()
            if b is None:
                raise ValueError(
                    f"Could not find {k} bindings for {spec.template_id} within max_tries={max_tries}"
                )
            results.append(b)
        return results

    # without replacement / de-dupe
    while len(results) < k:
        b = _search_once()
        if b is None:
            break
        sig = _signature(b)
        if sig in seen:
            continue
        seen.add(sig)
        results.append(b)

    if not results:
        raise ValueError(
            f"No valid bindings found for spec {spec.template_id} within max_tries={max_tries}"
        )

    return results
