from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable

from TRACE.shared.io import read_json

Quantity = Dict[str, Any]  # {value, unit, scale, type}
Period = Dict[str, Any]  # {period, value}


@dataclass(frozen=True)
class ExtractRecord:
    extraction_id: str
    snippet_id: str
    label: str
    period: Period
    quantity: Quantity
    company: str
    metric_key: str
    metric_role: str

    @property
    def qtype(self) -> str:
        return str(self.quantity.get("type"))

    @property
    def unit(self) -> str:
        return str(self.quantity.get("unit"))

    @property
    def scale(self) -> Any:
        return self.quantity.get("scale")

    @property
    def value(self) -> Any:
        return self.quantity.get("value")

    @property
    def period_kind(self) -> str:
        return str(self.period.get("period"))

    @property
    def period_value(self) -> Any:
        return self.period.get("value")

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "ExtractRecord":
        company = str(d.get("company", "")).strip()
        metric_key = str(d.get("metric_key", "")).strip()
        metric_role = str(d.get("metric_role", "")).strip()
        if not company:
            raise ValueError(
                f"extract missing company: {d.get('extraction_id') or '<unknown>'}"
            )
        if not metric_key:
            raise ValueError(
                f"extract missing metric_key: {d.get('extraction_id') or '<unknown>'}"
            )
        if not metric_role:
            raise ValueError(
                f"extract missing metric_role: {d.get('extraction_id') or '<unknown>'}"
            )
        return ExtractRecord(
            extraction_id=d["extraction_id"],
            snippet_id=d["snippet_id"],
            label=d["label"],
            period=d["period"],
            quantity=d["quantity"],
            company=company,
            metric_key=metric_key,
            metric_role=metric_role,
        )


def load_extracts(extracts_dir: Path) -> List[ExtractRecord]:
    out: List[ExtractRecord] = []
    for p in sorted(extracts_dir.glob("*.json")):
        if not p.is_file():
            continue
        d = read_json(p)
        out.append(ExtractRecord.from_dict(d))
    return out


def load_snippets(snippets_dir: Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for p in sorted(snippets_dir.glob("*.json")):
        if not p.is_file():
            continue
        d = read_json(p)
        sid = d.get("snippet_id")
        if not isinstance(sid, str) or not sid:
            raise ValueError(f"snippet missing snippet_id: {p}")
        out[sid] = d
    return out


Bindings = Dict[str, "ExtractRecord"]


@dataclass(frozen=True)
class VarSpec:
    qtype_in: Optional[List[str]] = None
    unit_in: Optional[List[str]] = None
    label_in: Optional[List[str]] = None
    period_kind_in: Optional[List[str]] = None
    # NEW:
    metric_role_in: Optional[List[str]] = None  # ["amount"] or ["rate"]
    metric_key_in: Optional[List[str]] = None  # optional, usually None


@dataclass  # (frozen=True)
class CompiledPlan:
    dag: Dict[str, Any]
    lookup_map: Dict[str, str]
    answer: Dict[str, Any]
    snippet_ids: List[str]
    operators: List[str]
    meta: Dict[str, Any]


@dataclass(frozen=True)
class Spec:
    template_id: str
    vars: Dict[str, VarSpec]
    ast: object
    render_question: Callable[[Bindings, CompiledPlan], str]
    distractor_policy: str = "D0"
    constraints: Optional[List["Constraint"]] = None
    compile_opts: dict[str, Any] = field(default_factory=dict)


# --- Constraints ---
@dataclass(frozen=True)
class SameCompany:
    a: str
    b: str


@dataclass(frozen=True)
class DifferentCompany:
    a: str
    b: str


@dataclass(frozen=True)
class SamePeriod:
    a: str
    b: str


@dataclass(frozen=True)
class SameLabel:
    a: str
    b: str


@dataclass(frozen=True)
class SameUnit:
    a: str
    b: str


@dataclass(frozen=True)
class SameScale:
    a: str
    b: str


@dataclass(frozen=True)
class DifferentExtraction:
    a: str
    b: str


@dataclass(frozen=True)
class DifferentLabel:
    a: str
    b: str


@dataclass(frozen=True)
class SameMetricKey:
    a: str
    b: str


@dataclass(frozen=True)
class DifferentScale:
    a: str
    b: str


@dataclass(frozen=True)
class DifferentPeriod:
    a: str
    b: str


@dataclass(frozen=True)
class DifferentUnit:
    a: str
    b: str


# NEW: “target FY record must not exist”
@dataclass(frozen=True)
class NotInExtracts:
    company_from: str  # var name to read company from
    metric_key_from: str  # var name to read metric_key from
    period_kind: str  # e.g. "FY"
    period_value_from: str  # var name to read base period_value from
    delta_years: int = 0  # e.g. +1


type Constraint = (
    SameCompany
    | DifferentCompany
    | SamePeriod
    | DifferentPeriod
    | SameLabel
    | DifferentLabel
    | SameUnit
    | DifferentUnit
    | SameScale
    | DifferentScale
    | SameMetricKey
    | NotInExtracts
    | DifferentExtraction
)
