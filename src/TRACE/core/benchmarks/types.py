from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from TRACE.generation.generation_types import ExtractRecord

type ExistsKey = tuple[tuple[str, object], ...]
type RecordLoader = Callable[[Path], list[ExtractRecord]]
type LabelLoader = Callable[[Path], list[str]]
type SlotDeriver = Callable[[ExtractRecord], Mapping[str, Any]]
type ExistsKeyBuilder = Callable[[ExtractRecord], ExistsKey | None]
type SamplerConstraintVars = Callable[[object], tuple[str, ...] | None]
type SamplerConstraintEvaluator = Callable[
    [dict[str, ExtractRecord], object, set[ExistsKey]],
    bool | None,
]
type DagValidator = Callable[[dict[str, Any]], None]
type MaintenanceToolLister = Callable[[], Mapping[str, str]]


@dataclass(frozen=True)
class PromptGuidance:
    lookup_rules: tuple[str, ...] = ()
    planner_grounding_rules: tuple[str, ...] = ()
    planner_compatibility_rules: tuple[str, ...] = ()
    planner_default_ordering: tuple[str, ...] = ()
    planner_minimality_rules: tuple[str, ...] = ()


@dataclass(frozen=True)
class BenchmarkDef:
    benchmark_id: str
    asset_root: Path
    snippets_dir: Path
    extracts_dir: Path
    schemas_dir: Path
    tables_dir: Path | None
    templates_module: str
    allowed_actions: set[str]
    register_actions: Callable[[object], None]
    load_extracts: RecordLoader
    load_allowed_labels: LabelLoader
    derive_slots: SlotDeriver
    build_exists_key: ExistsKeyBuilder | None
    sampler_constraint_vars: SamplerConstraintVars | None
    sampler_constraint_ok: SamplerConstraintEvaluator | None
    prompt_guidance: PromptGuidance = field(default_factory=PromptGuidance)
    validate_planner_dag: DagValidator | None = None
    list_maintenance_tools: MaintenanceToolLister | None = None
