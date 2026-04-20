from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.core.benchmarks.types import BenchmarkDef, ExistsKey, PromptGuidance
from TRACE.generation.generation_types import (
    DifferentExtraction,
    ExtractRecord,
    NotInExtracts,
    SameCompany,
    SameMetricKey,
    SamePeriod,
    SameSlot,
    Spec,
    VarSpec,
)
from TRACE.providers.shared.dag_validator import validate_dag_obj
from TRACE.providers.shared.prompt import build_lookup_prompt, build_planner_prompt
from TRACE.generation.sampler import sample_k_bindings_fast
from TRACE.core.compiler.lower import lower_spec
from TRACE.generation.expr import LookupQty


def _record(
    extraction_id: str,
    *,
    qtype: str = "money",
    unit: str = "USD",
    scale: float = 1.0,
    period_kind: str = "FY",
    period_value: object = 2024,
    company: str = "",
    metric_key: str = "",
    metric_role: str = "",
    slots: dict[str, object] | None = None,
) -> ExtractRecord:
    return ExtractRecord(
        extraction_id=extraction_id,
        snippet_id=f"snippet_{extraction_id}",
        label=f"label_{extraction_id}",
        period={"period": period_kind, "value": period_value},
        quantity={"value": 1.0, "unit": unit, "scale": scale, "type": qtype},
        company=company,
        metric_key=metric_key,
        metric_role=metric_role,
        slots=dict(slots or {}),
    )


def _exists_key_for_record(record: ExtractRecord) -> ExistsKey | None:
    company = record.slot("company")
    metric_key = record.slot("metric_key")
    if not company or not metric_key:
        return None
    return (
        ("company", company),
        ("metric_key", metric_key),
        ("period_kind", str(record.period_kind).upper()),
        ("period_value", record.period_value),
    )


def _benchmark(
    *,
    allowed_actions=None,
    load_allowed_labels=None,
    format_lookup_query=None,
    derive_slots=None,
    build_exists_key=None,
    sampler_constraint_vars=None,
    sampler_constraint_ok=None,
    prompt_guidance=None,
    validate_planner_dag=None,
    list_maintenance_tools=None,
) -> BenchmarkDef:
    return BenchmarkDef(
        benchmark_id="test",
        asset_root=Path("."),
        snippets_dir=Path("."),
        extracts_dir=Path("."),
        schemas_dir=Path("."),
        tables_dir=None,
        templates_module="",
        allowed_actions=set(allowed_actions or []),
        register_actions=lambda _registry: None,
        load_extracts=lambda _path: [],
        load_allowed_labels=load_allowed_labels or (lambda _path: ["revenue"]),
        format_lookup_query=format_lookup_query
        or (
            lambda record: (
                f"Extract the fact for: company={record.company} label={record.label}; "
                f"period={record.period_kind} {record.period_value}. "
                "Return a ModelFact with snippet_id, label, period, quantity."
            )
        ),
        derive_slots=derive_slots or (lambda record: dict(record.slots)),
        build_exists_key=build_exists_key,
        sampler_constraint_vars=sampler_constraint_vars,
        sampler_constraint_ok=sampler_constraint_ok,
        prompt_guidance=prompt_guidance or PromptGuidance(),
        validate_planner_dag=validate_planner_dag,
        list_maintenance_tools=list_maintenance_tools,
    )


class SamplerSlotTests(unittest.TestCase):
    def test_same_slot_constraint_samples_matching_records(self) -> None:
        benchmark_def = _benchmark()
        extracts = [
            _record("a1", slots={"topic": "alpha"}),
            _record("a2", slots={"topic": "alpha"}),
            _record("b1", slots={"topic": "beta"}),
        ]
        spec = Spec(
            template_id="slot_same",
            vars={"A": VarSpec(qtype_in=["money"]), "B": VarSpec(qtype_in=["money"])},
            ast=None,
            render_question=lambda _bindings, _compiled: "",
            constraints=[SameSlot("A", "B", "topic"), DifferentExtraction("A", "B")],
        )

        bindings = sample_k_bindings_fast(
            spec,
            extracts,
            k=1,
            benchmark_def=benchmark_def,
            seed=0,
            replace=True,
        )[0]

        self.assertEqual(bindings["A"].slot("topic"), bindings["B"].slot("topic"))

    def test_not_in_extracts_uses_benchmark_exists_index(self) -> None:
        benchmark_def = _benchmark(
            build_exists_key=_exists_key_for_record,
        )
        extracts = [
            _record(
                "amount_2020",
                period_value=2020,
                company="Acme",
                metric_key="revenue",
                metric_role="amount",
                slots={"company": "Acme", "metric_key": "revenue", "metric_role": "amount"},
            ),
            _record(
                "growth_2020",
                qtype="rate",
                unit="percent",
                period_value=2020,
                company="Acme",
                metric_key="revenue",
                metric_role="rate",
                slots={"company": "Acme", "metric_key": "revenue", "metric_role": "rate"},
            ),
            _record(
                "amount_2021",
                period_value=2021,
                company="Acme",
                metric_key="revenue",
                metric_role="amount",
                slots={"company": "Acme", "metric_key": "revenue", "metric_role": "amount"},
            ),
        ]
        spec = Spec(
            template_id="not_in_extracts",
            vars={
                "A": VarSpec(qtype_in=["money"], metric_role_in=["amount"]),
                "G": VarSpec(qtype_in=["rate"], metric_role_in=["rate"]),
            },
            ast=None,
            render_question=lambda _bindings, _compiled: "",
            constraints=[
                SameCompany("A", "G"),
                SameMetricKey("A", "G"),
                SamePeriod("A", "G"),
                DifferentExtraction("A", "G"),
                NotInExtracts(
                    company_from="A",
                    metric_key_from="A",
                    period_kind="FY",
                    period_value_from="A",
                    delta_years=1,
                ),
            ],
        )

        with self.assertRaisesRegex(
            ValueError, "Could not find 1 bindings|No valid bindings found"
        ):
            sample_k_bindings_fast(
                spec,
                extracts,
                k=1,
                benchmark_def=benchmark_def,
                seed=0,
                replace=True,
            )

    def test_custom_constraint_hook_participates_in_sampling(self) -> None:
        @dataclass(frozen=True)
        class SameBucket:
            a: str
            b: str

        def sampler_constraint_vars(constraint: object) -> tuple[str, ...] | None:
            if isinstance(constraint, SameBucket):
                return (constraint.a, constraint.b)
            return None

        def sampler_constraint_ok(
            bindings: dict[str, ExtractRecord],
            constraint: object,
            _exists: set[ExistsKey],
        ) -> bool | None:
            if isinstance(constraint, SameBucket):
                return (
                    bindings[constraint.a].slot("bucket")
                    == bindings[constraint.b].slot("bucket")
                )
            return None

        benchmark_def = _benchmark(
            sampler_constraint_vars=sampler_constraint_vars,
            sampler_constraint_ok=sampler_constraint_ok,
        )
        extracts = [
            _record("a1", slots={"bucket": "north"}),
            _record("a2", slots={"bucket": "north"}),
            _record("b1", slots={"bucket": "south"}),
        ]
        spec = Spec(
            template_id="custom_hook",
            vars={"A": VarSpec(qtype_in=["money"]), "B": VarSpec(qtype_in=["money"])},
            ast=None,
            render_question=lambda _bindings, _compiled: "",
            constraints=[SameBucket("A", "B"), DifferentExtraction("A", "B")],
        )

        bindings = sample_k_bindings_fast(
            spec,
            extracts,
            k=1,
            benchmark_def=benchmark_def,
            seed=0,
            replace=True,
        )[0]

        self.assertEqual(bindings["A"].slot("bucket"), bindings["B"].slot("bucket"))

    def test_trace_ufr_benchmark_loads_slots_from_extracts(self) -> None:
        benchmark_def = load_benchmark("trace_ufr")
        extract = {
            "extraction_id": "sample",
            "snippet_id": "snippet_sample",
            "label": "revenue",
            "period": {"period": "FY", "value": 2024},
            "quantity": {"value": 10, "unit": "USD", "scale": 1, "type": "money"},
            "company": "Acme",
            "metric_key": "revenue",
            "metric_role": "amount",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "sample.json"
            path.write_text(json.dumps(extract), encoding="utf-8")

            [record] = benchmark_def.load_extracts(Path(tmpdir))

        self.assertEqual(record.slot("company"), "Acme")
        self.assertEqual(record.slot("metric_key"), "revenue")
        self.assertEqual(record.slot("metric_role"), "amount")
        self.assertEqual(
            benchmark_def.build_exists_key(record),
            (
                ("company", "Acme"),
                ("metric_key", "revenue"),
                ("period_kind", "FY"),
                ("period_value", 2024),
            ),
        )
        self.assertIn(
            "label/metric + company + period",
            "\n".join(benchmark_def.prompt_guidance.planner_grounding_rules),
        )
        self.assertEqual(
            benchmark_def.list_maintenance_tools(),
            {
                "prepare_extracts": "benchmarks.trace_ufr.tools.prepare_extracts",
                "generate_rates": "benchmarks.trace_ufr.tools.generate_rates",
                "list_currencies": "benchmarks.trace_ufr.tools.list_currencies",
            },
        )

    def test_prompt_guidance_hooks_are_inserted(self) -> None:
        benchmark_def = _benchmark(
            prompt_guidance=PromptGuidance(
                lookup_rules=("- Benchmark lookup hint.",),
                planner_grounding_rules=("- Benchmark grounding hint.",),
                planner_minimality_rules=("- Benchmark minimality hint.",),
            )
        )

        lookup_prompt = build_lookup_prompt(
            "find revenue",
            "snippet_1: Revenue was 10",
            ["revenue"],
            benchmark_def=benchmark_def,
        )
        planner_prompt = build_planner_prompt(
            {
                "question": "What was revenue?",
                "context": {"snippets": [{"snippet_id": "snippet_1", "text": "Revenue was 10"}]},
            },
            benchmark_def=benchmark_def,
        )

        self.assertIn("Benchmark lookup hint.", lookup_prompt)
        self.assertIn("Benchmark grounding hint.", planner_prompt)
        self.assertIn("Benchmark minimality hint.", planner_prompt)

    def test_planner_prompt_uses_operator_specs(self) -> None:
        planner_prompt = build_planner_prompt(
            {
                "question": "What was revenue?",
                "context": {"snippets": [{"snippet_id": "snippet_1", "text": "Revenue was 10"}]},
            },
            benchmark_def=load_benchmark("trace_ufr"),
        )

        self.assertIn('- TEXT_LOOKUP: {"query": string}', planner_prompt)
        self.assertIn('- GET_QUANTITY: {"fact": "ref:<id>"}', planner_prompt)
        self.assertIn('- FX_LOOKUP: {"series_id": string, "year": number}', planner_prompt)

    def test_lookup_query_formatter_hook_is_used_during_lowering(self) -> None:
        benchmark_def = _benchmark(
            format_lookup_query=lambda record: f"CUSTOM QUERY FOR {record.extraction_id}",
        )
        record = _record("a1", company="Acme")
        spec = Spec(
            template_id="lookup_formatter",
            vars={"A": VarSpec(qtype_in=["money"])},
            ast=LookupQty("A"),
            render_question=lambda _bindings, _compiled: "",
            constraints=[],
        )

        compiled = lower_spec(spec, {"A": record}, benchmark_def=benchmark_def, seed=0)
        [lookup_node] = [node for node in compiled.dag["nodes"] if node["op"] == "TEXT_LOOKUP"]

        self.assertEqual(lookup_node["args"]["query"], "CUSTOM QUERY FOR a1")

    def test_validator_hook_runs_after_core_validation(self) -> None:
        seen: list[dict[str, object]] = []

        def validate_planner_dag(dag: dict[str, object]) -> None:
            seen.append(dag)
            raise ValueError("benchmark validator ran")

        benchmark_def = _benchmark(
            allowed_actions={"CONST"},
            validate_planner_dag=validate_planner_dag,
        )

        with self.assertRaisesRegex(ValueError, "benchmark validator ran"):
            validate_dag_obj(
                {
                    "dag": {
                        "nodes": [{"id": "n1", "op": "CONST", "args": {"value": 1}}],
                        "output": "ref:n1",
                    }
                },
                benchmark_def=benchmark_def,
            )

        self.assertEqual(len(seen), 1)


if __name__ == "__main__":
    unittest.main()
