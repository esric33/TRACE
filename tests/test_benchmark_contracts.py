from __future__ import annotations

import unittest
from importlib import import_module

from TRACE.core.actions import build_registry_for_benchmark
from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.core.compiler.lower import compile_spec
from TRACE.core.executor.runtime import execute_dag
from TRACE.generation.sampler import sample_k_bindings_fast

from tests.support import make_capsule_from_snippet_ids, load_trace_ufr_benchmark


class BenchmarkContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.benchmark_def = load_trace_ufr_benchmark()
        cls.extracts = cls.benchmark_def.load_extracts(cls.benchmark_def.extracts_dir)
        cls.registry = import_module(cls.benchmark_def.templates_module)

    def test_trace_ufr_benchmark_loads_with_expected_contract_hooks(self) -> None:
        benchmark_def = load_benchmark("trace_ufr")

        self.assertEqual(benchmark_def.benchmark_id, "trace_ufr")
        self.assertTrue(benchmark_def.snippets_dir.exists())
        self.assertTrue(benchmark_def.extracts_dir.exists())
        self.assertTrue(callable(benchmark_def.derive_slots))
        self.assertTrue(callable(benchmark_def.build_exists_key))
        self.assertTrue(callable(benchmark_def.validate_planner_dag))

    def test_trace_ufr_declared_actions_are_registered(self) -> None:
        registry = build_registry_for_benchmark(self.benchmark_def)

        self.assertTrue(self.benchmark_def.allowed_actions <= registry.allowed_ops())
        self.assertIn("FX_LOOKUP", registry.allowed_ops())
        self.assertIn("CPI_LOOKUP", registry.allowed_ops())

    def test_trace_ufr_extracts_include_derived_slots_and_exists_keys(self) -> None:
        record = self.extracts[0]

        self.assertEqual(record.slot("company"), record.company)
        self.assertEqual(record.slot("metric_key"), record.metric_key)
        self.assertEqual(record.slot("unit"), record.unit)
        self.assertEqual(record.slot("qtype"), record.qtype)

        exists_key = self.benchmark_def.build_exists_key(record)
        self.assertIsNotNone(exists_key)
        self.assertIn(("company", record.company), exists_key)
        self.assertIn(("metric_key", record.metric_key), exists_key)

    def test_trace_ufr_template_registry_is_non_empty(self) -> None:
        self.assertTrue(self.registry.ALL_SPECS)
        self.assertEqual(
            set(self.registry.SPECS_BY_ID),
            {spec.template_id for spec in self.registry.ALL_SPECS},
        )

    def test_every_template_can_sample_compile_and_oracle_execute(self) -> None:
        for index, spec in enumerate(self.registry.ALL_SPECS):
            with self.subTest(template_id=spec.template_id):
                [bindings] = sample_k_bindings_fast(
                    spec,
                    self.extracts,
                    k=1,
                    benchmark_def=self.benchmark_def,
                    seed=index,
                    replace=True,
                )
                compiled = compile_spec(
                    spec,
                    bindings,
                    self.benchmark_def,
                    seed=index,
                )
                capsule = make_capsule_from_snippet_ids(
                    compiled.snippet_ids,
                    qid=f"{spec.template_id}|smoke",
                    question=spec.render_question(bindings, compiled),
                )

                result = execute_dag(
                    compiled.dag,
                    self.benchmark_def,
                    "oracle",
                    provider_ctx=None,
                    capsule=capsule,
                    cache={},
                )

                self.assertEqual(result["output"], compiled.answer)
                self.assertTrue(compiled.operators)

    def test_fx_scale_add_template_uses_one_shared_target_scale(self) -> None:
        spec = self.registry.SPECS_BY_ID[
            "A0_ADD_FX_SCALE__A_TO_B_THEN_ADD_IN_TARGET_SCALE"
        ]
        [bindings] = sample_k_bindings_fast(
            spec,
            self.extracts,
            k=1,
            benchmark_def=self.benchmark_def,
            seed=11,
            replace=True,
        )
        compiled = compile_spec(
            spec,
            bindings,
            self.benchmark_def,
            seed=11,
        )
        target_scales = [
            node["args"]["target_scale"]
            for node in compiled.dag["nodes"]
            if node["op"] == "CONVERT_SCALE"
        ]

        self.assertEqual(len(target_scales), 2)
        self.assertEqual(len(set(target_scales)), 1)
        self.assertIn("reported in", spec.render_question(bindings, compiled))


if __name__ == "__main__":
    unittest.main()
