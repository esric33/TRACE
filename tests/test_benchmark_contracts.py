from __future__ import annotations

import unittest
from importlib import import_module

from TRACE.core.actions import build_registry_for_benchmark
from TRACE.core.compiler.lower import compile_spec
from TRACE.core.executor.oracle import make_oracle_context
from TRACE.core.executor.runtime import execute_dag
from TRACE.generation.sampler import sample_k_bindings_fast

from tests.support import make_capsule_from_snippet_ids, load_trace_ufr_benchmark


class BenchmarkContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.benchmark_def = load_trace_ufr_benchmark()
        cls.extracts = cls.benchmark_def.load_extracts(cls.benchmark_def.extracts_dir)
        cls.registry = import_module(cls.benchmark_def.templates_module)

    def test_trace_ufr_declared_actions_are_registered(self) -> None:
        registry = build_registry_for_benchmark(self.benchmark_def)

        self.assertTrue(self.benchmark_def.allowed_actions <= registry.allowed_ops())
        self.assertIn("FX_LOOKUP", registry.allowed_ops())
        self.assertIn("CPI_LOOKUP", registry.allowed_ops())

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
                oracle_ctx = make_oracle_context(bindings, compiled.lookup_map)
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
                    oracle_ctx=oracle_ctx,
                    capsule=capsule,
                    cache={},
                )

                self.assertEqual(result["output"], compiled.answer)
                self.assertTrue(compiled.operators)


if __name__ == "__main__":
    unittest.main()
