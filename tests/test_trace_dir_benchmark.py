from __future__ import annotations

import unittest
from importlib import import_module

from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.core.executor.runtime import execute_dag
from TRACE.generation.compiler import compile_spec
from TRACE.generation.sampler import sample_k_bindings_fast
from TRACE.providers.shared.prompt import build_planner_prompt
from TRACE.reporting.results import RunConfig, build_result_row


class TraceDirBenchmarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.benchmark_def = load_benchmark("trace_dir")
        cls.extracts = cls.benchmark_def.load_extracts(cls.benchmark_def.extracts_dir)
        cls.registry = import_module(cls.benchmark_def.templates_module)

    def test_trace_dir_loads_relation_extracts(self) -> None:
        self.assertTrue(self.extracts)
        record = self.extracts[0]
        self.assertEqual(record.qtype, "relation")
        self.assertEqual(record.slot("subject_type"), "drug")
        self.assertIn(record.label, self.benchmark_def.load_allowed_labels(self.benchmark_def.schemas_dir))

    def test_every_trace_dir_template_samples_compiles_and_executes(self) -> None:
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

                self.assertTrue(compiled.answer)
                self.assertIn(compiled.dag["nodes"][-1]["op"], self.benchmark_def.allowed_actions)

    def test_trace_dir_prompt_uses_relation_schema_and_set_ops(self) -> None:
        capsule = {
            "question": "What side effects are stated for Aspirin?",
            "context": {
                "snippets": [
                    {
                        "snippet_id": "aspirin_skin_01",
                        "text": "Taking aspirin can result in hives and headache.",
                    }
                ]
            },
        }

        prompt = build_planner_prompt(capsule, benchmark_def=self.benchmark_def)

        model_fact_section = prompt.split("MODEL_FACT SCHEMA:", 1)[1].split(
            "ALLOWED LABELS:", 1
        )[0]
        self.assertIn("SET_CONTAINS", prompt)
        self.assertIn("MAKE_SET", prompt)
        self.assertIn("exactly one atomic extracted fact", prompt)
        self.assertIn("one relation: one subject, one label, and one object", prompt)
        self.assertIn("extract ALL directly stated valid relations", prompt)
        self.assertIn("Do not stop after a representative subset", prompt)
        self.assertIn("VALID OBJECT VALUES IN THIS CONTEXT", prompt)
        self.assertIn("do not invent alternate object wording", prompt)
        self.assertIn('"subject"', model_fact_section)
        self.assertIn('"object"', model_fact_section)
        self.assertNotIn('"quantity"', model_fact_section)

    def test_trace_dir_prompt_lists_valid_context_object_values(self) -> None:
        capsule = {
            "question": "What side effects are stated for Atorvastatin?",
            "context": {
                "snippets": [
                    {
                        "snippet_id": "atorvastatin_side_effects_other_05",
                        "text": "stub",
                    }
                ]
            },
        }

        prompt = build_planner_prompt(capsule, benchmark_def=self.benchmark_def)

        object_values_section = prompt.split(
            "VALID OBJECT VALUES IN THIS CONTEXT:",
            1,
        )[1].split("MINIMALITY:", 1)[0]
        self.assertIn('"causes_side_effect"', object_values_section)
        self.assertIn("unusual bruising", object_values_section)
        self.assertNotIn('"treats_condition"', object_values_section)

    def test_trace_dir_result_row_reports_relation_fact_metrics(self) -> None:
        spec = self.registry.SPECS_BY_ID["L0_RELATION_SET__SAME_DRUG_SAME_LABEL_2X"]
        [bindings] = sample_k_bindings_fast(
            spec,
            self.extracts,
            k=1,
            benchmark_def=self.benchmark_def,
            seed=0,
            replace=True,
        )
        compiled = compile_spec(spec, bindings, self.benchmark_def, seed=0)
        capsule = {
            "qid": "trace_dir_metrics",
            "question": spec.render_question(bindings, compiled),
            "context": {"snippets": []},
            "gold": {
                "dag": compiled.dag,
                "answer": compiled.answer,
                "fact_map": compiled.fact_map,
            },
            "meta": {"benchmark_id": "trace_dir", "template_id": spec.template_id},
        }
        from TRACE.core.compiler.lower import hydrate_compiled_context

        capsule["context"]["snippets"] = hydrate_compiled_context(
            compiled,
            self.benchmark_def,
        )
        result = execute_dag(
            compiled.dag,
            self.benchmark_def,
            capsule=capsule,
            cache={},
        )

        row = build_result_row(
            capsule=capsule,
            cfg=RunConfig(mode="full", planner="gold"),
            ok=True,
            output=result["output"],
            gold=compiled.answer,
            trace=result["trace"],
            extra={"exec_dag": compiled.dag},
        )

        self.assertEqual(row["fact_prec"], 1.0)
        self.assertEqual(row["fact_rec"], 1.0)
        self.assertEqual(row["fact_f1"], 1.0)
        self.assertEqual(row["fact_gold_n"], len(compiled.fact_map))
        self.assertEqual(row["fact_pred_n"], len(set(compiled.fact_map.values())))
        self.assertGreaterEqual(row["fact_gold_n"], 2)
        self.assertEqual(row["anchored_fact_match_count"], len(set(compiled.fact_map.values())))
        self.assertTrue(row["anchored_graph_match"])
        self.assertEqual(row["intermediate_value_match_rate"], 1.0)
        self.assertEqual(set(row["fact_pred_extraction_ids"]), set(compiled.fact_map.values()))

    def test_trace_dir_relation_set_templates_compile_exhaustive_gold_sets(self) -> None:
        spec = self.registry.SPECS_BY_ID["L0_RELATION_SET__SAME_DRUG_SAME_LABEL_2X"]
        [bindings] = sample_k_bindings_fast(
            spec,
            self.extracts,
            k=1,
            benchmark_def=self.benchmark_def,
            seed=0,
            replace=True,
        )
        compiled = compile_spec(spec, bindings, self.benchmark_def, seed=0)
        anchor = bindings["A"]
        expected_ids = {
            record.extraction_id
            for record in self.extracts
            if record.snippet_id == anchor.snippet_id
            and record.label == anchor.label
            and record.slot("subject_value") == anchor.slot("subject_value")
            and record.slot("object_type") == anchor.slot("object_type")
        }

        self.assertEqual(set(compiled.fact_map.values()), expected_ids)
        self.assertGreaterEqual(len(expected_ids), 2)

    def test_trace_dir_capsule_context_snippets_are_deterministically_shuffled(self) -> None:
        import json
        import tempfile
        from pathlib import Path
        from unittest.mock import patch

        from TRACE.generation.cli_generate_corpus import main as generate_corpus_main

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "corpus"
            with patch(
                "sys.argv",
                [
                    "generate",
                    "--benchmark",
                    "trace_dir",
                    "--out",
                    str(out),
                    "--distractors",
                    "3",
                    "--n-total",
                    "20",
                    "--seed",
                    "0",
                    "--balance-templates",
                ],
            ):
                generate_corpus_main()

            shuffled_examples = 0
            for path in sorted((out / "d=3").glob("*.json")):
                capsule = json.loads(path.read_text(encoding="utf-8"))
                relevant = list(dict.fromkeys(capsule["meta"]["snippet_ids"]))
                observed = [snippet["snippet_id"] for snippet in capsule["context"]["snippets"]]
                self.assertEqual(observed, capsule["meta"]["context_snippet_ids"])
                if observed[: len(relevant)] != relevant:
                    shuffled_examples += 1

            self.assertGreater(shuffled_examples, 0)


if __name__ == "__main__":
    unittest.main()
