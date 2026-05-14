from __future__ import annotations

import json
import tempfile
import unittest
from importlib import import_module
from pathlib import Path

from TRACE.core.compiler.lower import (
    compile_spec,
    evaluate_compiled_plan_oracle,
    lower_spec,
)
from TRACE.reporting.evaluation import compare_outputs
from TRACE.reporting.results import RunConfig, write_result_row

from tests.support import find_record, load_trace_ufr_benchmark, load_trace_ufr_extracts


class CompilerAndReportingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.benchmark_def = load_trace_ufr_benchmark()
        self.extracts = load_trace_ufr_extracts()

    def test_compile_spec_is_deterministic_for_fixed_seed(self) -> None:
        registry = import_module(self.benchmark_def.templates_module)
        spec = registry.SPECS_BY_ID["L0_LOOKUP__SCALE__FORCE_NON_NOOP"]
        record = find_record(
            self.extracts,
            company="Alphabet Inc.",
            label="revenue",
            period_kind="FY",
            period_value=2022,
        )
        bindings = {"A": record}

        first = compile_spec(spec, bindings, self.benchmark_def, seed=7)
        second = compile_spec(spec, bindings, self.benchmark_def, seed=7)

        self.assertEqual(first.dag, second.dag)
        self.assertEqual(first.answer, second.answer)
        self.assertNotEqual(first.answer["scale"], record.scale)

    def test_lower_spec_separates_lowering_from_oracle_answer(self) -> None:
        registry = import_module(self.benchmark_def.templates_module)
        spec = registry.SPECS_BY_ID["L0_LOOKUP__PLAIN"]
        record = find_record(
            self.extracts,
            company="Alphabet Inc.",
            label="revenue",
            period_kind="FY",
            period_value=2022,
        )
        bindings = {"A": record}

        compiled = lower_spec(spec, bindings, self.benchmark_def, seed=0)
        answer = evaluate_compiled_plan_oracle(compiled, bindings, self.benchmark_def)

        self.assertIsNone(compiled.answer)
        self.assertEqual(answer["value"], record.value)
        self.assertEqual(compiled.fact_map["n1"], record.extraction_id)

    def test_write_result_row_classifies_scale_only_mismatch(self) -> None:
        capsule = {"qid": "q1", "meta": {"template_id": "L0_LOOKUP__PLAIN"}}
        output = {"value": 2.0, "unit": "USD", "scale": 1_000.0, "type": "money"}
        gold = {"value": 2000.0, "unit": "USD", "scale": 1.0, "type": "money"}

        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "results.jsonl"
            write_result_row(
                out_path,
                capsule=capsule,
                cfg=RunConfig(mode="oracle", planner="gold"),
                ok=False,
                output=output,
                gold=gold,
                trace=[],
            )

            [row] = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]

        self.assertEqual(row["mismatch_kind"], "scale_mismatch_only")
        self.assertIsNone(row["exec_error_code"])
        self.assertEqual(row["comparison"]["is_quantity"], True)
        self.assertEqual(row["comparison"]["type_match"], True)
        self.assertEqual(row["comparison"]["unit_match"], True)
        self.assertEqual(row["comparison"]["scale_match"], False)
        self.assertEqual(row["comparison"]["raw_value_match"], False)
        self.assertEqual(row["comparison"]["base_value_match"], True)
        self.assertEqual(row["comparison"]["output_base_value"], 2000.0)
        self.assertEqual(row["comparison"]["gold_base_value"], 2000.0)

    def test_write_result_row_preserves_exec_error_code(self) -> None:
        capsule = {"qid": "q2", "meta": {"template_id": "A0_ADD__SAME_COMPANY_DIFF_LABEL"}}
        exec_error = {"code": "E_bad_ref", "message": "Unknown ref ref:n9"}

        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "results.jsonl"
            write_result_row(
                out_path,
                capsule=capsule,
                cfg=RunConfig(mode="full", planner="model", model="demo"),
                ok=False,
                output=None,
                gold=None,
                trace=None,
                exec_error=exec_error,
                extra={},
            )

            [row] = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]

        self.assertEqual(row["exec_error_code"], "E_bad_ref")
        self.assertIsNone(row["mismatch_kind"])

    def test_compare_outputs_distinguishes_semantic_and_representation_mismatches(self) -> None:
        correct_wrong_scale = compare_outputs(
            {"value": 2.0, "unit": "USD", "scale": 1_000.0, "type": "money"},
            {"value": 2000.0, "unit": "USD", "scale": 1.0, "type": "money"},
        )
        wrong_value_right_scale = compare_outputs(
            {"value": 2.0, "unit": "USD", "scale": 1.0, "type": "money"},
            {"value": 3.0, "unit": "USD", "scale": 1.0, "type": "money"},
        )
        wrong_value_wrong_scale = compare_outputs(
            {"value": 2.0, "unit": "USD", "scale": 10.0, "type": "money"},
            {"value": 3.0, "unit": "USD", "scale": 1.0, "type": "money"},
        )

        self.assertTrue(correct_wrong_scale.correct)
        self.assertEqual(correct_wrong_scale.mismatch_kind, "scale_mismatch_only")
        self.assertFalse(wrong_value_right_scale.correct)
        self.assertEqual(wrong_value_right_scale.mismatch_kind, "semantic_value_mismatch")
        self.assertFalse(wrong_value_wrong_scale.correct)
        self.assertEqual(wrong_value_wrong_scale.mismatch_kind, "value_and_scale_mismatch")

    def test_compare_outputs_uses_tight_relative_tolerance_for_large_values(self) -> None:
        floating_point_dust = compare_outputs(
            {"value": 1273741600.0000002, "unit": "USD", "scale": 1.0, "type": "money"},
            {"value": 1273741600.0, "unit": "USD", "scale": 1.0, "type": "money"},
        )
        material_difference = compare_outputs(
            {"value": 1273741600.01, "unit": "USD", "scale": 1.0, "type": "money"},
            {"value": 1273741600.0, "unit": "USD", "scale": 1.0, "type": "money"},
        )

        self.assertTrue(floating_point_dust.correct)
        self.assertLess(floating_point_dust.details["base_value_rel_diff"], 1e-12)
        self.assertFalse(material_difference.correct)
        self.assertEqual(material_difference.mismatch_kind, "semantic_value_mismatch")

    def test_compare_outputs_normalizes_relation_set_surface_variants(self) -> None:
        output = {
            "type": "relation_set",
            "label": "causes_side_effect",
            "object_type": "effect",
            "items": [
                {
                    "object": {
                        "type": "effect",
                        "value": "Stevens-Johnson syndrome",
                    }
                },
                {"object": {"type": "effect", "value": "liver problems"}},
            ],
        }
        gold = {
            "type": "relation_set",
            "label": "causes_side_effect",
            "object_type": "effect",
            "items": [
                {
                    "object": {
                        "type": "effect",
                        "value": "stevens johnson syndrome",
                    }
                },
                {"object": {"type": "effect", "value": "liver problem"}},
            ],
        }

        comparison = compare_outputs(output, gold)

        self.assertTrue(comparison.correct)
        self.assertIsNone(comparison.mismatch_kind)


if __name__ == "__main__":
    unittest.main()
