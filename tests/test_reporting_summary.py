from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from TRACE.reporting.results import RunConfig, write_result_row
from TRACE.reporting.summary import summarize_results_file, write_run_summary_artifacts


def _sample_quantity(value: float) -> dict:
    return {"value": value, "unit": "USD", "scale": 1.0, "type": "money"}


def _sample_fact(*, snippet_id: str = "s_gold", value: float = 12.0) -> dict:
    return {
        "snippet_id": snippet_id,
        "label": "revenue",
        "period": {"period": "FY", "value": 2022},
        "quantity": _sample_quantity(value),
    }


def _full_mode_capsule(*, qid: str, family: str, template_id: str, extraction_id: str) -> dict:
    return {
        "qid": qid,
        "gold": {
            "answer": _sample_quantity(12.0),
            "fact_map": {"n1": extraction_id},
            "dag": {
                "nodes": [
                    {"id": "n1", "op": "MODEL_FACT", "args": _sample_fact()},
                    {"id": "n2", "op": "CONST", "args": {"value": 1}},
                    {"id": "n3", "op": "ADD", "args": {"a": "ref:n1", "b": "ref:n2"}},
                ],
                "output": "ref:n3",
            },
        },
        "meta": {
            "benchmark_id": "trace_ufr",
            "family": family,
            "template_id": template_id,
            "distractor_policy": "D0",
            "seed": 1,
            "generator_version": "gen_v1",
            "snippet_ids": ["s_gold"],
            "extraction_ids": [extraction_id],
            "labels": ["revenue"],
            "periods": [{"period": "FY", "value": 2022}],
        },
    }


class ReportingSummaryTests(unittest.TestCase):
    def test_write_result_row_emits_diagnostic_fields_for_full_mode(self) -> None:
        capsule = _full_mode_capsule(
            qid="q1",
            family="A0",
            template_id="A0_ADD__ONE",
            extraction_id="ex1",
        )
        trace = [
            {
                "node": "n1",
                "op": "MODEL_FACT",
                "args": _sample_fact(snippet_id="s_other", value=10.0),
                "result": {**_sample_quantity(10.0), "source": {"snippet_id": "s_other", "label": "revenue", "period": {"period": "FY", "value": 2022}}},
            }
        ]
        exec_dag = {
            "nodes": [
                {"id": "n1", "op": "MODEL_FACT", "args": _sample_fact(snippet_id="s_other", value=10.0)},
                {"id": "n2", "op": "CONST", "args": {"value": 1}},
                {"id": "n3", "op": "GT", "args": {"a": "ref:n1", "b": "ref:n2"}},
            ],
            "output": "ref:n3",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "results.jsonl"
            write_result_row(
                out_path,
                capsule=capsule,
                cfg=RunConfig(mode="full", planner="openai", model="demo"),
                ok=False,
                output=_sample_quantity(10.0),
                gold=_sample_quantity(12.0),
                trace=trace,
                exec_error=None,
                extra={"benchmark_id": "trace_ufr", "exec_dag": exec_dag},
            )

            [row] = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]

        self.assertEqual(row["benchmark_id"], "trace_ufr")
        self.assertEqual(row["failure_stage"], "under_grounded")
        self.assertTrue(row["dag_wrong_output_op"])
        self.assertEqual(row["dag_missing_node_types"], {"ADD": 1})
        self.assertEqual(row["dag_extra_node_types"], {"GT": 1})
        self.assertFalse(row["fact_exact"])
        self.assertEqual(row["fact_under_extraction"], 1)
        self.assertEqual(row["fact_over_extraction"], 1)
        self.assertEqual(row["fact_error_tag_counts"], {"fact_no_match": 1, "fact_wrong_snippet": 1})
        self.assertEqual(row["fact_resolution_status_counts"], {"no_match": 1})
        self.assertEqual(row["fact_gold_extraction_ids"], ["ex1"])
        self.assertEqual(row["fact_pred_extraction_ids"], [None])
        self.assertEqual(row["fact_trace"][0]["model_fact"], _sample_fact(snippet_id="s_other", value=10.0))

    def test_write_run_summary_artifacts_aggregates_failures_and_slices(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "results.jsonl"

            capsule_a = _full_mode_capsule(
                qid="q1",
                family="A0",
                template_id="A0_ADD__ONE",
                extraction_id="ex1",
            )
            trace_a = [
                {
                    "node": "n1",
                    "op": "MODEL_FACT",
                    "args": _sample_fact(snippet_id="s_other", value=10.0),
                    "result": {**_sample_quantity(10.0), "source": {"snippet_id": "s_other", "label": "revenue", "period": {"period": "FY", "value": 2022}}},
                }
            ]
            exec_dag_a = {
                "nodes": [
                    {"id": "n1", "op": "MODEL_FACT", "args": _sample_fact(snippet_id="s_other", value=10.0)},
                    {"id": "n2", "op": "CONST", "args": {"value": 1}},
                    {"id": "n3", "op": "GT", "args": {"a": "ref:n1", "b": "ref:n2"}},
                ],
                "output": "ref:n3",
            }
            write_result_row(
                out_path,
                capsule=capsule_a,
                cfg=RunConfig(mode="full", planner="openai", model="demo"),
                ok=False,
                output=_sample_quantity(10.0),
                gold=_sample_quantity(12.0),
                trace=trace_a,
                extra={"benchmark_id": "trace_ufr", "exec_dag": exec_dag_a},
            )

            capsule_b = _full_mode_capsule(
                qid="q2",
                family="B0",
                template_id="B0_GT__ONE",
                extraction_id="ex2",
            )
            write_result_row(
                out_path,
                capsule=capsule_b,
                cfg=RunConfig(mode="full", planner="openai", model="demo"),
                ok=False,
                output=None,
                gold=None,
                trace=[],
                exec_error={
                    "code": "E_planner_invalid",
                    "message": "bad planner output",
                    "data": {"phase": "planner", "provider": "openai"},
                },
                extra={"benchmark_id": "trace_ufr", "exec_dag": {}},
            )

            capsule_c = _full_mode_capsule(
                qid="q3",
                family="A0",
                template_id="A0_ADD__ONE",
                extraction_id="ex3",
            )
            write_result_row(
                out_path,
                capsule=capsule_c,
                cfg=RunConfig(mode="oracle", planner="gold"),
                ok=True,
                output=_sample_quantity(12.0),
                gold=_sample_quantity(12.0),
                trace=[],
                extra={"benchmark_id": "trace_ufr"},
            )

            summary = write_run_summary_artifacts(out_path, out_dir=Path(tmpdir))
            summary_from_file = summarize_results_file(out_path)
            summary_json = json.loads((Path(tmpdir) / "summary.json").read_text(encoding="utf-8"))
            summary_md = (Path(tmpdir) / "summary.md").read_text(encoding="utf-8")

        self.assertEqual(summary["total_examples"], 3)
        self.assertEqual(summary_from_file["overall"]["failure_funnel"]["counts"]["planner_invalid"], 1)
        self.assertEqual(summary_json["overall"]["failure_funnel"]["counts"]["under_grounded"], 1)
        self.assertEqual(summary_json["overall"]["failure_funnel"]["counts"]["correct"], 1)
        self.assertEqual(summary_json["overall"]["error_breakdowns"]["exec_error_code"]["E_planner_invalid"], 1)
        self.assertEqual(
            summary_json["overall"]["diagnostics"]["fact_error_tag_counts"]["fact_wrong_snippet"],
            1,
        )
        self.assertEqual(
            summary_json["slices"]["family"]["A0"]["incorrect"],
            1,
        )
        self.assertEqual(
            summary_json["slices"]["family"]["A0"]["total_examples"],
            2,
        )
        self.assertTrue(summary_json["top_failing_slices"])
        self.assertIn("# Run Summary", summary_md)
        self.assertIn("Top Failure Modes", summary_md)


if __name__ == "__main__":
    unittest.main()
