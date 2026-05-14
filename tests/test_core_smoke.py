from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from TRACE.core.benchmarks.types import BenchmarkDef, PromptGuidance
from TRACE.core.compiler.lower import lower_spec
from TRACE.core.executor.runtime import execute_dag
from TRACE.generation.expr import LookupQty
from TRACE.generation.generation_types import ExtractRecord, Spec, VarSpec
from TRACE.generation.sampler import sample_k_bindings_fast
from TRACE.reporting.results import RunConfig, write_result_row
from TRACE.reporting.summary import write_run_summary_artifacts


def _benchmark() -> BenchmarkDef:
    return BenchmarkDef(
        benchmark_id="core_smoke",
        asset_root=Path("."),
        snippets_dir=Path("."),
        extracts_dir=Path("."),
        schemas_dir=Path("."),
        tables_dir=None,
        templates_module="",
        allowed_actions={"MODEL_FACT"},
        register_actions=lambda _registry: None,
        load_extracts=lambda _path: [],
        load_allowed_labels=lambda _path: ["revenue"],
        derive_slots=lambda record: dict(record.slots),
        build_planner_prompt=None,
        build_exists_key=None,
        sampler_constraint_vars=None,
        sampler_constraint_ok=None,
        prompt_guidance=PromptGuidance(),
    )


def _record() -> ExtractRecord:
    return ExtractRecord(
        extraction_id="rev_2024",
        snippet_id="snippet_rev_2024",
        label="revenue",
        period={"period": "FY", "value": 2024},
        quantity={"value": 42.0, "unit": "USD", "scale": 1_000_000.0, "type": "money"},
        company="Acme",
        metric_key="revenue",
        metric_role="amount",
        slots={"company": "Acme", "metric_key": "revenue"},
    )


class CoreSmokeTests(unittest.TestCase):
    def test_local_fixture_can_sample_lower_execute_and_report(self) -> None:
        benchmark_def = _benchmark()
        spec = Spec(
            template_id="CORE_LOOKUP_SMOKE",
            vars={"A": VarSpec(qtype_in=["money"])},
            ast=LookupQty("A"),
            render_question=lambda _bindings, _compiled: "What was revenue?",
            constraints=[],
        )

        [bindings] = sample_k_bindings_fast(
            spec,
            [_record()],
            k=1,
            benchmark_def=benchmark_def,
            seed=0,
            replace=True,
        )
        compiled = lower_spec(spec, bindings, benchmark_def=benchmark_def, seed=0)
        result = execute_dag(
            compiled.dag,
            benchmark_def,
            "oracle",
            provider_ctx=None,
            capsule={
                "qid": "core-smoke",
                "meta": {"benchmark_id": "core_smoke", "template_id": spec.template_id},
                "context": {"snippets": [{"snippet_id": "snippet_rev_2024", "text": "Revenue was 42 million USD."}]},
            },
            cache={},
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            results_path = out_dir / "results.jsonl"
            write_result_row(
                results_path,
                capsule={
                    "qid": "core-smoke",
                    "meta": {"benchmark_id": "core_smoke", "template_id": spec.template_id},
                },
                cfg=RunConfig(mode="oracle", planner="gold"),
                ok=True,
                output=result["output"],
                gold=result["output"],
                trace=result["trace"],
            )
            summary = write_run_summary_artifacts(results_path, out_dir=out_dir)

            [row] = [
                json.loads(line)
                for line in results_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertTrue(row["correct"])
            self.assertEqual(row["ops"], {"MODEL_FACT": 1})
            self.assertEqual(summary["overall"]["accuracy"], 1.0)
            self.assertTrue((out_dir / "summary.json").exists())
            self.assertTrue((out_dir / "summary.md").exists())


if __name__ == "__main__":
    unittest.main()
