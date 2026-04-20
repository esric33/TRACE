from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from TRACE.generation.profile import (
    build_benchmark_profile,
    write_benchmark_profile_artifacts,
)


class BenchmarkProfileTests(unittest.TestCase):
    def test_build_benchmark_profile_aggregates_templates_and_families(self) -> None:
        capsules = [
            {
                "qid": "q1",
                "context": {"snippets": [{"snippet_id": "s1"}]},
                "gold": {
                    "lookup_map": {"n1": "ex1"},
                    "dag": {
                        "nodes": [
                            {"id": "n1", "op": "TEXT_LOOKUP", "args": {"query": "x"}},
                            {"id": "n2", "op": "GET_QUANTITY", "args": {"fact": "ref:n1"}},
                            {"id": "n3", "op": "CONST", "args": {"value": 1}},
                            {"id": "n4", "op": "ADD", "args": {"a": "ref:n2", "b": "ref:n3"}},
                        ],
                        "output": "ref:n4",
                    },
                },
                "meta": {"family": "A0", "template_id": "A0_ADD__ONE"},
            },
            {
                "qid": "q2",
                "context": {"snippets": [{"snippet_id": "s2"}, {"snippet_id": "s3"}]},
                "gold": {
                    "lookup_map": {"n1": "ex2", "n2": "ex3"},
                    "dag": {
                        "nodes": [
                            {"id": "n1", "op": "TEXT_LOOKUP", "args": {"query": "x"}},
                            {"id": "n2", "op": "TEXT_LOOKUP", "args": {"query": "y"}},
                            {"id": "n3", "op": "GET_QUANTITY", "args": {"fact": "ref:n1"}},
                            {"id": "n4", "op": "GET_QUANTITY", "args": {"fact": "ref:n2"}},
                            {"id": "n5", "op": "GT", "args": {"a": "ref:n3", "b": "ref:n4"}},
                        ],
                        "output": "ref:n5",
                    },
                },
                "meta": {"family": "B0", "template_id": "B0_GT__ONE"},
            },
        ]

        profile = build_benchmark_profile(
            capsules,
            benchmark_id="trace_ufr",
            corpus_id="demo-corpus",
        )

        self.assertEqual(profile["total_queries"], 2)
        self.assertEqual(profile["total_templates"], 2)
        self.assertEqual(profile["total_families"], 2)
        self.assertEqual(profile["totals"]["snippets"], 3)
        self.assertEqual(profile["totals"]["lookup_bindings"], 3)
        self.assertEqual(profile["histograms"]["action_count"], {"4": 1, "5": 1})
        self.assertEqual(profile["histograms"]["lookup_count"], {"1": 1, "2": 1})
        self.assertEqual(profile["operator_counts"]["TEXT_LOOKUP"], 3)
        self.assertEqual(profile["per_family"]["A0"]["queries"], 1)
        self.assertEqual(profile["per_template"]["B0_GT__ONE"]["queries"], 1)

    def test_write_benchmark_profile_artifacts_emits_json_and_markdown(self) -> None:
        capsules = [
            {
                "qid": "q1",
                "context": {"snippets": [{"snippet_id": "s1"}]},
                "gold": {
                    "lookup_map": {"n1": "ex1"},
                    "dag": {
                        "nodes": [
                            {"id": "n1", "op": "TEXT_LOOKUP", "args": {"query": "x"}},
                            {"id": "n2", "op": "GET_QUANTITY", "args": {"fact": "ref:n1"}},
                        ],
                        "output": "ref:n2",
                    },
                },
                "meta": {"family": "L0", "template_id": "L0_LOOKUP__PLAIN"},
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            profile = write_benchmark_profile_artifacts(
                out_dir,
                capsules=capsules,
                benchmark_id="trace_ufr",
                corpus_id="demo-corpus",
            )

            json_profile = json.loads(
                (out_dir / "benchmark_profile.json").read_text(encoding="utf-8")
            )
            markdown_profile = (out_dir / "benchmark_profile.md").read_text(
                encoding="utf-8"
            )

        self.assertEqual(json_profile["total_queries"], 1)
        self.assertEqual(json_profile["corpus_id"], "demo-corpus")
        self.assertEqual(profile["benchmark_id"], "trace_ufr")
        self.assertIn("# Benchmark Profile", markdown_profile)
        self.assertIn("`L0_LOOKUP__PLAIN`", markdown_profile)


if __name__ == "__main__":
    unittest.main()
