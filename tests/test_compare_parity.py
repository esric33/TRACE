from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from TRACE.cli.compare_parity import _compare_corpus_dirs


class CompareParityTests(unittest.TestCase):
    def test_compare_corpus_dirs_ignores_benchmark_profile_artifacts(self) -> None:
        capsule = {
            "qid": "q1",
            "question": "demo",
            "context": {"snippets": []},
            "gold": {"lookup_map": {}, "answer": {"value": 1}, "dag": {"nodes": [], "output": "ref:n1"}},
            "meta": {"template_id": "T1", "family": "L0"},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            legacy = root / "legacy"
            refactor = root / "refactor"
            legacy.mkdir()
            refactor.mkdir()

            legacy_meta = {"corpus_id": "demo", "extracts_dir": "/tmp/ex", "snippets_dir": "/tmp/sn"}
            refactor_meta = {
                "corpus_id": "demo",
                "extracts_dir": "/tmp/ex",
                "snippets_dir": "/tmp/sn",
                "benchmark_profile": {
                    "json": "benchmark_profile.json",
                    "md": "benchmark_profile.md",
                    "total_queries": 1,
                },
            }

            (legacy / "meta.json").write_text(json.dumps(legacy_meta), encoding="utf-8")
            (refactor / "meta.json").write_text(json.dumps(refactor_meta), encoding="utf-8")
            (legacy / "q1.json").write_text(json.dumps(capsule), encoding="utf-8")
            (refactor / "q1.json").write_text(json.dumps(capsule), encoding="utf-8")
            (refactor / "benchmark_profile.json").write_text(
                json.dumps({"total_queries": 1}),
                encoding="utf-8",
            )

            ok, message = _compare_corpus_dirs(legacy, refactor)

        self.assertTrue(ok)
        self.assertEqual(message, "corpus parity ok")


if __name__ == "__main__":
    unittest.main()
