from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from TRACE.cli import __main__ as trace_cli
from TRACE.cli.benchmark_tools import _resolve_tools, main as benchmark_tools_main
from TRACE.generation.cli_generate_corpus import main as generate_corpus_main


class CliSurfaceTests(unittest.TestCase):
    def test_trace_cli_dispatcher_exposes_public_commands(self) -> None:
        self.assertEqual(
            set(trace_cli.COMMANDS),
            {"generate", "run", "run_sweep", "benchmark_tools"},
        )

    def test_benchmark_tools_are_benchmark_owned(self) -> None:
        tools = _resolve_tools("trace_ufr")

        self.assertEqual(
            tools,
            {
                "prepare_extracts": "benchmarks.trace_ufr.tools.prepare_extracts",
                "generate_rates": "benchmarks.trace_ufr.tools.generate_rates",
                "list_currencies": "benchmarks.trace_ufr.tools.list_currencies",
            },
        )

    def test_benchmark_tools_list_prints_registered_tools(self) -> None:
        stdout = io.StringIO()
        with patch("sys.argv", ["benchmark_tools", "list", "--benchmark", "trace_ufr"]):
            with redirect_stdout(stdout):
                benchmark_tools_main()

        text = stdout.getvalue()
        self.assertIn("prepare_extracts", text)
        self.assertIn("generate_rates", text)
        self.assertIn("list_currencies", text)

    def test_trace_cli_help_includes_benchmark_tools(self) -> None:
        stdout = io.StringIO()
        with patch("sys.argv", ["TRACE.cli", "--help"]):
            with redirect_stdout(stdout):
                trace_cli.main()

        self.assertIn("benchmark_tools", stdout.getvalue())

    def test_generate_can_balance_evenly_across_templates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "corpus"
            with patch(
                "sys.argv",
                [
                    "generate",
                    "--benchmark",
                    "trace_ufr",
                    "--out",
                    str(out),
                    "--distractors",
                    "0",
                    "--n-total",
                    "31",
                    "--seed",
                    "0",
                    "--balance-templates",
                ],
            ):
                generate_corpus_main()

            counts: dict[str, int] = {}
            for line in (out / "capsules.jsonl").read_text(encoding="utf-8").splitlines():
                row = json.loads(line)
                counts[row["template_id"]] = counts.get(row["template_id"], 0) + 1

            self.assertEqual(len(counts), 31)
            self.assertEqual(set(counts.values()), {1})


if __name__ == "__main__":
    unittest.main()
