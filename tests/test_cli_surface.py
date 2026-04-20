from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from TRACE.cli import __main__ as trace_cli
from TRACE.cli.benchmark_tools import _resolve_tools, main as benchmark_tools_main


class CliSurfaceTests(unittest.TestCase):
    def test_trace_cli_dispatcher_exposes_public_commands(self) -> None:
        self.assertEqual(
            set(trace_cli.COMMANDS),
            {"generate", "run", "run_sweep", "compare", "benchmark_tools"},
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


if __name__ == "__main__":
    unittest.main()
