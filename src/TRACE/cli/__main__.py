from __future__ import annotations

import sys

from TRACE.cli import benchmark_tools, compare, generate, run, run_sweep


COMMANDS = {
    "generate": generate.main,
    "run": run.main,
    "run_sweep": run_sweep.main,
    "compare": compare.main,
    "benchmark_tools": benchmark_tools.main,
}


def _print_help() -> None:
    cmds = ", ".join(sorted(COMMANDS))
    print(f"Usage: python -m TRACE.cli <command> [...]\nCommands: {cmds}")


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] in {"-h", "--help"}:
        _print_help()
        return

    command = sys.argv[1]
    handler = COMMANDS.get(command)
    if handler is None:
        _print_help()
        raise SystemExit(f"Unknown TRACE.cli command: {command}")

    sys.argv = [f"{sys.argv[0]} {command}", *sys.argv[2:]]
    handler()


if __name__ == "__main__":
    main()
