from __future__ import annotations

import argparse
from importlib import import_module
from typing import Callable

from TRACE.core.benchmarks.loader import load_benchmark


def _resolve_tools(benchmark: str) -> dict[str, str]:
    benchmark_def = load_benchmark(benchmark)
    if benchmark_def.list_maintenance_tools is None:
        return {}
    return dict(benchmark_def.list_maintenance_tools())


def _load_tool_main(module_name: str) -> Callable[[], None]:
    module = import_module(module_name)
    try:
        return getattr(module, "main")
    except AttributeError as exc:
        raise SystemExit(f"Tool module has no main(): {module_name}") from exc


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="command", required=True)

    list_parser = sub.add_parser("list")
    list_parser.add_argument("--benchmark", default="trace_ufr")

    run_parser = sub.add_parser("run")
    run_parser.add_argument("--benchmark", default="trace_ufr")
    run_parser.add_argument("tool")

    args = ap.parse_args()
    tools = _resolve_tools(args.benchmark)

    if args.command == "list":
        for name in sorted(tools):
            print(f"{name}\t{tools[name]}")
        return

    module_name = tools.get(args.tool)
    if module_name is None:
        raise SystemExit(
            f"Unknown maintenance tool {args.tool!r} for benchmark {args.benchmark!r}"
        )
    _load_tool_main(module_name)()


if __name__ == "__main__":
    main()

