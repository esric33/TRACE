# reason_bench/cli_run.py
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.core.executor.runtime import execute_dag
from TRACE.core.executor.support import (
    _q_norm,
    ExecError,
    exec_error_to_dict,
    load_extract_store,
)
from TRACE.reporting.results import RunConfig, write_result_row
from TRACE.shared.io import _clean_path, read_json, read_jsonl

from TRACE.providers.shared.base import load_schema_json


# --- add near top imports ---
from typing import Set


def _read_done_qids(results_path: str) -> Set[str]:
    p = Path(results_path)
    if not p.exists():
        return set()
    done: Set[str] = set()
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            qid = r.get("qid") or (r.get("extra", {}) or {}).get("qid")
            if qid:
                done.add(qid)
    return done


# -----------------------------------------------------------------------------
# Types
# -----------------------------------------------------------------------------


@dataclass
class RunOutcome:
    capsule: Dict[str, Any]
    ok: bool
    output: Any = None
    gold: Any = None
    dag: Optional[Dict[str, Any]] = None
    trace: Optional[list[dict]] = None
    trace_nodes: int = 0
    exec_error: Optional[Dict[str, Any]] = None


# -----------------------------------------------------------------------------
# Equality / IO helpers
# -----------------------------------------------------------------------------


def is_quantity_dict(x: Any) -> bool:
    return isinstance(x, dict) and {"value", "unit", "scale", "type"} <= set(x.keys())


def q_equal(a: Any, b: Any) -> bool:
    """
    Strict equality for "Quantity-like" dicts, direct equality otherwise.
    Ensures scale is float-normalized to avoid int/float drift.
    """
    if not is_quantity_dict(a) or not is_quantity_dict(b):
        return a == b

    a = _q_norm(a)
    b = _q_norm(b)

    a = {**a, "scale": float(a["scale"])}
    b = {**b, "scale": float(b["scale"])}

    return (
        a["type"] == b["type"]
        and a["unit"] == b["unit"]
        and a["scale"] == b["scale"]
        and a["value"] == b["value"]
    )


def iter_capsules(path: Path) -> Iterator[Dict[str, Any]]:
    if path.is_dir():
        for p in sorted(path.glob("*.json")):
            yield read_json(p)
    else:
        yield from read_jsonl(path)


def find_capsule(path: Path, qid: str) -> Dict[str, Any]:
    for c in iter_capsules(path):
        if c.get("qid") == qid:
            return c
    raise KeyError(f"qid not found: {qid}")


def iter_selected_capsules(args, cap_path: Path) -> Iterator[Dict[str, Any]]:
    if args.qid:
        yield find_capsule(cap_path, args.qid)
        return
    yield from iter_capsules(cap_path)


# -----------------------------------------------------------------------------
# Eval fn selection (oracle/retrieval/full)
# -----------------------------------------------------------------------------


def make_eval_fns(args, benchmark_def):
    plan_fn = None
    lookup_fn = None

    if args.mode == "oracle":
        from TRACE.providers.offline.lookup_offline import offline_lookup_fn

        return None, offline_lookup_fn  # plan_fn=None => use gold dag
    if args.mode in ("retrieval", "full"):
        if not args.model:
            raise SystemExit("--model is required for mode=retrieval/full")

        # load schema + cache (shared across providers)
        schema = load_schema_json(Path(args.schema))

        cache_path = Path(args.cache)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache = (
            json.loads(cache_path.read_text(encoding="utf-8"))
            if cache_path.exists()
            else {}
        )

        # ----------------------------
        # OPENAI
        # ----------------------------
        if args.provider == "openai":
            from openai import OpenAI

            client = OpenAI()

            from TRACE.providers.openai.lookup_openai import openai_lookup_fn

            def lookup(nid, query, capsule, extracts_by_snippet):
                return openai_lookup_fn(
                    nid,
                    query,
                    capsule,
                    extracts_by_snippet,
                    client=client,
                    model=args.model,
                    schema=schema,
                    cache=cache,
                    cache_path=cache_path,
                    benchmark_def=benchmark_def,
                )

            lookup_fn = lookup

            if args.mode == "full":
                from TRACE.providers.openai.planner_openai import openai_plan_fn

                def plan(capsule):
                    return openai_plan_fn(
                        capsule,
                        client=client,
                        model=args.model,
                        benchmark_def=benchmark_def,
                    )

                plan_fn = plan
            else:
                plan_fn = None

            return plan_fn, lookup_fn

        # ----------------------------
        # ANTHROPIC
        # ----------------------------
        if args.provider == "anthropic":
            from TRACE.providers.anthropic._client import make_client

            client = make_client()

            from TRACE.providers.anthropic.lookup_anthropic import (
                anthropic_lookup_fn,
            )

            def lookup(nid, query, capsule, extracts_by_snippet):
                return anthropic_lookup_fn(
                    nid,
                    query,
                    capsule,
                    extracts_by_snippet,
                    client=client,
                    model=args.model,
                    schema=schema,
                    cache=cache,
                    cache_path=cache_path,
                    benchmark_def=benchmark_def,
                )

            lookup_fn = lookup

            if args.mode == "full":
                from TRACE.providers.anthropic.planner_anthropic import (
                    anthropic_plan_fn,
                )

                def plan(capsule):
                    return anthropic_plan_fn(
                        capsule,
                        client=client,
                        model=args.model,
                        benchmark_def=benchmark_def,
                    )

                plan_fn = plan
            else:
                plan_fn = None

            return plan_fn, lookup_fn

        # ----------------------------
        # GEMINI
        # ----------------------------
        if args.provider == "gemini":
            from TRACE.providers.gemini._client import make_client

            client = make_client()

            from TRACE.providers.gemini.lookup_gemini import gemini_lookup_fn

            def lookup(nid, query, capsule, extracts_by_snippet):
                return gemini_lookup_fn(
                    nid,
                    query,
                    capsule,
                    extracts_by_snippet,
                    client=client,
                    model=args.model,
                    schema=schema,
                    cache=cache,
                    cache_path=cache_path,
                    benchmark_def=benchmark_def,
                )

            lookup_fn = lookup

            if args.mode == "full":
                from TRACE.providers.gemini.planner_gemini import gemini_plan_fn

                def plan(capsule):
                    return gemini_plan_fn(
                        capsule,
                        client=client,
                        model=args.model,
                        benchmark_def=benchmark_def,
                    )

                plan_fn = plan
            else:
                plan_fn = None

            return plan_fn, lookup_fn

        raise SystemExit(f"Unknown provider: {args.provider}")
    raise SystemExit(f"Unknown mode: {args.mode}")


# -----------------------------------------------------------------------------
# Run logic
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class ProviderContext:
    lookup_fn: Any
    extracts_by_snippet: Dict[str, list[dict]]


def run_one(capsule, extracts_by_snippet, *, plan_fn, lookup_fn, benchmark_def) -> RunOutcome:
    dag = None
    gold = capsule.get("gold", {}).get("answer")
    trace = []

    try:
        dag = capsule["gold"]["dag"] if plan_fn is None else plan_fn(capsule)

        res = execute_dag(
            dag=dag,
            benchmark_def=benchmark_def,
            mode="provider",
            provider_ctx=ProviderContext(
                lookup_fn=lookup_fn,
                extracts_by_snippet=extracts_by_snippet,
            ),
            oracle_ctx=None,
            capsule=capsule,
            cache={},
        )

        out = res["output"]
        trace = res.get("trace", [])
        gold_cmp = _q_norm(gold) if is_quantity_dict(gold) else gold
        ok = q_equal(out, gold_cmp)

        return RunOutcome(
            capsule=capsule,
            ok=ok,
            output=out,
            gold=gold,
            dag=dag,
            trace=trace,
            trace_nodes=len(trace),
            exec_error=None,
        )

    except ExecError as e:
        return RunOutcome(
            capsule=capsule,
            ok=False,
            output=None,
            gold=gold,
            dag=dag,  # <-- keep the planned dag if we got it
            trace=trace or [],
            trace_nodes=len(trace) if trace else 0,
            exec_error=exec_error_to_dict(e),
        )


def maybe_dump_trace(
    outcome: RunOutcome, dump_trace_dir: Optional[Path], *, include_trace_on_pass: bool
) -> None:
    if dump_trace_dir is None:
        return

    if (not include_trace_on_pass) and outcome.ok:
        return

    dump_trace_dir.mkdir(parents=True, exist_ok=True)

    qid = outcome.capsule.get("qid", "unknown_qid")
    trace_path = dump_trace_dir / f"{qid}.trace.json"

    trace_blob = {
        "qid": qid,
        "question": outcome.capsule.get("question"),
        "dag": outcome.dag,
        "trace": outcome.trace or [],
        "output": outcome.output,
        "gold": outcome.capsule.get("gold", {}).get("answer"),
        "exec_error": outcome.exec_error,
    }
    trace_path.write_text(
        json.dumps(trace_blob, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def maybe_write_result(outcome: RunOutcome, args, cfg: RunConfig) -> None:
    if not args.results_out:
        return
    write_result_row(
        args.results_out,
        capsule=outcome.capsule,
        cfg=cfg,
        ok=outcome.ok,
        output=outcome.output,
        gold=_q_norm(outcome.gold) if is_quantity_dict(outcome.gold) else outcome.gold,
        trace=outcome.trace or [],
        exec_error=outcome.exec_error,
        extra={
            "qid": outcome.capsule.get("qid"),
            "family": outcome.capsule.get("meta", {}).get("family"),
            "template_id": outcome.capsule.get("meta", {}).get("template_id"),
            "gold_dag": outcome.capsule.get("gold", {}).get("dag"),
            "exec_dag": outcome.dag,
            "bindings": {
                k: v.extraction_id
                for k, v in outcome.capsule.get("bindings", {}).items()
            }
            if "bindings" in outcome.capsule
            else None,
        },
    )


def print_outcome(outcome: RunOutcome, *, verbose: bool, multi: bool) -> None:
    qid = outcome.capsule.get("qid", "<missing qid>")

    if multi:
        if outcome.ok:
            print(f"PASS {qid}  trace_nodes={outcome.trace_nodes}")
            return

        # fail
        if outcome.exec_error:
            print(f"FAIL {qid} (ExecError {outcome.exec_error['code']})")
            print(" ", outcome.exec_error.get("message"))
            if verbose and outcome.exec_error.get("data") is not None:
                print("  data:", outcome.exec_error.get("data"))
        else:
            print(f"FAIL {qid}")
            if verbose:
                print("  out :", outcome.output)
                print("  gold:", outcome.gold)
        return

    # single
    print("PASS" if outcome.ok else "FAIL")
    print("qid:", qid)
    if outcome.exec_error:
        print("ExecError:", outcome.exec_error["code"])
        print("message:", outcome.exec_error["message"])
        if outcome.exec_error.get("data") is not None:
            print("data:", outcome.exec_error.get("data"))
    else:
        print("out :", outcome.output)
        print("gold:", outcome.gold)
        print("trace_nodes:", outcome.trace_nodes)
        if verbose and outcome.trace:
            print("last_trace:", outcome.trace[-1])


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--benchmark", default="trace_ufr")
    ap.add_argument(
        "--capsules", required=True, help="Directory of .json capsules OR a .jsonl file"
    )
    ap.add_argument(
        "--extracts", default=None, help="Directory containing extract JSON files"
    )

    ap.add_argument("--qid", default=None, help="Run a single capsule by qid")
    ap.add_argument(
        "--all", action="store_true", help="Run all capsules found in --capsules"
    )

    # --- add CLI flag ---
    ap.add_argument(
        "--skip-existing",
        action="store_true",
        help="If --results-out exists, skip capsules whose qid is already present there.",
    )

    ap.add_argument(
        "--mode",
        choices=["oracle", "retrieval", "full"],
        default="oracle",
        help="oracle=gold+offline, retrieval=gold+openai_lookup, full=openai_plan+openai_lookup",
    )

    ap.add_argument(
        "--provider",
        choices=["openai", "anthropic", "gemini"],
        default=None,
        help="Which API provider to use for retrieval/full modes",
    )

    ap.add_argument(
        "--model", default=None, help="Model name (required for retrieval/full)"
    )
    ap.add_argument(
        "--schema",
        default=None,
        help="Path to ModelFact json_schema",
    )
    ap.add_argument(
        "--cache", default="cache/lookups.json", help="Path to lookup cache JSON file"
    )

    ap.add_argument(
        "--verbose",
        action="store_true",
        help="More detail on failures (and single runs)",
    )
    ap.add_argument(
        "--dump-trace", default=None, help="Directory to dump execution traces as JSON"
    )
    ap.add_argument(
        "--dump-trace-on-pass",
        action="store_true",
        help="Also dump traces for passing capsules (default dumps only failures)",
    )
    ap.add_argument(
        "--results-out", default=None, help="Append results as JSONL to this path"
    )

    args = ap.parse_args()
    benchmark_def = load_benchmark(args.benchmark)
    if args.extracts is None:
        args.extracts = str(benchmark_def.extracts_dir)
    if args.schema is None:
        args.schema = str(benchmark_def.schemas_dir / "model_fact.json")

    done_qids = set()
    if args.skip_existing and args.results_out:
        done_qids = _read_done_qids(args.results_out)
        if done_qids:
            print(
                f"[resume] skipping {len(done_qids)} already-completed qids from {args.results_out}"
            )

    cap_path = _clean_path(args.capsules)
    extracts_path = _clean_path(args.extracts)

    if (args.qid is None) == (not args.all):
        raise SystemExit("Provide either --qid <...> or --all")

    extracts_by_snippet = load_extract_store(extracts_path)

    plan_fn, lookup_fn = make_eval_fns(args, benchmark_def)

    dump_dir = Path(args.dump_trace) if args.dump_trace else None
    is_multi = bool(args.all)

    cfg = RunConfig(
        mode=args.mode,
        planner="gold" if args.mode in ("oracle", "retrieval") else args.provider,
        lookup="offline" if args.mode == "oracle" else args.provider,
        model=args.model if args.mode != "oracle" else None,
    )

    total = 0
    failures = 0

    for capsule in iter_selected_capsules(args, cap_path):
        total += 1

        qid = capsule.get("qid")
        if done_qids and qid in done_qids:
            # optionally print a tiny marker if you want, but silence is fine
            continue

        outcome = run_one(
            capsule,
            extracts_by_snippet,
            plan_fn=plan_fn,
            lookup_fn=lookup_fn,
            benchmark_def=benchmark_def,
        )

        maybe_dump_trace(
            outcome, dump_dir, include_trace_on_pass=bool(args.dump_trace_on_pass)
        )
        maybe_write_result(outcome, args, cfg)
        print_outcome(outcome, verbose=bool(args.verbose), multi=is_multi)

        if not outcome.ok:
            failures += 1

    if is_multi:
        passed = total - failures
        print(
            f"\nSummary: {passed}/{total} passed ({(passed / total * 100.0 if total else 0):.1f}%)"
        )
        if failures:
            raise SystemExit(0)
    else:
        if failures:
            raise SystemExit(0)


if __name__ == "__main__":
    main()
