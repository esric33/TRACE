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
    ExecError,
    exec_error_to_dict,
)
from TRACE.reporting.evaluation import compare_outputs
from TRACE.reporting.results import RunConfig, build_result_row, write_result_row
from TRACE.reporting.summary import write_run_summary_artifacts
from TRACE.shared.io import _clean_path, read_json, read_jsonl
from TRACE.providers.shared.prompt import build_planner_prompt

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
    planner_prompt: Optional[str] = None
    trace_nodes: int = 0
    exec_error: Optional[Dict[str, Any]] = None


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
# Eval fn selection
# -----------------------------------------------------------------------------


def make_eval_fns(args, benchmark_def):
    if args.mode == "oracle":
        return None  # plan_fn=None => use gold dag
    if args.mode == "full":
        if not args.model:
            raise SystemExit("--model is required for mode=full")

        # ----------------------------
        # OPENAI
        # ----------------------------
        if args.provider == "openai":
            from openai import OpenAI

            client = OpenAI()
            from TRACE.providers.openai.planner_openai import openai_plan_fn

            def plan(capsule):
                return openai_plan_fn(
                    capsule,
                    client=client,
                    model=args.model,
                    benchmark_def=benchmark_def,
                )

            return plan

        # ----------------------------
        # ANTHROPIC
        # ----------------------------
        if args.provider == "anthropic":
            from TRACE.providers.anthropic._client import make_client

            client = make_client()
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

            return plan

        # ----------------------------
        # GEMINI
        # ----------------------------
        if args.provider == "gemini":
            from TRACE.providers.gemini._client import make_client

            client = make_client()
            from TRACE.providers.gemini.planner_gemini import gemini_plan_fn

            def plan(capsule):
                return gemini_plan_fn(
                    capsule,
                    client=client,
                    model=args.model,
                    benchmark_def=benchmark_def,
                )

            return plan

        raise SystemExit(f"Unknown provider: {args.provider}")
    raise SystemExit(f"Unknown mode: {args.mode}")


# -----------------------------------------------------------------------------
# Run logic
# -----------------------------------------------------------------------------


def run_one(capsule, *, plan_fn, benchmark_def) -> RunOutcome:
    dag = None
    gold = capsule.get("gold", {}).get("answer")
    trace = []
    planner_prompt = None

    try:
        if plan_fn is None:
            dag = capsule["gold"]["dag"]
        else:
            planner_prompt = build_planner_prompt(capsule, benchmark_def=benchmark_def)
            dag = plan_fn(capsule)

        res = execute_dag(
            dag=dag,
            benchmark_def=benchmark_def,
            capsule=capsule,
            cache={},
        )

        out = res["output"]
        trace = res.get("trace", [])
        ok = compare_outputs(out, gold).correct

        return RunOutcome(
            capsule=capsule,
            ok=ok,
            output=out,
            gold=gold,
            dag=dag,
            trace=trace,
            planner_prompt=planner_prompt,
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
            planner_prompt=planner_prompt,
            trace_nodes=len(trace) if trace else 0,
            exec_error=exec_error_to_dict(e),
        )


def _result_extra(outcome: RunOutcome) -> dict[str, Any]:
    return {
        "qid": outcome.capsule.get("qid"),
        "benchmark_id": outcome.capsule.get("meta", {}).get("benchmark_id"),
        "family": outcome.capsule.get("meta", {}).get("family"),
        "template_id": outcome.capsule.get("meta", {}).get("template_id"),
        "gold_dag": outcome.capsule.get("gold", {}).get("dag"),
        "exec_dag": outcome.dag,
        "planner_prompt": outcome.planner_prompt,
        "bindings": {
            k: v.extraction_id
            for k, v in outcome.capsule.get("bindings", {}).items()
        }
        if "bindings" in outcome.capsule
        else None,
    }


def _executed_dag(dag: Optional[Dict[str, Any]], trace: list[dict], exec_error: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(dag, dict):
        return None
    by_node = {
        step.get("node"): step
        for step in trace
        if isinstance(step, dict) and isinstance(step.get("node"), str)
    }
    nodes = []
    for node in dag.get("nodes", []) or []:
        if not isinstance(node, dict):
            continue
        node_id = node.get("id")
        step = by_node.get(node_id)
        executed = dict(node)
        if isinstance(step, dict):
            executed["result"] = step.get("result")
        nodes.append(executed)
    return {
        "nodes": nodes,
        "output": dag.get("output"),
        "execution_status": "failed" if exec_error else "ok",
        "exec_error": exec_error,
    }


def _evaluation_artifact(row: Dict[str, Any]) -> Dict[str, Any]:
    executor_error = None
    if row.get("exec_error") is not None:
        executor_error = {
            "code": row.get("exec_error_code"),
            "phase": row.get("exec_error_phase"),
            "op": row.get("exec_error_op"),
            "node_id": row.get("exec_error_node_id"),
            "arg": row.get("exec_error_arg"),
            "message": (row.get("exec_error") or {}).get("message")
            if isinstance(row.get("exec_error"), dict)
            else None,
            "details": (row.get("exec_error") or {}).get("data")
            if isinstance(row.get("exec_error"), dict)
            else None,
        }

    metric_prefixes = (
        "dag_",
        "fact_",
        "anchored_",
        "intermediate_",
    )
    metrics = {
        key: value
        for key, value in row.items()
        if key.startswith(metric_prefixes)
    }
    return {
        "qid": row.get("qid"),
        "benchmark_id": row.get("benchmark_id"),
        "template_id": row.get("template_id"),
        "mode": row.get("mode"),
        "planner": row.get("planner"),
        "model": row.get("model"),
        "correct": row.get("correct"),
        "failure_stage": row.get("failure_stage"),
        "executor_error": executor_error,
        "mismatch_kind": row.get("mismatch_kind"),
        "comparison": row.get("comparison"),
        "output": row.get("output"),
        "gold": row.get("gold"),
        "ops": row.get("ops"),
        "metrics": metrics,
    }


def maybe_dump_trace(
    outcome: RunOutcome,
    dump_trace_dir: Optional[Path],
    *,
    include_trace_on_pass: bool,
    cfg: RunConfig,
) -> None:
    if dump_trace_dir is None:
        return

    if (not include_trace_on_pass) and outcome.ok:
        return

    dump_trace_dir.mkdir(parents=True, exist_ok=True)

    qid = outcome.capsule.get("qid", "unknown_qid")
    trace_path = dump_trace_dir / f"{qid}.trace.json"
    prompt_path = dump_trace_dir / f"{qid}.prompt.txt"
    plan_path = dump_trace_dir / f"{qid}.plan.json"
    executed_dag_path = dump_trace_dir / f"{qid}.executed_dag.json"
    evaluation_path = dump_trace_dir / f"{qid}.evaluation.json"

    trace_blob = {
        "qid": qid,
        "question": outcome.capsule.get("question"),
        "planner_prompt": outcome.planner_prompt,
        "dag": outcome.dag,
        "trace": outcome.trace or [],
        "output": outcome.output,
        "gold": outcome.capsule.get("gold", {}).get("answer"),
        "exec_error": outcome.exec_error,
    }
    trace_path.write_text(
        json.dumps(trace_blob, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    if outcome.planner_prompt is not None:
        prompt_path.write_text(outcome.planner_prompt, encoding="utf-8")

    plan_blob = {
        "qid": qid,
        "question": outcome.capsule.get("question"),
        "planner": cfg.planner,
        "model": cfg.model,
        "prompt_path": str(prompt_path) if outcome.planner_prompt is not None else None,
        "dag": outcome.dag,
    }
    plan_path.write_text(
        json.dumps(plan_blob, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    executed = _executed_dag(outcome.dag, outcome.trace or [], outcome.exec_error)
    executed_dag_path.write_text(
        json.dumps(
            {
                "qid": qid,
                "dag": executed,
                "output": outcome.output,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    row = build_result_row(
        capsule=outcome.capsule,
        cfg=cfg,
        ok=outcome.ok,
        output=outcome.output,
        gold=outcome.gold,
        trace=outcome.trace or [],
        exec_error=outcome.exec_error,
        extra=_result_extra(outcome),
    )
    evaluation_path.write_text(
        json.dumps(_evaluation_artifact(row), indent=2, ensure_ascii=False),
        encoding="utf-8",
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
        gold=outcome.gold,
        trace=outcome.trace or [],
        exec_error=outcome.exec_error,
        extra=_result_extra(outcome),
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
        choices=["oracle", "full"],
        default="oracle",
        help="oracle=gold dag, full=model planned dag",
    )

    ap.add_argument(
        "--provider",
        choices=["openai", "anthropic", "gemini"],
        default=None,
        help="Which API provider to use for full mode",
    )

    ap.add_argument(
        "--model", default=None, help="Model name (required for full)"
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

    done_qids = set()
    if args.skip_existing and args.results_out:
        done_qids = _read_done_qids(args.results_out)
        if done_qids:
            print(
                f"[resume] skipping {len(done_qids)} already-completed qids from {args.results_out}"
            )

    cap_path = _clean_path(args.capsules)
    if (args.qid is None) == (not args.all):
        raise SystemExit("Provide either --qid <...> or --all")

    plan_fn = make_eval_fns(args, benchmark_def)

    dump_dir = Path(args.dump_trace) if args.dump_trace else None
    is_multi = bool(args.all)

    cfg = RunConfig(
        mode=args.mode,
        planner="gold" if args.mode == "oracle" else args.provider,
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
            plan_fn=plan_fn,
            benchmark_def=benchmark_def,
        )

        maybe_dump_trace(
            outcome,
            dump_dir,
            include_trace_on_pass=bool(args.dump_trace_on_pass),
            cfg=cfg,
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
        if args.results_out:
            write_run_summary_artifacts(args.results_out)
        if failures:
            raise SystemExit(0)
    else:
        if args.results_out:
            write_run_summary_artifacts(args.results_out)
        if failures:
            raise SystemExit(0)


if __name__ == "__main__":
    main()
