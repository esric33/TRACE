from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parents[1]


DEFAULT_MODELS = {
    "openai": ["gpt-5.2", "gpt-5-mini", "gpt-5-nano"],
    "anthropic": ["claude-opus-4-6", "claude-sonnet-4-5", "claude-haiku-4-5"],
    "gemini": ["gemini-2.5-pro", "gemini-3-flash-preview", "gemini-2.5-flash"],
}


@dataclass(frozen=True)
class SweepJob:
    benchmark: str
    corpus_dir: Path
    run_dir: Path
    provider: str
    models: Sequence[str]


def _run(cmd: Sequence[str], *, dry_run: bool) -> None:
    printable = " ".join(str(part) for part in cmd)
    print(f"[cmd] {printable}")
    if dry_run:
        return
    env = os.environ.copy()
    src_path = str(ROOT / "src")
    env["PYTHONPATH"] = (
        f"{src_path}{os.pathsep}{env['PYTHONPATH']}"
        if env.get("PYTHONPATH")
        else src_path
    )
    subprocess.check_call(list(cmd), cwd=ROOT, env=env)


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _jsonl_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def _corpus_complete(
    corpus_dir: Path,
    *,
    benchmark: str,
    n_total: int,
    distractors: Sequence[int],
) -> bool:
    meta = _read_json(corpus_dir / "meta.json")
    profile = _read_json(corpus_dir / "benchmark_profile.json")
    expected = int(n_total) * len(distractors)
    return bool(
        meta.get("n_total_per_distractor") == int(n_total)
        and meta.get("distractors") == list(distractors)
        and bool(meta.get("template_balanced")) is True
        and profile.get("benchmark_id") == benchmark
        and profile.get("total_queries") == expected
        and _jsonl_count(corpus_dir / "capsules.jsonl") == expected
        and all((corpus_dir / f"d={d}").is_dir() for d in distractors)
    )


def _generate_corpus(
    *,
    benchmark: str,
    corpus_dir: Path,
    n_total: int,
    seed: int,
    distractors: Sequence[int],
    max_compile_attempts: int,
    resume: bool,
    dry_run: bool,
) -> None:
    if resume and _corpus_complete(
        corpus_dir,
        benchmark=benchmark,
        n_total=n_total,
        distractors=distractors,
    ):
        print(f"[resume] corpus complete: {corpus_dir}")
        return

    cmd = [
        sys.executable,
        "-m",
        "TRACE.cli.generate",
        "--benchmark",
        benchmark,
        "--out",
        str(corpus_dir),
        "--distractors",
        *[str(d) for d in distractors],
        "--n-total",
        str(n_total),
        "--seed",
        str(seed),
        "--balance-templates",
        "--max-compile-attempts",
        str(max_compile_attempts),
        "--force",
    ]
    _run(cmd, dry_run=dry_run)


def _run_provider(
    *,
    benchmark: str,
    corpus_dir: Path,
    run_dir: Path,
    provider: str,
    models: Sequence[str],
    max_jobs: int,
    resume: bool,
    dry_run: bool,
) -> None:
    cmd = [
        sys.executable,
        "-m",
        "TRACE.cli.run_sweep",
        "--benchmark",
        benchmark,
        "--corpus-dir",
        str(corpus_dir),
        "--out-dir",
        str(run_dir),
        "--modes",
        "full",
        "--provider",
        provider,
        "--models",
        *models,
        "--max-jobs",
        str(max_jobs),
        "--dump-trace-on-pass",
    ]
    if resume:
        cmd.append("--resume")
    _run(cmd, dry_run=dry_run)


def _append_jsonl(src: Path, dst: Path, *, experiment_id: str) -> int:
    if not src.exists():
        return 0
    count = 0
    dst.parent.mkdir(parents=True, exist_ok=True)
    with src.open("r", encoding="utf-8") as in_f, dst.open("a", encoding="utf-8") as out_f:
        for line in in_f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            row["experiment_id"] = experiment_id
            out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    return count


def _combine_results(
    *,
    experiment_dir: Path,
    experiment_id: str,
    benchmarks: Sequence[str],
    providers: Sequence[str],
    dry_run: bool,
) -> None:
    combined_path = experiment_dir / "results_all.jsonl"
    print(f"[combine] {combined_path}")
    if dry_run:
        return
    if combined_path.exists():
        combined_path.unlink()
    counts: dict[str, int] = {}
    for benchmark in benchmarks:
        for provider in providers:
            src = experiment_dir / "runs" / benchmark / provider / "results_all.jsonl"
            count = _append_jsonl(src, combined_path, experiment_id=experiment_id)
            counts[f"{benchmark}/{provider}"] = count
    (experiment_dir / "result_counts.json").write_text(
        json.dumps(counts, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _run_sweeps(
    jobs: Sequence[SweepJob],
    *,
    max_sweeps: int,
    max_jobs: int,
    resume: bool,
    dry_run: bool,
) -> None:
    if dry_run:
        for job in jobs:
            _run_provider(
                benchmark=job.benchmark,
                corpus_dir=job.corpus_dir,
                run_dir=job.run_dir,
                provider=job.provider,
                models=job.models,
                max_jobs=max_jobs,
                resume=resume,
                dry_run=True,
            )
        return

    with ThreadPoolExecutor(max_workers=max(1, int(max_sweeps))) as executor:
        future_map = {
            executor.submit(
                _run_provider,
                benchmark=job.benchmark,
                corpus_dir=job.corpus_dir,
                run_dir=job.run_dir,
                provider=job.provider,
                models=job.models,
                max_jobs=max_jobs,
                resume=resume,
                dry_run=False,
            ): job
            for job in jobs
        }
        for future in as_completed(future_map):
            job = future_map[future]
            try:
                future.result()
            except subprocess.CalledProcessError as exc:
                raise SystemExit(
                    "Sweep failed "
                    f"benchmark={job.benchmark} provider={job.provider} "
                    f"exit={exc.returncode}"
                )
            print(f"[done] sweep benchmark={job.benchmark} provider={job.provider}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Run one TRACE experiment across datasets/providers.")
    ap.add_argument("--experiment-id", default="trace-both-n600-d3-seed0-nine-models")
    ap.add_argument("--root", default=str(ROOT / "outputs" / "experiments"))
    ap.add_argument("--benchmarks", nargs="+", default=["trace_ufr", "trace_dir"])
    ap.add_argument("--n-total", type=int, default=600)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--distractors", nargs="+", type=int, default=[3])
    ap.add_argument("--max-compile-attempts", type=int, default=100)
    ap.add_argument("--max-jobs", type=int, default=1)
    ap.add_argument(
        "--max-sweeps",
        type=int,
        default=1,
        help="Max concurrent benchmark/provider sweeps.",
    )
    ap.add_argument("--openai-models", nargs="+", default=DEFAULT_MODELS["openai"])
    ap.add_argument("--anthropic-models", nargs="+", default=DEFAULT_MODELS["anthropic"])
    ap.add_argument("--gemini-models", nargs="+", default=DEFAULT_MODELS["gemini"])
    ap.add_argument("--providers", nargs="+", default=["openai", "anthropic", "gemini"])
    ap.add_argument("--no-resume", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    provider_models = {
        "openai": args.openai_models,
        "anthropic": args.anthropic_models,
        "gemini": args.gemini_models,
    }
    providers = list(args.providers)
    for provider in providers:
        if provider not in provider_models:
            raise SystemExit(f"Unknown provider: {provider}")

    experiment_dir = Path(args.root) / args.experiment_id
    corpus_root = experiment_dir / "corpora"
    runs_root = experiment_dir / "runs"
    resume = not bool(args.no_resume)

    manifest = {
        "experiment_id": args.experiment_id,
        "experiment_dir": str(experiment_dir.resolve()),
        "benchmarks": args.benchmarks,
        "n_total_per_distractor": int(args.n_total),
        "seed": int(args.seed),
        "distractors": [int(d) for d in args.distractors],
        "providers": providers,
        "models": {provider: provider_models[provider] for provider in providers},
        "modes": ["full"],
        "template_balanced": True,
        "max_sweeps": int(args.max_sweeps),
        "max_jobs_per_sweep": int(args.max_jobs),
    }
    print(f"[experiment] {experiment_dir}")
    if not args.dry_run:
        experiment_dir.mkdir(parents=True, exist_ok=True)
        (experiment_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    sweep_jobs: list[SweepJob] = []
    for benchmark in args.benchmarks:
        corpus_dir = corpus_root / benchmark
        _generate_corpus(
            benchmark=benchmark,
            corpus_dir=corpus_dir,
            n_total=int(args.n_total),
            seed=int(args.seed),
            distractors=[int(d) for d in args.distractors],
            max_compile_attempts=int(args.max_compile_attempts),
            resume=resume,
            dry_run=bool(args.dry_run),
        )
        for provider in providers:
            sweep_jobs.append(
                SweepJob(
                    benchmark=benchmark,
                    corpus_dir=corpus_dir,
                    run_dir=runs_root / benchmark / provider,
                    provider=provider,
                    models=provider_models[provider],
                )
            )

    _run_sweeps(
        sweep_jobs,
        max_sweeps=int(args.max_sweeps),
        max_jobs=int(args.max_jobs),
        resume=resume,
        dry_run=bool(args.dry_run),
    )

    _combine_results(
        experiment_dir=experiment_dir,
        experiment_id=args.experiment_id,
        benchmarks=args.benchmarks,
        providers=providers,
        dry_run=bool(args.dry_run),
    )
    print(f"[done] experiment: {experiment_dir}")


if __name__ == "__main__":
    main()
