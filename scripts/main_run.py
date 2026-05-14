from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parents[1]


def _run(cmd: Sequence[str], *, dry_run: bool) -> None:
    printable = " ".join(str(part) for part in cmd)
    print(f"[cmd] {printable}")
    if not dry_run:
        env = os.environ.copy()
        src_path = str(ROOT / "src")
        env["PYTHONPATH"] = (
            f"{src_path}{os.pathsep}{env['PYTHONPATH']}"
            if env.get("PYTHONPATH")
            else src_path
        )
        subprocess.check_call(list(cmd), cwd=ROOT, env=env)


def _jsonl_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _corpus_complete(
    corpus_dir: Path,
    *,
    benchmark: str,
    n_total: int,
    distractors: Sequence[int],
    template_balanced: bool,
) -> bool:
    meta = _read_json(corpus_dir / "meta.json")
    if not meta:
        return False
    if meta.get("n_total_per_distractor") != n_total:
        return False
    if meta.get("distractors") != list(distractors):
        return False
    if bool(meta.get("template_balanced")) != bool(template_balanced):
        return False
    profile = _read_json(corpus_dir / "benchmark_profile.json")
    if profile.get("benchmark_id") != benchmark:
        return False
    expected = n_total * len(distractors)
    if profile.get("total_queries") != expected:
        return False
    if _jsonl_count(corpus_dir / "capsules.jsonl") != expected:
        return False
    return all((corpus_dir / f"d={d}").is_dir() for d in distractors)


def _run_complete_for_config(
    run_dir: Path,
    *,
    expected_total: int,
    corpus_dir: Path | None = None,
    provider: str | None = None,
    models: Sequence[str] | None = None,
    modes: Sequence[str] | None = None,
) -> bool:
    meta = _read_json(run_dir / "meta.json")
    if not meta:
        return False
    if corpus_dir is not None and meta.get("corpus_dir") != str(corpus_dir.resolve()):
        return False
    if provider is not None and meta.get("providers") != [provider]:
        return False
    if models is not None and meta.get("models") != list(models):
        return False
    if modes is not None and meta.get("modes") != list(modes):
        return False
    results_all = run_dir / "results_all.jsonl"
    if _jsonl_count(results_all) != expected_total:
        return False
    return (run_dir / "summary.json").exists() and (run_dir / "summary.md").exists()


def _copy_if_exists(src: Path, dst: Path) -> None:
    if src.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)


def _write_final_results(
    *,
    final_dir: Path,
    corpus_dir: Path,
    run_dir: Path,
    manifest: dict,
    dry_run: bool,
) -> None:
    print(f"[finalize] {final_dir}")
    if dry_run:
        return
    final_dir.mkdir(parents=True, exist_ok=True)
    _copy_if_exists(run_dir / "results_all.jsonl", final_dir / "results_all.jsonl")
    _copy_if_exists(run_dir / "summary.json", final_dir / "summary.json")
    _copy_if_exists(run_dir / "summary.md", final_dir / "summary.md")
    _copy_if_exists(run_dir / "meta.json", final_dir / "run_meta.json")
    _copy_if_exists(corpus_dir / "meta.json", final_dir / "corpus_meta.json")
    _copy_if_exists(corpus_dir / "benchmark_profile.json", final_dir / "benchmark_profile.json")
    _copy_if_exists(corpus_dir / "benchmark_profile.md", final_dir / "benchmark_profile.md")
    _copy_if_exists(corpus_dir / "capsules.jsonl", final_dir / "capsules.jsonl")
    (final_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the main TRACE benchmark pipeline.")
    ap.add_argument("--benchmark", default="trace_ufr")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--n-total", type=int, default=600)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--distractors", nargs="+", type=int, default=[3])
    ap.add_argument("--provider", default="openai")
    ap.add_argument("--models", nargs="+", default=["gpt-5.2"])
    ap.add_argument("--modes", nargs="+", default=["full"])
    ap.add_argument("--max-jobs", type=int, default=1)
    ap.add_argument("--max-compile-attempts", type=int, default=100)
    ap.add_argument("--intermediate-root", default=str(ROOT / "outputs" / "intermediate"))
    ap.add_argument("--final-root", default=str(ROOT / "outputs" / "final-results"))
    ap.add_argument("--no-resume", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    distractors = [int(d) for d in args.distractors]
    d_slug = "d" + "-".join(str(d) for d in distractors)
    model_slug = "-".join(str(model).replace("/", "_") for model in args.models)
    run_id = (
        args.run_id
        or f"{args.benchmark}-n{args.n_total}-{d_slug}-seed{args.seed}-{args.provider}-{model_slug}"
    )
    intermediate_dir = Path(args.intermediate_root) / run_id
    corpus_dir = intermediate_dir / "corpus"
    run_dir = intermediate_dir / "runs" / args.provider
    final_dir = Path(args.final_root) / run_id

    expected_total = int(args.n_total) * len(distractors) * len(args.models) * len(args.modes)
    corpus_expected_total = int(args.n_total) * len(distractors)
    resume = not bool(args.no_resume)

    if resume and _corpus_complete(
        corpus_dir,
        benchmark=args.benchmark,
        n_total=int(args.n_total),
        distractors=distractors,
        template_balanced=True,
    ):
        print(f"[resume] corpus complete: {corpus_dir}")
    else:
        cmd = [
            sys.executable,
            "-m",
            "TRACE.cli.generate",
            "--benchmark",
            args.benchmark,
            "--out",
            str(corpus_dir),
            "--distractors",
            *[str(d) for d in distractors],
            "--n-total",
            str(args.n_total),
            "--seed",
            str(args.seed),
            "--balance-templates",
            "--max-compile-attempts",
            str(args.max_compile_attempts),
            "--force",
        ]
        _run(cmd, dry_run=bool(args.dry_run))

    run_is_complete = resume and _run_complete_for_config(
        run_dir,
        expected_total=expected_total,
        corpus_dir=corpus_dir,
        provider=args.provider,
        models=args.models,
        modes=args.modes,
    )
    if run_is_complete:
        print(f"[resume] run complete: {run_dir}")
    else:
        cmd = [
            sys.executable,
            "-m",
            "TRACE.cli.run_sweep",
            "--benchmark",
            args.benchmark,
            "--corpus-dir",
            str(corpus_dir),
            "--out-dir",
            str(run_dir),
            "--modes",
            *args.modes,
            "--provider",
            args.provider,
            "--models",
            *args.models,
            "--max-jobs",
            str(args.max_jobs),
            "--dump-trace-on-pass",
        ]
        if resume:
            cmd.append("--resume")
        _run(cmd, dry_run=bool(args.dry_run))

    manifest = {
        "run_id": run_id,
        "benchmark": args.benchmark,
        "n_total_per_distractor": int(args.n_total),
        "corpus_expected_rows": corpus_expected_total,
        "result_expected_rows": expected_total,
        "seed": int(args.seed),
        "distractors": distractors,
        "provider": args.provider,
        "models": args.models,
        "modes": args.modes,
        "template_balanced": True,
        "corpus_dir": str(corpus_dir.resolve()),
        "run_dir": str(run_dir.resolve()),
        "final_dir": str(final_dir.resolve()),
    }
    _write_final_results(
        final_dir=final_dir,
        corpus_dir=corpus_dir,
        run_dir=run_dir,
        manifest=manifest,
        dry_run=bool(args.dry_run),
    )
    print(f"[done] final results: {final_dir}")


if __name__ == "__main__":
    main()
