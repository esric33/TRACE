# reason_bench/cli_run_sweep.py
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _iter_d_folders(corpus_dir: Path) -> List[Tuple[int, Path]]:
    out: List[Tuple[int, Path]] = []
    for p in sorted(corpus_dir.glob("d=*")):
        if not p.is_dir():
            continue
        try:
            d = int(p.name.split("=", 1)[1])
        except Exception:
            continue
        out.append((d, p))
    if not out:
        raise SystemExit(
            f"No distractor folders found under {corpus_dir} (expected d=<int>/)"
        )
    return out


def _append_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _iter_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


@dataclass(frozen=True)
class Job:
    d: int
    cap_dir: Path
    provider: str
    mode: str
    model_use: Optional[str]
    model_tag: str
    leaf_dir: Path
    traces_dir: Path
    results_path: Path
    cache_use: Optional[Path]


def _cache_for_job(cache_base: Path, *, provider: str, run_id: str, d: int) -> Path:
    """
    Zero contention: one cache per (provider, run_id, d).
    Example: cache/lookups.openai.run_openai_5d.d5.json
    """
    stem = cache_base.stem
    suffix = cache_base.suffix or ".json"
    name = f"{stem}.{provider}.{run_id}.d{d}{suffix}"
    return cache_base.with_name(name)


def _build_jobs(
    *,
    d_folders: List[Tuple[int, Path]],
    out_dir: Path,
    run_id: str,
    providers: List[str],
    modes: List[str],
    models: Optional[List[Optional[str]]],
    args,
) -> List[Job]:
    jobs: List[Job] = []
    cache_base = Path(args.cache)

    for d, cap_dir in d_folders:
        for provider in providers:
            for mode in modes:
                model_list = models if models else [None]
                for model in model_list:
                    if mode == "oracle":
                        model_use = None
                    else:
                        model_use = model
                        if not model_use:
                            raise SystemExit(f"Mode={mode} requires --model/--models")

                    # Prefer explicit --model-tag only for the "main" model (matches your old behavior)
                    model_tag = (
                        args.model_tag
                        if (
                            args.model_tag
                            and args.model
                            and args.model == model_use
                            and mode != "oracle"
                        )
                        else (model_use or "oracle")
                    )

                    leaf_dir = (
                        out_dir
                        / f"d={d}"
                        / f"provider={provider}"
                        / f"model={model_tag}"
                        / f"mode={mode}"
                    )
                    traces_dir = leaf_dir / "traces"
                    results_path = leaf_dir / "results.jsonl"
                    _ensure_dir(traces_dir)

                    cache_use = None
                    if mode in ("retrieval", "full"):
                        cache_use = _cache_for_job(
                            cache_base, provider=provider, run_id=run_id, d=d
                        )
                        _ensure_dir(cache_use.parent)

                    jobs.append(
                        Job(
                            d=d,
                            cap_dir=cap_dir,
                            provider=provider,
                            mode=mode,
                            model_use=model_use,
                            model_tag=model_tag,
                            leaf_dir=leaf_dir,
                            traces_dir=traces_dir,
                            results_path=results_path,
                            cache_use=cache_use,
                        )
                    )

    return jobs


def _run_job(job: Job, args) -> Path:
    cmd = [
        sys.executable,
        "-m",
        "TRACE.execute.cli_run",
        "--capsules",
        str(job.cap_dir),
        "--extracts",
        args.extracts,
        "--all",
        "--mode",
        job.mode,
        "--dump-trace",
        str(job.traces_dir),
        "--results-out",
        str(job.results_path),
        "--provider",
        job.provider,
    ]

    if args.resume:
        cmd.append("--skip-existing")

    if args.dump_trace_on_pass:
        cmd.append("--dump-trace-on-pass")
    if args.verbose:
        cmd.append("--verbose")

    if job.mode in ("retrieval", "full"):
        cmd += [
            "--model",
            job.model_use,  # type: ignore[arg-type]
            "--schema",
            args.schema,
            "--cache",
            str(job.cache_use),  # type: ignore[arg-type]
        ]

    print(
        f"[run_sweep] d={job.d} provider={job.provider} mode={job.mode} model={job.model_tag}"
    )
    subprocess.check_call(cmd)
    return job.results_path


def main() -> None:
    ap = argparse.ArgumentParser()

    ap.add_argument("--corpus-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--extracts", required=True)

    ap.add_argument(
        "--resume",
        action="store_true",
        help="Resume leaf jobs by skipping qids already present in results.jsonl",
    )

    # single vs multi
    ap.add_argument("--mode", choices=["oracle", "retrieval", "full"], default=None)
    ap.add_argument("--modes", nargs="+", default=None)

    ap.add_argument("--model", default=None)
    ap.add_argument("--models", nargs="+", default=None)
    ap.add_argument("--model-tag", default=None)

    ap.add_argument(
        "--provider",
        choices=["openai", "anthropic", "gemini"],
        default=None,
        help="Single provider (default: openai if omitted)",
    )
    ap.add_argument(
        "--providers",
        nargs="+",
        default=None,
        help="Multiple providers (e.g., openai anthropic gemini)",
    )

    ap.add_argument("--schema", default="schemas/model_fact.json")
    ap.add_argument("--cache", default="cache/lookups.json")

    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--dump-trace-on-pass", action="store_true")

    # NEW: parallelism
    ap.add_argument(
        "--max-jobs",
        type=int,
        default=min(8, (os.cpu_count() or 8)),
        help="Max concurrent leaf jobs (subprocesses).",
    )

    args, unknown = ap.parse_known_args()

    corpus_dir = Path(args.corpus_dir)
    out_dir = Path(args.out_dir)
    _ensure_dir(out_dir)

    corpus_id = corpus_dir.name
    run_id = out_dir.name

    # Resolve modes/models/providers
    modes = args.modes if args.modes else ([args.mode] if args.mode else None)
    if not modes:
        raise SystemExit("Provide --mode or --modes")

    models = args.models if args.models else ([args.model] if args.model else None)
    if any(m in ("retrieval", "full") for m in modes) and (
        not models or models == [None]
    ):
        raise SystemExit("Provide --model/--models for retrieval/full")

    providers = (
        args.providers
        if args.providers
        else ([args.provider] if args.provider else ["openai"])
    )

    d_folders = _iter_d_folders(corpus_dir)

    meta = {
        "run_id": run_id,
        "corpus_id": corpus_id,
        "corpus_dir": str(corpus_dir.resolve()),
        "out_dir": str(out_dir.resolve()),
        "extracts_dir": str(Path(args.extracts).resolve()),
        "modes": modes,
        "models": [m for m in (models or []) if m],
        "providers": providers,
        "schema": str(Path(args.schema).resolve()),
        "cache_base": str(Path(args.cache).resolve()),
        "cache_policy": "per (provider, run_id, d): lookups.{provider}.{run_id}.d{d}.json",
        "distractors": [d for d, _ in d_folders],
        "max_jobs": int(args.max_jobs),
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    results_all = out_dir / "results_all.jsonl"
    # leave existing results_all in place; if you want overwrite, uncomment:
    # if results_all.exists(): results_all.unlink()

    jobs = _build_jobs(
        d_folders=d_folders,
        out_dir=out_dir,
        run_id=run_id,
        providers=providers,
        modes=modes,
        models=models,
        args=args,
    )

    # Run leaf jobs in parallel (subprocesses)
    # Note: ThreadPool is fine because we're just waiting on subprocess I/O.
    results_by_job: Dict[Path, Job] = {}
    with ThreadPoolExecutor(max_workers=int(args.max_jobs)) as ex:
        fut_map = {ex.submit(_run_job, j, args): j for j in jobs}
        for fut in as_completed(fut_map):
            j = fut_map[fut]
            try:
                rp = fut.result()
            except subprocess.CalledProcessError as e:
                raise SystemExit(
                    f"Job failed (exit={e.returncode}): d={j.d} provider={j.provider} mode={j.mode} model={j.model_tag}"
                )
            results_by_job[rp] = j
            print(
                f"[done] d={j.d} provider={j.provider} mode={j.mode} model={j.model_tag}"
            )

    # Aggregate sequentially to avoid jsonl corruption
    # Deterministic order: sort by leaf dir path
    for rp in sorted(results_by_job.keys(), key=lambda p: str(p.parent)):
        j = results_by_job[rp]
        rows = _iter_jsonl(rp)
        for r in rows:
            r["run_id"] = run_id
            r["corpus_id"] = corpus_id
            r["distractor"] = j.d
            r["provider"] = j.provider
            r["mode"] = j.mode
            r["model"] = j.model_use
            r["model_tag"] = j.model_tag
            r["cache_path"] = str(j.cache_use) if j.cache_use else None
        _append_jsonl(results_all, rows)

    print(f"Wrote {results_all}")


if __name__ == "__main__":
    main()
