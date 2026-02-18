# reason_bench/generation/cli_generate_corpus.py
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from TRACE.shared.io import read_json  # if you have it; optional

from TRACE.generation.cli_generate import (
    main as generate_main,
)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--extracts", required=True)
    ap.add_argument("--snippets", required=True)
    ap.add_argument("--out", required=True)

    ap.add_argument("--distractors", nargs="+", type=int, default=[0, 1, 3, 5, 10])
    ap.add_argument("--n-total", type=int, default=100)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--max-compile-attempts", type=int, default=50)

    ap.add_argument("--p-family", default="")
    ap.add_argument("--w", default="")
    # future: qtype mix
    ap.add_argument("--p-qtype", default="")

    ap.add_argument("--force", action="store_true")

    args = ap.parse_args()

    out_dir = Path(args.out)
    if out_dir.exists() and any(out_dir.iterdir()) and not args.force:
        raise SystemExit(f"Out dir not empty: {out_dir} (use --force to overwrite)")

    _ensure_dir(out_dir)

    corpus_id = out_dir.name

    meta: Dict[str, Any] = {
        "corpus_id": corpus_id,
        "extracts_dir": str(Path(args.extracts).resolve()),
        "snippets_dir": str(Path(args.snippets).resolve()),
        "distractors": list(args.distractors),
        "n_total_per_distractor": int(args.n_total),
        "seed_base": int(args.seed),
        "max_compile_attempts": int(args.max_compile_attempts),
        "p_family_raw": args.p_family,
        "p_qtype_raw": args.p_qtype,
        "w_raw": args.w,
        "per_distractor": {},
    }

    capsules_index_path = out_dir / "capsules.jsonl"
    if capsules_index_path.exists():
        capsules_index_path.unlink()

    for d in args.distractors:
        d = int(d)
        sub = out_dir / f"d={d}"
        _ensure_dir(sub)

        # deterministic per-d seed
        seed_d = int(args.seed) + d * 1000
        meta["per_distractor"][str(d)] = {"seed": seed_d}

        # Call cli_generate as a module (simple & safe for now).
        # Later we replace this with a direct function call.
        import subprocess, sys

        cmd = [
            sys.executable,
            "-m",
            "TRACE.generation.cli_generate",
            "--extracts",
            args.extracts,
            "--snippets",
            args.snippets,
            "--out",
            str(sub),
            "--n",
            str(args.n_total),
            "--seed",
            str(seed_d),
            "--max-compile-attempts",
            str(args.max_compile_attempts),
            "--distractor-count",
            str(d),
        ]
        if args.p_family:
            cmd += ["--p", args.p_family]
        if args.w:
            cmd += ["--w", args.w]
        # p-qtype ignored for now (you’ll add it later)

        subprocess.check_call(cmd)

        # Build capsules.jsonl index (non-recursive join source for reporting)
        for p in sorted(sub.glob("*.json")):
            cap = json.loads(p.read_text(encoding="utf-8"))
            rec = {
                "qid": cap.get("qid"),
                "distractor": d,
                "capsule_path": str(p.relative_to(out_dir)),
                "family": (cap.get("meta") or {}).get("family"),
                "qkey": cap.get("qkey") or (cap.get("meta") or {}).get("qkey"),
                "template_id": (cap.get("meta") or {}).get("template_id"),
                "seed": (cap.get("meta") or {}).get("seed"),
                "question": cap.get("question"),
            }
            capsules_index_path.write_text(
                (
                    capsules_index_path.read_text(encoding="utf-8")
                    if capsules_index_path.exists()
                    else ""
                )
                + json.dumps(rec, ensure_ascii=False)
                + "\n",
                encoding="utf-8",
            )

    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"Wrote corpus to {out_dir}")


if __name__ == "__main__":
    main()
