from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

from TRACE.shared.io import read_json, read_jsonl


def _load_jsonl_by_qid(path: Path) -> Dict[str, Dict[str, Any]]:
    return {row["qid"]: row for row in read_jsonl(path)}


def _normalize(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _normalize(v) for k, v in sorted(obj.items())}
    if isinstance(obj, list):
        return [_normalize(v) for v in obj]
    if isinstance(obj, float):
        return round(obj, 12)
    return obj


def _compare_jsonl(legacy: Path, refactor: Path) -> tuple[bool, str]:
    if not legacy.exists() or not refactor.exists():
        return False, "one or both jsonl files are missing"
    legacy_all = [row for row in read_jsonl(legacy)]
    refactor_all = [row for row in read_jsonl(refactor)]
    if len(legacy_all) != len(refactor_all):
        return False, "jsonl row counts differ"
    legacy_rows = {row["qid"]: row for row in legacy_all}
    refactor_rows = {row["qid"]: row for row in refactor_all}
    if set(legacy_rows) != set(refactor_rows):
        return False, "qid sets differ"
    for qid in sorted(legacy_rows):
        legacy_rows[qid].pop("ts_utc", None)
        refactor_rows[qid].pop("ts_utc", None)
        if _normalize(legacy_rows[qid]) != _normalize(refactor_rows[qid]):
            return False, f"row mismatch for qid={qid}"
    return True, "jsonl parity ok"


def _compare_corpus_dirs(legacy: Path, refactor: Path) -> tuple[bool, str]:
    legacy_meta = read_json(legacy / "meta.json")
    refactor_meta = read_json(refactor / "meta.json")
    for key in ("extracts_dir", "snippets_dir"):
        legacy_meta.pop(key, None)
        refactor_meta.pop(key, None)
    if _normalize(legacy_meta) != _normalize(refactor_meta):
        return False, "meta.json mismatch"

    legacy_capsules_index = legacy / "capsules.jsonl"
    refactor_capsules_index = refactor / "capsules.jsonl"
    if legacy_capsules_index.exists() or refactor_capsules_index.exists():
        ok, message = _compare_jsonl(legacy_capsules_index, refactor_capsules_index)
        if not ok:
            return False, f"capsules.jsonl mismatch: {message}"

    legacy_capsules = {
        p.name: read_json(p)
        for p in sorted(legacy.rglob("*.json"))
        if p.name != "meta.json"
    }
    refactor_capsules = {
        p.name: read_json(p)
        for p in sorted(refactor.rglob("*.json"))
        if p.name != "meta.json"
    }
    if set(legacy_capsules) != set(refactor_capsules):
        return False, "capsule file sets differ"
    for name in sorted(legacy_capsules):
        legacy_capsules[name].get("meta", {}).pop("benchmark_id", None)
        refactor_capsules[name].get("meta", {}).pop("benchmark_id", None)
        if _normalize(legacy_capsules[name]) != _normalize(refactor_capsules[name]):
            return False, f"capsule mismatch: {name}"
    return True, "corpus parity ok"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", choices=["jsonl", "corpus"], required=True)
    ap.add_argument("--legacy", required=True)
    ap.add_argument("--refactor", required=True)
    args = ap.parse_args()

    legacy = Path(args.legacy)
    refactor = Path(args.refactor)

    if args.kind == "jsonl":
        ok, message = _compare_jsonl(legacy, refactor)
    else:
        ok, message = _compare_corpus_dirs(legacy, refactor)

    print(json.dumps({"ok": ok, "message": message}, indent=2))
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
