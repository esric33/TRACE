#!/usr/bin/env python3
"""Parse low-precision audit annotations from annotation_cases.md."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path


CASE_RE = re.compile(r"^# CASE\s+(\S+)\s*$", re.MULTILINE)
LABEL_LINE_RE = re.compile(r"^ANNOTATION_LABEL:\s*(.*)$", re.MULTILINE)
NOTES_RE = re.compile(r"## ANNOTATION_NOTES:\s*(.*?)(?:\n---|\Z)", re.DOTALL)


def _case_blocks(text: str) -> list[tuple[str, str]]:
    matches = list(CASE_RE.finditer(text))
    blocks: list[tuple[str, str]] = []
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        blocks.append((match.group(1), text[start:end]))
    return blocks


def _parse(text: str) -> list[dict[str, str | None]]:
    rows: list[dict[str, str | None]] = []
    for case_id, block in _case_blocks(text):
        label = None
        label_match = LABEL_LINE_RE.search(block)
        if label_match:
            raw = label_match.group(1).strip()
            if raw and "[" not in raw and "/" not in raw:
                token = raw.split()[0].strip()
                if re.fullmatch(r"[A-Z_]+", token):
                    label = token
        notes_match = NOTES_RE.search(block)
        rows.append(
            {
                "annotation_id": case_id,
                "annotation_label": label,
                "annotation_notes": notes_match.group(1).strip() if notes_match else "",
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--annotations-md", type=Path, required=True)
    args = parser.parse_args()

    text = args.annotations_md.read_text(encoding="utf-8")
    rows = _parse(text)
    out_dir = args.annotations_md.parent
    csv_path = out_dir / "annotation_results.csv"
    jsonl_path = out_dir / "annotation_results.jsonl"
    summary_path = out_dir / "annotation_label_summary.json"

    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["annotation_id", "annotation_label", "annotation_notes"]
        )
        writer.writeheader()
        writer.writerows(rows)

    with jsonl_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = {
        "annotations_md": str(args.annotations_md),
        "cases_parsed": len(rows),
        "label_counts": dict(Counter(row["annotation_label"] or "UNSET" for row in rows)),
        "output_paths": {
            "csv": str(csv_path),
            "jsonl": str(jsonl_path),
            "summary": str(summary_path),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"cases parsed: {len(rows)}")
    print(f"label counts: {summary['label_counts']}")
    print(f"wrote: {csv_path}")
    print(f"wrote: {jsonl_path}")
    print(f"wrote: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
