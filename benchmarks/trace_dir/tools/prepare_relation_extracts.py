from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any


BENCHMARK_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BENCHMARK_DIR / "data"
SNIPPETS_DIR = BENCHMARK_DIR / "snippets"
EXTRACTS_DIR = BENCHMARK_DIR / "extracts"
REVIEW_PATH = BENCHMARK_DIR / "review.csv"

KEEP_LABELS = {
    "treats_condition",
    "contraindicated_for",
    "interacts_with",
    "causes_side_effect",
}

SOURCE_LABEL_MAP = {
    "medical_use": "treats_condition",
    "adverse_effect": "causes_side_effect",
}

OBJECT_TYPE_BY_LABEL = {
    "treats_condition": "condition",
    "contraindicated_for": "condition",
    "interacts_with": "drug",
    "causes_side_effect": "effect",
}

DRUG_NAME_BY_RAW_FILE = {
    "asprin": "Aspirin",
    "aspirin": "Aspirin",
    "atorvastatin": "Atorvastatin",
    "codeine": "Codeine",
    "diazepam": "Diazepam",
    "hydrocortisone": "Hydrocortisone",
    "ibuprofen": "Ibuprofen",
    "insulin": "Insulin",
    "metformin": "Metformin",
    "morphin": "Morphine",
    "morphine": "Morphine",
    "paracetamol": "Paracetamol",
    "acetaminophen": "Paracetamol",
    "warfarin": "Warfarin",
}

DRUG_ALIASES = {
    "acetaminophen": "Paracetamol",
    "amiodarone": "Amiodarone",
    "aprepitant": "Aprepitant",
    "aspirin": "Aspirin",
    "atorvastatin": "Atorvastatin",
    "clarithromycin": "Clarithromycin",
    "clozapine": "Clozapine",
    "cyclosporine": "Cyclosporine",
    "diltiazem": "Diltiazem",
    "erythromycin": "Erythromycin",
    "fluconazole": "Fluconazole",
    "ibuprofen": "Ibuprofen",
    "insulin": "Insulin",
    "itraconazole": "Itraconazole",
    "ketoconazole": "Ketoconazole",
    "metformin": "Metformin",
    "naproxen": "Naproxen",
    "olanzapine": "Olanzapine",
    "paracetamol": "Paracetamol",
    "telithromycin": "Telithromycin",
    "verapamil": "Verapamil",
    "voriconazole": "Voriconazole",
    "warfarin": "Warfarin",
}

INTERACTION_CLASS_ALIASES = {
    "anticholinergic medication": "anticholinergic medications",
    "anticholinergic drugs": "anticholinergic drugs",
    "cyp3a4 inhibitor": "CYP3A4 inhibitors",
    "nonsteroidal anti-inflammatory drug": "nonsteroidal anti-inflammatory drugs",
    "protease inhibitor": "protease inhibitors",
}


def _load_json(path: Path) -> dict[str, Any] | None:
    if path.stat().st_size == 0:
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _clean_source(source: object) -> str:
    text = str(source or "").strip()
    match = re.fullmatch(r"\[(https?://[^\]]+)\]\((https?://[^)]+)\)", text)
    if match:
        return match.group(2)
    return text


def _slug(text: object) -> str:
    value = str(text or "").strip().lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "unknown"


def _canonical_drug(raw_name: str, page: dict[str, Any]) -> str:
    raw_key = _slug(raw_name)
    if raw_key in DRUG_NAME_BY_RAW_FILE:
        return DRUG_NAME_BY_RAW_FILE[raw_key]
    for snippet in page.get("snippets", []):
        snippet_id = str(snippet.get("snippet_id") or "")
        if not snippet_id:
            continue
        prefix = snippet_id.split("_", 1)[0]
        if prefix in DRUG_NAME_BY_RAW_FILE:
            return DRUG_NAME_BY_RAW_FILE[prefix]
    return raw_name.replace("_", " ").strip().title()


def _normalize_common_object(text: object) -> str:
    value = str(text or "").strip()
    value = value.replace("–", "-").replace("—", "-")
    value = re.sub(r"\s+", " ", value)
    value = value.strip(" .;:,")
    return value


def _normalize_relation_object(text: object, *, label: str) -> str:
    value = _normalize_common_object(text)
    lower = value.lower()
    if label == "interacts_with":
        lower = re.sub(r"\b(use|therapy|treatment)$", "", lower).strip()
        if lower in DRUG_ALIASES:
            return DRUG_ALIASES[lower]
        if lower in INTERACTION_CLASS_ALIASES:
            return INTERACTION_CLASS_ALIASES[lower]
        return lower
    return lower


def _target_label(extraction: dict[str, Any], snippet: dict[str, Any]) -> str | None:
    source_label = str(extraction.get("label") or "").strip()
    if source_label in SOURCE_LABEL_MAP:
        return SOURCE_LABEL_MAP[source_label]
    if source_label != "contraindication_or_warning":
        return None
    section = str(snippet.get("section") or "").lower()
    raw_text = str(extraction.get("text") or "").lower().strip()
    if "interaction" in section and re.search(r"\b(use|therapy)$", raw_text):
        return "interacts_with"
    return "contraindicated_for"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _clear_json_files(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for path in directory.glob("*.json"):
        path.unlink()


def _unique_id(base: str, seen: set[str]) -> str:
    candidate = base
    idx = 2
    while candidate in seen:
        candidate = f"{base}_{idx}"
        idx += 1
    seen.add(candidate)
    return candidate


def convert(
    *,
    data_dir: Path = DATA_DIR,
    snippets_dir: Path = SNIPPETS_DIR,
    extracts_dir: Path = EXTRACTS_DIR,
    review_path: Path = REVIEW_PATH,
) -> dict[str, Any]:
    _clear_json_files(snippets_dir)
    _clear_json_files(extracts_dir)
    review_path.parent.mkdir(parents=True, exist_ok=True)

    snippets_by_id: dict[str, dict[str, Any]] = {}
    extracts: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []
    seen_extract_ids: set[str] = set()
    skipped_pages: list[dict[str, str]] = []
    duplicate_keys: set[tuple[str, str, str, str]] = set()

    for path in sorted(data_dir.glob("*.json")):
        try:
            page = _load_json(path)
        except Exception as exc:
            skipped_pages.append({"file": path.name, "reason": f"invalid_json: {exc}"})
            continue
        if page is None:
            skipped_pages.append({"file": path.name, "reason": "empty"})
            continue

        subject = _canonical_drug(path.stem, page)
        page_snippets = {
            str(snippet.get("snippet_id")): snippet
            for snippet in page.get("snippets", [])
            if isinstance(snippet, dict) and snippet.get("snippet_id")
        }

        for raw in page.get("extractions", []):
            if not isinstance(raw, dict):
                continue
            snippet_id = str(raw.get("snippet_id") or "")
            snippet = page_snippets.get(snippet_id)
            if snippet is None:
                review_rows.append(
                    {
                        "status": "dropped",
                        "reason": "missing_snippet",
                        "raw_file": path.name,
                        "snippet_id": snippet_id,
                        "source_label": raw.get("label", ""),
                        "source_type": raw.get("type", ""),
                        "target_label": "",
                        "subject": subject,
                        "object": raw.get("text", ""),
                        "normalized_object": "",
                    }
                )
                continue

            target_label = _target_label(raw, snippet)
            if target_label not in KEEP_LABELS:
                review_rows.append(
                    {
                        "status": "dropped",
                        "reason": "label_not_kept",
                        "raw_file": path.name,
                        "snippet_id": snippet_id,
                        "source_label": raw.get("label", ""),
                        "source_type": raw.get("type", ""),
                        "target_label": target_label or "",
                        "subject": subject,
                        "object": raw.get("text", ""),
                        "normalized_object": "",
                    }
                )
                continue

            normalized_object = _normalize_relation_object(
                raw.get("text", ""),
                label=target_label,
            )
            if not normalized_object:
                continue

            dedupe_key = (snippet_id, target_label, subject, normalized_object)
            if dedupe_key in duplicate_keys:
                review_rows.append(
                    {
                        "status": "dropped",
                        "reason": "duplicate",
                        "raw_file": path.name,
                        "snippet_id": snippet_id,
                        "source_label": raw.get("label", ""),
                        "source_type": raw.get("type", ""),
                        "target_label": target_label,
                        "subject": subject,
                        "object": raw.get("text", ""),
                        "normalized_object": normalized_object,
                    }
                )
                continue
            duplicate_keys.add(dedupe_key)

            object_type = OBJECT_TYPE_BY_LABEL[target_label]
            extraction_id = _unique_id(
                f"{_slug(subject)}_{target_label}_{_slug(normalized_object)}",
                seen_extract_ids,
            )
            extract = {
                "extraction_id": extraction_id,
                "snippet_id": snippet_id,
                "label": target_label,
                "subject": {"type": "drug", "value": subject},
                "object": {"type": object_type, "value": normalized_object},
            }
            extracts.append(extract)

            if snippet_id not in snippets_by_id:
                snippets_by_id[snippet_id] = {
                    "snippet_id": snippet_id,
                    "text": str(snippet.get("text") or "").strip(),
                    "source": _clean_source(snippet.get("source")),
                    "meta": {
                        "drug": subject,
                        "section": str(snippet.get("section") or "").strip(),
                    },
                }

            review_rows.append(
                {
                    "status": "included",
                    "reason": "",
                    "raw_file": path.name,
                    "snippet_id": snippet_id,
                    "source_label": raw.get("label", ""),
                    "source_type": raw.get("type", ""),
                    "target_label": target_label,
                    "subject": subject,
                    "object": raw.get("text", ""),
                    "normalized_object": normalized_object,
                }
            )

    for snippet_id, snippet in sorted(snippets_by_id.items()):
        _write_json(snippets_dir / f"{snippet_id}.json", snippet)
    for extract in sorted(extracts, key=lambda item: item["extraction_id"]):
        _write_json(extracts_dir / f"{extract['extraction_id']}.json", extract)

    with review_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "status",
            "reason",
            "raw_file",
            "snippet_id",
            "source_label",
            "source_type",
            "target_label",
            "subject",
            "object",
            "normalized_object",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(review_rows)

    summary = {
        "data_dir": str(data_dir),
        "snippets_dir": str(snippets_dir),
        "extracts_dir": str(extracts_dir),
        "review_path": str(review_path),
        "snippets": len(snippets_by_id),
        "extracts": len(extracts),
        "included_rows": sum(1 for row in review_rows if row["status"] == "included"),
        "dropped_rows": sum(1 for row in review_rows if row["status"] == "dropped"),
        "skipped_pages": skipped_pages,
        "labels": sorted(KEEP_LABELS),
    }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--snippets-dir", type=Path, default=SNIPPETS_DIR)
    parser.add_argument("--extracts-dir", type=Path, default=EXTRACTS_DIR)
    parser.add_argument("--review-path", type=Path, default=REVIEW_PATH)
    args = parser.parse_args()
    summary = convert(
        data_dir=args.data_dir,
        snippets_dir=args.snippets_dir,
        extracts_dir=args.extracts_dir,
        review_path=args.review_path,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
