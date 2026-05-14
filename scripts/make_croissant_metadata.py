#!/usr/bin/env python3
"""Create Croissant metadata files for TRACE corpus directories."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any


DEFAULT_CREATORS = [
    {"@type": "sc:Person", "name": "Edward Richards"},
    {"@type": "sc:Person", "name": "Javier Sanz-Cruzado Puig"},
    {"@type": "sc:Person", "name": "Richard McCreadie"},
]

DATASET_DESCRIPTIONS = {
    "trace_ufr": {
        "name": "TRACE-UFR",
        "alternateName": "TRACE Unit-Aware Financial Reasoning",
        "description": (
            "TRACE-UFR is a unit-aware financial reasoning benchmark. Each "
            "example is a self-contained TRACE capsule containing a natural-"
            "language question, supporting financial evidence snippets, a gold "
            "answer, and a gold executable reasoning DAG."
        ),
        "keywords": [
            "reasoning",
            "question answering",
            "financial reasoning",
            "unit conversion",
            "executable traces",
            "directed acyclic graphs",
        ],
    },
    "trace_dir": {
        "name": "TRACE-DIR",
        "alternateName": "TRACE Drug Interaction and Treatment Reasoning",
        "description": (
            "TRACE-DIR is a drug interaction and treatment reasoning benchmark. "
            "Each example is a self-contained TRACE capsule containing a natural-"
            "language question, supporting drug-relation evidence snippets, a "
            "gold answer, and a gold executable reasoning DAG."
        ),
        "keywords": [
            "reasoning",
            "question answering",
            "drug interactions",
            "set reasoning",
            "executable traces",
            "directed acyclic graphs",
        ],
    },
}

CROISSANT_CONTEXT = {
    "@language": "en",
    "@vocab": "https://schema.org/",
    "sc": "https://schema.org/",
    "cr": "http://mlcommons.org/croissant/",
    "dct": "http://purl.org/dc/terms/",
    "citeAs": "cr:citeAs",
    "conformsTo": "dct:conformsTo",
    "data": {"@id": "cr:data", "@type": "@json"},
    "dataType": {"@id": "cr:dataType", "@type": "@vocab"},
    "extract": "cr:extract",
    "field": "cr:field",
    "fileObject": "cr:fileObject",
    "fileSet": "cr:fileSet",
    "fileProperty": "cr:fileProperty",
    "includes": "cr:includes",
    "jsonPath": "cr:jsonPath",
    "key": "cr:key",
    "recordSet": "cr:recordSet",
    "source": "cr:source",
}


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _content_size(path: Path) -> str:
    return f"{path.stat().st_size} bytes"


def _field(
    record_set: str,
    name: str,
    description: str,
    data_type: str,
    file_id: str,
    json_path: str,
) -> dict[str, Any]:
    return {
        "@type": "cr:Field",
        "@id": f"{record_set}/{name}",
        "name": name,
        "description": description,
        "dataType": data_type,
        "source": {
            "fileObject": {"@id": file_id},
            "extract": {"jsonPath": json_path},
        },
    }


def _profile_summary(profile: dict[str, Any]) -> str:
    total_queries = profile.get("total_queries")
    total_templates = profile.get("total_templates")
    total_families = profile.get("total_families")
    return (
        f"{total_queries} queries generated from {total_templates} templates "
        f"across {total_families} reasoning families."
    )


def build_metadata(corpus_dir: Path, *, license_text: str, repository_url: str | None) -> dict[str, Any]:
    corpus_dir = corpus_dir.resolve()
    dataset_id = corpus_dir.name
    info = DATASET_DESCRIPTIONS.get(
        dataset_id,
        {
            "name": dataset_id,
            "alternateName": dataset_id,
            "description": "TRACE benchmark corpus.",
            "keywords": ["reasoning", "question answering", "executable traces"],
        },
    )

    capsules_path = corpus_dir / "capsules.jsonl"
    profile_path = corpus_dir / "benchmark_profile.json"
    profile_md_path = corpus_dir / "benchmark_profile.md"
    meta_path = corpus_dir / "meta.json"
    full_capsule_count = len(list(corpus_dir.glob("d=*/*.json")))
    profile = _json(profile_path) if profile_path.exists() else {}
    meta = _json(meta_path) if meta_path.exists() else {}

    distribution: list[dict[str, Any]] = [
        {
            "@type": "cr:FileObject",
            "@id": "capsules_jsonl",
            "name": "capsules.jsonl",
            "description": "JSONL index of TRACE evaluation capsules.",
            "contentUrl": "capsules.jsonl",
            "contentSize": _content_size(capsules_path),
            "encodingFormat": "application/jsonl",
        },
        {
            "@type": "cr:FileObject",
            "@id": "benchmark_profile_json",
            "name": "benchmark_profile.json",
            "description": "Machine-readable summary statistics for the TRACE corpus.",
            "contentUrl": "benchmark_profile.json",
            "contentSize": _content_size(profile_path),
            "encodingFormat": "application/json",
        },
        {
            "@type": "cr:FileObject",
            "@id": "meta_json",
            "name": "meta.json",
            "description": "Corpus generation metadata.",
            "contentUrl": "meta.json",
            "contentSize": _content_size(meta_path),
            "encodingFormat": "application/json",
        },
        {
            "@type": "cr:FileSet",
            "@id": "full_capsule_json_files",
            "name": "Full capsule JSON files",
            "description": "One full TRACE capsule JSON file per evaluation instance.",
            "includes": "d=*/*.json",
            "encodingFormat": "application/json",
        },
    ]
    if profile_md_path.exists():
        distribution.append(
            {
                "@type": "cr:FileObject",
                "@id": "benchmark_profile_md",
                "name": "benchmark_profile.md",
                "description": "Human-readable summary statistics for the TRACE corpus.",
                "contentUrl": "benchmark_profile.md",
                "contentSize": _content_size(profile_md_path),
                "encodingFormat": "text/markdown",
            }
        )

    metadata: dict[str, Any] = {
        "@context": CROISSANT_CONTEXT,
        "@type": "sc:Dataset",
        "name": info["name"],
        "alternateName": info["alternateName"],
        "description": f"{info['description']} {_profile_summary(profile)}",
        "conformsTo": "http://mlcommons.org/croissant/1.0",
        "license": license_text,
        "creator": DEFAULT_CREATORS,
        "dateCreated": str(date.today()),
        "keywords": info["keywords"],
        "isAccessibleForFree": True,
        "inLanguage": "en",
        "version": str(meta.get("generator_version") or "1.0"),
        "distribution": distribution,
        "recordSet": [
            {
                "@type": "cr:RecordSet",
                "@id": "capsules",
                "name": "capsules",
                "description": (
                    "One record per TRACE evaluation capsule. The full capsule "
                    "content is stored in the file referenced by capsule_path."
                ),
                "key": [{"@id": "capsules/qid"}],
                "field": [
                    _field("capsules", "qid", "Unique capsule identifier.", "sc:Text", "capsules_jsonl", "$.qid"),
                    _field("capsules", "question", "Natural-language question.", "sc:Text", "capsules_jsonl", "$.question"),
                    _field("capsules", "template_id", "TRACE generation template identifier.", "sc:Text", "capsules_jsonl", "$.template_id"),
                    _field("capsules", "qkey", "Coarse question type.", "sc:Text", "capsules_jsonl", "$.qkey"),
                    _field("capsules", "family", "Reasoning family.", "sc:Text", "capsules_jsonl", "$.family"),
                    _field("capsules", "seed", "Sampling seed used to instantiate the capsule.", "sc:Integer", "capsules_jsonl", "$.seed"),
                    _field("capsules", "distractor", "Number of distractor snippets included in the context.", "sc:Integer", "capsules_jsonl", "$.distractor"),
                    _field("capsules", "capsule_path", "Relative path to the full capsule JSON file.", "sc:Text", "capsules_jsonl", "$.capsule_path"),
                ],
            },
            {
                "@type": "cr:RecordSet",
                "@id": "full_capsules",
                "name": "full_capsules",
                "description": (
                    "Full TRACE capsule files containing context snippets, gold "
                    "answers, gold executable DAGs, and generation metadata."
                ),
                "field": [
                    {
                        "@type": "cr:Field",
                        "@id": "full_capsules/path",
                        "name": "path",
                        "description": "Relative path of the full capsule JSON file.",
                        "dataType": "sc:Text",
                        "source": {
                            "fileSet": {"@id": "full_capsule_json_files"},
                            "extract": {"fileProperty": "fullpath"},
                        },
                    },
                    {
                        "@type": "cr:Field",
                        "@id": "full_capsules/content",
                        "name": "content",
                        "description": "Raw JSON content of the full capsule.",
                        "dataType": "sc:Text",
                        "source": {
                            "fileSet": {"@id": "full_capsule_json_files"},
                            "extract": {"fileProperty": "content"},
                        },
                    },
                ],
            },
            {
                "@type": "cr:RecordSet",
                "@id": "benchmark_profile",
                "name": "benchmark_profile",
                "description": "Single-record benchmark profile summary.",
                "field": [
                    _field("benchmark_profile", "total_queries", "Total number of generated queries.", "sc:Integer", "benchmark_profile_json", "$.total_queries"),
                    _field("benchmark_profile", "total_templates", "Total number of question templates.", "sc:Integer", "benchmark_profile_json", "$.total_templates"),
                    _field("benchmark_profile", "total_families", "Total number of reasoning families.", "sc:Integer", "benchmark_profile_json", "$.total_families"),
                ],
            },
        ],
        "additionalProperty": [
            {
                "@type": "sc:PropertyValue",
                "name": "full_capsule_count",
                "value": full_capsule_count,
            },
            {
                "@type": "sc:PropertyValue",
                "name": "corpus_id",
                "value": dataset_id,
            },
        ],
    }
    if repository_url:
        metadata["url"] = repository_url
        metadata["codeRepository"] = repository_url
    return metadata


def write_metadata(corpus_dir: Path, *, output_name: str, license_text: str, repository_url: str | None) -> Path:
    metadata = build_metadata(corpus_dir, license_text=license_text, repository_url=repository_url)
    out_path = corpus_dir / output_name
    out_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "corpus_dirs",
        nargs="+",
        type=Path,
        help="Corpus directories containing capsules.jsonl, benchmark_profile.json, and d=*/*.json.",
    )
    parser.add_argument("--output-name", default="metadata.json")
    parser.add_argument("--repository-url", default="https://github.com/esric33/TRACE")
    parser.add_argument("--license", default="License pending; see repository License.txt")
    args = parser.parse_args()

    for corpus_dir in args.corpus_dirs:
        out = write_metadata(
            corpus_dir,
            output_name=args.output_name,
            license_text=args.license,
            repository_url=args.repository_url,
        )
        # Reload to catch accidental non-JSON output before reporting success.
        _json(out)
        print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
