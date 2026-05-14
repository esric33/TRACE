#!/usr/bin/env python3
"""Sample correct executable TRACE cases with low precision for manual audit."""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


FIELD_ALIASES = {
    "qid": ("qid", "id", "question_id"),
    "dataset": ("dataset", "domain", "task", "benchmark", "benchmark_id", "corpus_id"),
    "provider": ("provider", "planner"),
    "model": ("model", "model_tag"),
    "question": ("question", "query"),
    "answer_correct": ("answer_correct", "correct", "is_correct"),
    "execution_success": ("execution_success", "executed"),
    "fact_precision": ("fact_precision", "retrieval_precision", "fact_prec"),
    "graph_precision": ("graph_precision", "dag_edge_prec", "dag_node_prec"),
    "fact_recall": ("fact_recall", "retrieval_recall", "fact_rec"),
    "graph_recall": ("graph_recall", "dag_edge_rec", "dag_node_rec"),
    "gold_output": ("gold_output", "expected_output", "gold_answer", "gold", "answer"),
    "predicted_output": ("predicted_output", "output", "pred_output"),
    "gold_dag": ("gold_dag", "reference_dag"),
    "predicted_dag": ("predicted_dag", "dag"),
    "gold_evidence": ("gold_evidence", "gold_factoids", "fact_gold_extraction_ids"),
    "predicted_evidence": ("predicted_evidence", "predicted_factoids", "fact_pred_extraction_ids"),
}


def _iter_json_file(path: Path) -> Iterable[dict[str, Any]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return
    if isinstance(data, dict):
        if isinstance(data.get("results"), list):
            for row in data["results"]:
                if isinstance(row, dict):
                    yield row
        elif isinstance(data.get("rows"), list):
            for row in data["rows"]:
                if isinstance(row, dict):
                    yield row
        else:
            yield data
    elif isinstance(data, list):
        for row in data:
            if isinstance(row, dict):
                yield row


def _iter_jsonl_file(path: Path) -> Iterable[dict[str, Any]]:
    try:
        fh = path.open("r", encoding="utf-8")
    except OSError:
        return
    with fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def _candidate_files(experiment_dir: Path) -> list[Path]:
    files: list[Path] = []
    for path in experiment_dir.rglob("*"):
        if path.is_file() and path.suffix.lower() in {".json", ".jsonl"}:
            parts = set(path.parts)
            if "__pycache__" in parts or "annotations" in parts:
                continue
            files.append(path)
    return sorted(files)


def _get(row: dict[str, Any], logical: str) -> Any:
    for key in FIELD_ALIASES[logical]:
        if key in row:
            return row[key]
    extra = row.get("extra")
    if isinstance(extra, dict):
        for key in FIELD_ALIASES[logical]:
            if key in extra:
                return extra[key]
    return None


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "ok", "success"}:
            return True
        if text in {"false", "0", "no", "failed", "fail", "error"}:
            return False
    return None


def _answer_correct(row: dict[str, Any]) -> bool | None:
    value = _as_bool(_get(row, "answer_correct"))
    if value is not None:
        return value
    accuracy = row.get("answer_accuracy")
    if accuracy is not None:
        try:
            return float(accuracy) == 1.0
        except (TypeError, ValueError):
            return None
    return None


def _execution_success(row: dict[str, Any]) -> bool | None:
    value = _as_bool(_get(row, "execution_success"))
    if value is not None:
        return value
    status = row.get("execution_status")
    if isinstance(status, str):
        return status.lower() == "ok"
    if row.get("exec_error_code") or row.get("exec_error"):
        return False
    if "output" in row:
        return True
    return None


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _norm_dataset(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if "trace_dir" in text:
        return "trace_dir"
    if "trace_ufr" in text:
        return "trace_ufr"
    return text


def _infer_from_path(path: Path) -> dict[str, str | None]:
    text = str(path)
    dataset = "trace_dir" if "trace_dir" in path.parts else "trace_ufr" if "trace_ufr" in path.parts else None
    provider = None
    model = None
    for part in path.parts:
        if part.startswith("provider="):
            provider = part.split("=", 1)[1]
        elif part.startswith("model="):
            model = part.split("=", 1)[1]
    if provider is None:
        for candidate in ("anthropic", "gemini", "openai"):
            if f"/{candidate}/" in text:
                provider = candidate
                break
    return {"dataset": dataset, "provider": provider, "model": model}


def _merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in extra.items():
        if key not in out or out[key] is None:
            out[key] = value
        elif key == "source_paths":
            out[key] = sorted(set(out[key]) | set(value))
    return out


def _normalize_record(row: dict[str, Any], source_path: Path) -> dict[str, Any]:
    inferred = _infer_from_path(source_path)
    qid = _get(row, "qid")
    dataset = _norm_dataset(_get(row, "dataset")) or inferred["dataset"]
    return {
        "qid": qid,
        "dataset": dataset,
        "provider": _get(row, "provider") or inferred["provider"],
        "model": _get(row, "model") or inferred["model"],
        "question": _get(row, "question"),
        "answer_correct": _answer_correct(row),
        "execution_success": _execution_success(row),
        "fact_precision": _as_float(_get(row, "fact_precision")),
        "graph_precision": _as_float(_get(row, "graph_precision")),
        "fact_recall": _as_float(_get(row, "fact_recall")),
        "graph_recall": _as_float(_get(row, "graph_recall")),
        "gold_output": _get(row, "gold_output"),
        "predicted_output": _get(row, "predicted_output"),
        "gold_dag": _get(row, "gold_dag"),
        "predicted_dag": _get(row, "predicted_dag"),
        "gold_evidence": _get(row, "gold_evidence"),
        "predicted_evidence": _get(row, "predicted_evidence"),
        "source_paths": [str(source_path)],
    }


def _join_key(record: dict[str, Any]) -> tuple[Any, ...] | None:
    if record["qid"] is None:
        return None
    dataset = record.get("dataset")
    provider = record.get("provider")
    model = record.get("model")
    if dataset is not None and provider is not None and model is not None:
        return (record["qid"], dataset, provider, model)
    return (record["qid"], dataset, provider, model)


def _load_records(experiment_dir: Path) -> tuple[list[dict[str, Any]], int]:
    files = _candidate_files(experiment_dir)
    by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    no_key: list[dict[str, Any]] = []
    for path in files:
        iterator = _iter_jsonl_file(path) if path.suffix.lower() == ".jsonl" else _iter_json_file(path)
        for row in iterator:
            record = _normalize_record(row, path)
            key = _join_key(record)
            if key is None:
                no_key.append(record)
                continue
            if key in by_key:
                by_key[key] = _merge(by_key[key], record)
            else:
                by_key[key] = record
    return [*by_key.values(), *no_key], len(files)


def _eligible(record: dict[str, Any], args: argparse.Namespace) -> bool:
    if record["answer_correct"] is not True or record["execution_success"] is not True:
        return False
    if args.lowest_graph_precision:
        gp = record.get("graph_precision")
        return gp is not None and gp < 1.0
    fp = record.get("fact_precision")
    gp = record.get("graph_precision")
    if fp is None and gp is None:
        return False
    return (fp is not None and fp <= args.max_fact_precision) or (
        gp is not None and gp <= args.max_graph_precision
    )


def _sample(eligible: list[dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    rng = random.Random(args.seed)
    by_dataset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in eligible:
        by_dataset[str(record.get("dataset") or "unknown")].append(record)
    for records in by_dataset.values():
        if args.lowest_graph_precision:
            records.sort(
                key=lambda r: (
                    r.get("graph_precision") if r.get("graph_precision") is not None else 2.0,
                    r.get("fact_precision") if r.get("fact_precision") is not None else 2.0,
                    str(r.get("provider")),
                    str(r.get("model")),
                    str(r.get("qid")),
                )
            )
        else:
            records.sort(
                key=lambda r: (
                    str(r.get("provider")),
                    str(r.get("model")),
                    str(r.get("qid")),
                )
            )
            rng.shuffle(records)

    selected: list[dict[str, Any]] = []
    targets = [("trace_dir", args.dir_target), ("trace_ufr", args.ufr_target)]
    used: set[int] = set()
    for dataset, target in targets:
        for record in by_dataset.get(dataset, [])[:target]:
            selected.append(record)
            used.add(id(record))

    if len(selected) < args.n:
        remainder = [record for record in eligible if id(record) not in used]
        remainder.sort(key=lambda r: (str(r.get("dataset")), str(r.get("model")), str(r.get("qid"))))
        rng.shuffle(remainder)
        selected.extend(remainder[: args.n - len(selected)])
    return selected[: args.n]


def _json_block(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False) if value is not None else "null"


def _write_jsonl(path: Path, cases: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for case in cases:
            fh.write(json.dumps(case, ensure_ascii=False) + "\n")


def _write_markdown(path: Path, cases: list[dict[str, Any]]) -> None:
    labels = (
        "VALID_ALTERNATIVE_TRACE",
        "EQUIVALENT_EVIDENCE_OR_DUPLICATE_ANSWER",
        "EXTRA_IRRELEVANT_EVIDENCE_BUT_REASONING_OK",
        "INCORRECT_INTERMEDIATE_REASONING_MASKED_BY_CORRECT_ANSWER",
        "METRIC_ARTEFACT_OR_MATCHING_TOO_STRICT",
        "OTHER_OR_UNCLEAR",
    )
    lines: list[str] = []
    for case in cases:
        lines.extend(
            [
                f"# CASE {case['annotation_id']}",
                "",
                "## Metadata",
                f"- dataset: {case.get('dataset')}",
                f"- provider: {case.get('provider')}",
                f"- model: {case.get('model')}",
                f"- qid: {case.get('qid')}",
                f"- fact_precision: {case.get('fact_precision')}",
                f"- graph_precision: {case.get('graph_precision')}",
                f"- fact_recall: {case.get('fact_recall')}",
                f"- graph_recall: {case.get('graph_recall')}",
                f"- source_paths: {case.get('source_paths')}",
                "",
                "## Question",
                str(case.get("question") or ""),
                "",
                "## Gold output",
                "```json",
                _json_block(case.get("gold_output")),
                "```",
                "",
                "## Predicted output",
                "```json",
                _json_block(case.get("predicted_output")),
                "```",
                "",
                "## Gold DAG",
                "```json",
                _json_block(case.get("gold_dag")),
                "```",
                "",
                "## Predicted DAG",
                "```json",
                _json_block(case.get("predicted_dag")),
                "```",
                "",
                "## Gold evidence / factoids",
                "```json",
                _json_block(case.get("gold_evidence")),
                "```",
                "",
                "## Predicted evidence / factoids",
                "```json",
                _json_block(case.get("predicted_evidence")),
                "```",
                "",
                "## Annotation",
                "",
                "Delete all options except the one that applies.",
                "",
                "ANNOTATION_LABEL: [",
                *[f"{label} /" for label in labels[:-1]],
                f"{labels[-1]}",
                "]",
                "",
                "## ANNOTATION_NOTES:",
                "",
                "---",
                "",
            ]
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-dir", type=Path, required=True)
    parser.add_argument("--n", type=int, default=30)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--max-fact-precision", type=float, default=0.75)
    parser.add_argument("--max-graph-precision", type=float, default=0.60)
    parser.add_argument("--dir-target", type=int, default=20)
    parser.add_argument("--ufr-target", type=int, default=10)
    parser.add_argument(
        "--lowest-graph-precision",
        action="store_true",
        help="Select the lowest graph_precision cases with graph_precision < 1.0; ignores fact precision thresholds.",
    )
    args = parser.parse_args()

    records, files_scanned = _load_records(args.experiment_dir)
    eligible = [record for record in records if _eligible(record, args)]
    sampled = _sample(eligible, args)
    for idx, case in enumerate(sampled, start=1):
        case["annotation_id"] = f"LP{idx:03d}"

    out_dir = args.experiment_dir / "annotations"
    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = out_dir / "annotation_cases.jsonl"
    md_path = out_dir / "annotation_cases.md"
    summary_path = out_dir / "annotation_summary.json"
    _write_jsonl(jsonl_path, sampled)
    _write_markdown(md_path, sampled)

    summary = {
        "files_scanned": files_scanned,
        "records_loaded": len(records),
        "eligible_low_precision_correct_cases": len(eligible),
        "sampled_cases": len(sampled),
        "sampled_cases_by_dataset": dict(Counter(str(case.get("dataset")) for case in sampled)),
        "selection": {
            "n": args.n,
            "seed": args.seed,
            "max_fact_precision": args.max_fact_precision,
            "max_graph_precision": args.max_graph_precision,
            "dir_target": args.dir_target,
            "ufr_target": args.ufr_target,
            "lowest_graph_precision": args.lowest_graph_precision,
        },
        "output_paths": {
            "jsonl": str(jsonl_path),
            "markdown": str(md_path),
            "summary": str(summary_path),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"files scanned: {files_scanned}")
    print(f"records loaded: {len(records)}")
    print(f"eligible low-precision correct cases: {len(eligible)}")
    print(f"sampled cases by dataset: {summary['sampled_cases_by_dataset']}")
    print(f"wrote: {jsonl_path}")
    print(f"wrote: {md_path}")
    print(f"wrote: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
