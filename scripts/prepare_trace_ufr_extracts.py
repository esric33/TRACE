from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
BENCHMARK_DIR = REPO_ROOT / "benchmarks" / "trace_ufr"
SNIPPETS_DIR = BENCHMARK_DIR / "snippets"
EXTRACTS_DIR = BENCHMARK_DIR / "extracts"


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _metric_key_for(label: str) -> str:
    metric_key = label
    if "_growth" in metric_key:
        metric_key = metric_key.split("_growth", 1)[0]
    return metric_key


def _metric_role_for(quantity: dict[str, Any]) -> str:
    return "rate" if quantity.get("type") == "rate" else "amount"


def main() -> None:
    snippets = {}
    for path in sorted(SNIPPETS_DIR.glob("*.json")):
        snippet = _load_json(path)
        snippet_id = snippet.get("snippet_id")
        if not isinstance(snippet_id, str) or not snippet_id:
            raise ValueError(f"snippet missing snippet_id: {path}")
        snippets[snippet_id] = snippet

    rewritten = 0
    unchanged = 0

    for path in sorted(EXTRACTS_DIR.glob("*.json")):
        extract = _load_json(path)
        snippet_id = extract.get("snippet_id")
        if not isinstance(snippet_id, str) or not snippet_id:
            raise ValueError(f"extract missing snippet_id: {path}")

        snippet = snippets.get(snippet_id)
        if snippet is None:
            raise KeyError(f"missing snippet for snippet_id={snippet_id}: {path}")

        company = str(((snippet.get("meta") or {}).get("company") or "")).strip()
        if not company:
            raise ValueError(f"missing snippet meta.company for {snippet_id}: {path}")

        label = extract.get("label")
        if not isinstance(label, str) or not label:
            raise ValueError(f"extract missing label: {path}")

        quantity = extract.get("quantity")
        if not isinstance(quantity, dict):
            raise ValueError(f"extract missing quantity object: {path}")

        normalized = {
            "extraction_id": extract["extraction_id"],
            "snippet_id": snippet_id,
            "label": label,
            "period": extract["period"],
            "quantity": quantity,
            "company": extract.get("company") or company,
            "metric_key": extract.get("metric_key") or _metric_key_for(label),
            "metric_role": extract.get("metric_role") or _metric_role_for(quantity),
        }

        for key, value in extract.items():
            if key not in normalized:
                normalized[key] = value

        if normalized == extract:
            unchanged += 1
            continue

        path.write_text(
            json.dumps(normalized, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        rewritten += 1

    print(
        json.dumps(
            {
                "extracts_dir": str(EXTRACTS_DIR),
                "rewritten": rewritten,
                "unchanged": unchanged,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
