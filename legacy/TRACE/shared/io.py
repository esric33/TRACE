import json
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Dict, Iterator, Union


def read_json(path):
    text = path.read_text(encoding="utf-8").strip()

    decoder = json.JSONDecoder()
    try:
        obj, end = decoder.raw_decode(text)
    except JSONDecodeError as e:
        print(f"\n❌ JSON decode error in file: {path}")
        print(e)
        raise

    # Optional: warn if there is non-whitespace after the JSON object
    rest = text[end:].strip()
    if rest:
        print(f"⚠️  Warning: trailing data ignored in {path}")

    return obj


def read_jsonl(path: Union[str, Path]) -> Iterator[Dict[str, Any]]:
    """
    Stream JSONL records.

    - Skips blank lines.
    - Raises with line number + filename on JSON errors.
    - Ensures each line decodes to a JSON object (dict) by default.
      (If you want to allow arrays/scalars later, relax the isinstance check.)
    """
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj: Any = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"JSONL decode error in {path} at line {lineno}: {e.msg}"
                ) from e

            if not isinstance(obj, dict):
                raise ValueError(
                    f"JSONL record in {path} at line {lineno} is not an object/dict: {type(obj).__name__}"
                )
            yield obj


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _clean_path(s: str) -> Path:
    return Path(s.strip()).expanduser()
