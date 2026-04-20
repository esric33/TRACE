from __future__ import annotations

from pathlib import Path

from TRACE.generation.generation_types import load_extracts


EXTRACTS_DIR = Path(__file__).resolve().parent.parent / "extracts"


def main(extracts_dir: Path = EXTRACTS_DIR) -> None:
    extracts = load_extracts(extracts_dir)
    currencies = {
        extract.unit
        for extract in extracts
        if extract.qtype == "money" and isinstance(extract.unit, str) and extract.unit.strip()
    }
    for currency in sorted(currencies):
        print(currency)


if __name__ == "__main__":
    main()
