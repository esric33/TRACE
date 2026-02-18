from pathlib import Path
from TRACE.generation.generation_types import load_extracts


def list_currencies(extracts_dir: Path) -> None:
    extracts = load_extracts(extracts_dir)

    currencies = {
        r.unit
        for r in extracts
        if r.qtype == "money" and isinstance(r.unit, str) and r.unit.strip()
    }

    for c in sorted(currencies):
        print(c)


if __name__ == "__main__":
    list_currencies(Path("data/extracts"))
