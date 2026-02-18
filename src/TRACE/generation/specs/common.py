# reason_bench/generation/specs/common.py
from __future__ import annotations

from typing import Callable, Sequence, Optional

from TRACE.generation.generation_types import (
    Bindings,
    CompiledPlan,
    ExtractRecord,
)

from pathlib import Path
from TRACE.shared.io import read_json


RenderFn = Callable[[Bindings, CompiledPlan], str]

INSTR = ""


def with_instr(q: str) -> str:
    return INSTR + "\n" + q


DEFAULT_SCALES: Sequence[float] = (
    1.0,
    1_000.0,
    1_000_000.0,
    1_000_000_000.0,
    1_000_000_000_000.0,
)


def label_renderer(label: str) -> str:
    return label.replace("_", " ").lower()


def period_renderer(r: ExtractRecord) -> str:
    return f"{r.period_kind} {r.period_value}"


def scale_renderer(scale: float) -> str:
    match float(scale):
        case 1.0:
            return ""
        case 1_000.0:
            return "thousands"
        case 1_000_000.0:
            return "millions"
        case 1_000_000_000.0:
            return "billions"
        case 1_000_000_000_000.0:
            return "trillions"
        case _:
            raise ValueError(f"Unknown scale: {scale}")


def dag_arg_single(compiled: CompiledPlan, *, op: str, arg: str) -> float:
    for n in compiled.dag["nodes"]:
        if n["op"] == op:
            return float(n["args"][arg])
    raise KeyError(f"Missing op={op} arg={arg} in compiled DAG")


# ... keep your existing code ...


def available_fx_pairs(
    fx_dir: Path = Path("data") / "tables" / "fx",
    *,
    prefer_json_series_id: bool = True,
) -> list[tuple[str, str]]:
    """
    Return [(BASE, QUOTE), ...] for all FX tables available under data/tables/fx.

    Accepts either:
      - filename: FX_<BASE>_<QUOTE>.json
      - OR series_id inside JSON: "FX_<BASE>_<QUOTE>"
    """
    pairs: set[tuple[str, str]] = set()
    if not fx_dir.exists():
        return []

    for p in fx_dir.glob("*.json"):
        base: Optional[str] = None
        quote: Optional[str] = None

        if prefer_json_series_id:
            try:
                tbl = read_json(p)
                sid = str(tbl.get("series_id", ""))
                if sid.startswith("FX_"):
                    parts = sid.split("_", 2)
                    # "FX", BASE, QUOTE
                    if len(parts) == 3:
                        base, quote = parts[1], parts[2]
            except Exception:
                pass

        if base is None or quote is None:
            name = p.stem  # e.g. FX_EUR_USD
            if name.startswith("FX_"):
                parts = name.split("_", 2)
                if len(parts) == 3:
                    base, quote = parts[1], parts[2]

        if base and quote:
            pairs.add((base, quote))

    return sorted(pairs)


def fx_quotes_for_base(
    base: str,
    pairs: Sequence[tuple[str, str]] | None = None,
) -> list[str]:
    if pairs is None:
        pairs = available_fx_pairs()
    return sorted({q for b, q in pairs if b == base})


def parse_fx_series_id(series_id: str) -> tuple[str, str]:
    # "FX_<BASE>_<QUOTE>"
    if not series_id.startswith("FX_"):
        raise ValueError(f"Not an FX series id: {series_id}")
    parts = series_id.split("_", 2)
    if len(parts) != 3:
        raise ValueError(f"Bad FX series id: {series_id}")
    return parts[1], parts[2]


def dag_arg_fx_series(compiled: CompiledPlan) -> tuple[str, str]:
    """
    Convenience: find the first FX_LOOKUP node and parse its series_id -> (BASE, QUOTE).
    Useful for render_question functions when FX pair is chosen at compile time.
    """
    for n in compiled.dag["nodes"]:
        if n["op"] == "FX_LOOKUP":
            sid = str(n["args"]["series_id"])
            return parse_fx_series_id(sid)
    raise KeyError("No FX_LOOKUP node found in compiled DAG")


# add near imports
from typing import Dict, Tuple

# ... keep your existing code ...


def build_default_fx_quotes(
    fx_dir: Path = Path("data") / "tables" / "fx",
) -> dict[str, tuple[str, ...]]:
    """
    Build {BASE: (QUOTE1, QUOTE2, ...)} from available FX tables.

    - Deduped
    - Sorted
    - No BASE->BASE no-ops
    """
    pairs = available_fx_pairs(fx_dir=fx_dir)
    m: dict[str, set[str]] = {}
    for base, quote in pairs:
        if base == quote:
            continue
        m.setdefault(base, set()).add(quote)

    # Freeze to tuples for stability / hashability / repr
    return {b: tuple(sorted(qs)) for b, qs in sorted(m.items())}


# Default FX domain: derived from shipped tables
DEFAULT_FX_QUOTES: dict[str, tuple[str, ...]] = build_default_fx_quotes()

if not DEFAULT_FX_QUOTES:
    raise RuntimeError(
        "No FX tables found under data/tables/fx; DEFAULT_FX_QUOTES is empty"
    )
