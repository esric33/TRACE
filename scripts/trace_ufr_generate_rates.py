"""
Generate rough FX tables for TRACE-UFR under benchmarks/trace_ufr/tables/fx.
"""

from __future__ import annotations

import json
import math
import random
from pathlib import Path
from typing import Dict, List


REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "benchmarks" / "trace_ufr" / "tables" / "fx"
CURRENCIES: List[str] = ["CHF", "EUR", "GBP", "JPY", "KRW", "RMB", "TWD", "USD"]
YEARS: List[int] = list(range(2018, 2026))
USD_ANCHOR: Dict[str, float] = {
    "USD": 1.0,
    "EUR": 0.92,
    "GBP": 0.78,
    "CHF": 0.90,
    "JPY": 120.0,
    "KRW": 1200.0,
    "TWD": 31.0,
    "RMB": 6.7,
}
USD_VOL: Dict[str, float] = {
    "USD": 0.0,
    "EUR": 0.05,
    "GBP": 0.06,
    "CHF": 0.05,
    "JPY": 0.10,
    "KRW": 0.12,
    "TWD": 0.04,
    "RMB": 0.04,
}
USD_CLAMP: Dict[str, tuple[float, float]] = {
    "USD": (1.0, 1.0),
    "EUR": (0.70, 1.30),
    "GBP": (0.55, 1.05),
    "CHF": (0.70, 1.30),
    "JPY": (70.0, 200.0),
    "KRW": (700.0, 2200.0),
    "TWD": (20.0, 45.0),
    "RMB": (4.5, 9.0),
}
ROUND_DP: Dict[str, int] = {
    "USD": 6,
    "EUR": 6,
    "GBP": 6,
    "CHF": 6,
    "RMB": 6,
    "TWD": 4,
    "JPY": 3,
    "KRW": 3,
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def generate_usd_series(currency: str, *, seed: int) -> Dict[str, float]:
    rng = random.Random(seed)
    anchor = USD_ANCHOR[currency]
    vol = USD_VOL[currency]
    low, high = USD_CLAMP[currency]
    if currency == "USD":
        return {str(year): 1.0 for year in YEARS}

    series: Dict[str, float] = {}
    value = _clamp(anchor * math.exp(rng.gauss(0.0, vol * 0.25)), low, high)
    for year in YEARS:
        series[str(year)] = value
        log_value = math.log(value)
        log_anchor = math.log(anchor)
        log_next = log_value + 0.35 * (log_anchor - log_value) + rng.gauss(0.0, vol)
        value = _clamp(math.exp(log_next), low, high)
    return series


def usd_to_pair_rate(usd_to_a: float, usd_to_b: float) -> float:
    return usd_to_b / usd_to_a


def main(out_dir: Path = OUT_DIR, seed: int = 7) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    usd_series = {
        currency: generate_usd_series(currency, seed=seed + 1000 * idx)
        for idx, currency in enumerate(CURRENCIES)
    }
    for base in CURRENCIES:
        for quote in CURRENCIES:
            if base == quote:
                continue
            rate_by_year: Dict[str, float] = {}
            for year in YEARS:
                rate = usd_to_pair_rate(
                    float(usd_series[base][str(year)]),
                    float(usd_series[quote][str(year)]),
                )
                rate_by_year[str(year)] = round(rate, ROUND_DP.get(quote, 6))
            payload = {
                "series_id": f"FX_{base}_{quote}",
                "rate_by_year": rate_by_year,
                "from": base,
                "to": quote,
            }
            (out_dir / f"fx_{base.lower()}_{quote.lower()}.json").write_text(
                json.dumps(payload, indent=2) + "\n",
                encoding="utf-8",
            )
    print(f"Wrote {len(CURRENCIES) * (len(CURRENCIES) - 1)} FX tables to {out_dir}")


if __name__ == "__main__":
    main()
