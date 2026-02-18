"""
Generate sensible-ish FX tables for every ordered currency pair among:

CHF, EUR, GBP, JPY, KRW, RMB, TWD, USD

Output format (one file per pair):
{
  "series_id": "FX_USD_KRW",
  "rate_by_year": {"2018": 1120.5, ...}
}

Semantics:
FX_A_B = units of B per 1 unit of A (so USD->KRW is KRW per USD).
"""

from __future__ import annotations

import json
import math
import random
from pathlib import Path
from typing import Dict, List

CURRENCIES: List[str] = ["CHF", "EUR", "GBP", "JPY", "KRW", "RMB", "TWD", "USD"]
YEARS: List[int] = list(range(2018, 2026))  # 2018..2025 inclusive


# Approx “USD -> currency” anchors (units of currency per 1 USD)
# These are deliberately rough but sensible.
USD_ANCHOR: Dict[str, float] = {
    "USD": 1.0,
    "EUR": 0.92,  # EUR per USD
    "GBP": 0.78,  # GBP per USD
    "CHF": 0.90,  # CHF per USD
    "JPY": 120.0,  # JPY per USD
    "KRW": 1200.0,  # KRW per USD
    "TWD": 31.0,  # TWD per USD
    "RMB": 6.7,  # RMB per USD (treat as CNY-ish)
}

# Annual vol (log-space). High-vol for KRW/JPY; low-vol for CHF/EUR/GBP.
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

# Clamps for USD->currency (keeps “sensible” even with randomness)
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

# Rounding per currency (quote currency) for nicer numbers
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


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def generate_usd_series(
    currency: str,
    *,
    seed: int,
    mean_revert: float = 0.35,  # pull back to anchor each year
    drift: float = 0.00,  # deterministic drift in log-space per year
) -> Dict[str, float]:
    """
    Returns mapping year->(currency per USD).
    Uses a mean-reverting log-random-walk so it stays plausible.
    """
    rng = random.Random(seed)

    anchor = USD_ANCHOR[currency]
    vol = USD_VOL[currency]
    lo, hi = USD_CLAMP[currency]

    out: Dict[str, float] = {}
    if currency == "USD":
        for y in YEARS:
            out[str(y)] = 1.0
        return out

    # start at anchor with a small initial perturbation
    x = anchor * math.exp(rng.gauss(0.0, vol * 0.25))
    x = _clamp(x, lo, hi)

    for y in YEARS:
        out[str(y)] = x
        # update next year (log space)
        logx = math.log(x)
        loga = math.log(anchor)
        # mean reversion + drift + noise
        logx_next = logx + mean_revert * (loga - logx) + drift + rng.gauss(0.0, vol)
        x = _clamp(math.exp(logx_next), lo, hi)

    return out


def usd_to_pair_rate(usd_to_a: float, usd_to_b: float) -> float:
    """
    Given:
      usd_to_a = units of A per 1 USD
      usd_to_b = units of B per 1 USD
    Then:
      1 A = (USD per A) USD = (1 / usd_to_a) USD
      -> in B: (1 / usd_to_a) * usd_to_b
      => FX_A_B = usd_to_b / usd_to_a  (units of B per 1 A)
    """
    return usd_to_b / usd_to_a


def main(out_dir: str = "data/tables/fx", seed: int = 7) -> None:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # Build USD->currency time series once, then derive every pair.
    usd_series: Dict[str, Dict[str, float]] = {}
    for i, c in enumerate(CURRENCIES):
        usd_series[c] = generate_usd_series(c, seed=seed + 1000 * i)

    # Emit every ordered pair A->B where A != B
    for a in CURRENCIES:
        for b in CURRENCIES:
            if a == b:
                continue

            rates_by_year: Dict[str, float] = {}
            for y in YEARS:
                usd_to_a = float(usd_series[a][str(y)])
                usd_to_b = float(usd_series[b][str(y)])
                rate = usd_to_pair_rate(usd_to_a, usd_to_b)
                rates_by_year[str(y)] = round(rate, ROUND_DP.get(b, 6))

            payload = {
                "series_id": f"FX_{a}_{b}",
                "rate_by_year": rates_by_year,
                "from": a,
                "to": b,
            }

            (out_path / f"fx_{a.lower()}_{b.lower()}.json").write_text(
                json.dumps(payload, indent=2), encoding="utf-8"
            )

    print(f"Wrote {len(CURRENCIES) * (len(CURRENCIES) - 1)} FX tables to {out_path}")


if __name__ == "__main__":
    main()
