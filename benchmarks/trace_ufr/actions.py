from __future__ import annotations

from pathlib import Path
from typing import Any

from TRACE.core.actions.types import ActionDef, ActionExecContext
from TRACE.execute.executor import ExecError
from TRACE.shared.io import read_json


def _tables_dir() -> Path:
    return Path(__file__).resolve().parent / "tables"


def _fx_path(series_id: str) -> Path:
    return _tables_dir() / "fx" / f"{series_id.lower()}.json"


def _cpi_path(series_id: str) -> Path:
    if series_id != "CPI_US_CPIU":
        raise FileNotFoundError(series_id)
    return _tables_dir() / "cpi_us_cpiu.json"


def _canon_year(x: Any) -> int:
    if isinstance(x, int):
        return x
    s = str(x).strip()
    if s.isdigit():
        return int(s)
    raise ExecError("E_bad_args", "Expected year int (or numeric string)", {"got": x})


def _load_fx_table(series_id: str, *, cache: dict[str, Any]) -> dict[str, Any]:
    key = f"trace_ufr::fx_table::{series_id}"
    if key not in cache:
        path = _fx_path(series_id)
        try:
            table = read_json(path)
        except FileNotFoundError as exc:
            raise ExecError(
                "E_missing_table", "FX table not found", {"series_id": series_id}
            ) from exc
        if table.get("series_id") != series_id:
            raise ExecError(
                "E_bad_table", "FX table series_id mismatch", {"path": str(path)}
            )
        cache[key] = table
    return cache[key]


def _load_cpi_table(series_id: str, *, cache: dict[str, Any]) -> dict[str, Any]:
    key = f"trace_ufr::cpi_table::{series_id}"
    if key not in cache:
        try:
            path = _cpi_path(series_id)
        except FileNotFoundError as exc:
            raise ExecError(
                "E_bad_args", f"Unknown CPI series_id: {series_id}"
            ) from exc
        table = read_json(path)
        if table.get("series_id") != series_id:
            raise ExecError(
                "E_bad_table", "CPI table series_id mismatch", {"path": str(path)}
            )
        cache[key] = table
    return cache[key]


def _exec_fx_lookup(ctx: ActionExecContext, _: str, args: dict[str, Any]) -> dict[str, Any]:
    series_id = args["series_id"]
    year = _canon_year(args["year"])
    if not isinstance(series_id, str) or not series_id.strip():
        raise ExecError("E_bad_args", "FX_LOOKUP requires series_id string")

    table = _load_fx_table(series_id, cache=ctx.cache)
    rate = table.get("rate_by_year", {}).get(str(year))
    if rate is None:
        raise ExecError(
            "E_missing_table_key",
            "FX year not found",
            {"series_id": series_id, "year": year},
        )

    frm = table.get("from")
    to = table.get("to")
    if not isinstance(frm, str) or not isinstance(to, str):
        raise ExecError(
            "E_bad_table",
            "FX table must include 'from' and 'to' currency strings",
            {"series_id": series_id},
        )

    return {
        "value": float(rate),
        "unit": "fx_rate",
        "scale": 1,
        "type": "rate",
        "from": {"currency": frm},
        "to": {"currency": to},
        "at": {"year": year},
        "series_id": series_id,
    }


def _exec_cpi_lookup(ctx: ActionExecContext, _: str, args: dict[str, Any]) -> dict[str, Any]:
    series_id = args["series_id"]
    from_year = _canon_year(args["from_year"])
    to_year = _canon_year(args["to_year"])
    if not isinstance(series_id, str) or not series_id.strip():
        raise ExecError("E_bad_args", "CPI_LOOKUP requires series_id string")

    table = _load_cpi_table(series_id, cache=ctx.cache)
    index_by_year = table.get("index_by_year", {})
    to_value = index_by_year.get(str(to_year))
    from_value = index_by_year.get(str(from_year))
    if to_value is None:
        raise ExecError(
            "E_missing_table_key",
            "CPI year not found",
            {"series_id": series_id, "year": to_year},
        )
    if from_value is None:
        raise ExecError(
            "E_missing_table_key",
            "CPI year not found",
            {"series_id": series_id, "year": from_year},
        )
    denom = float(from_value)
    if denom == 0.0:
        raise ExecError("E_div_zero", "CPI rate denom is zero", {"from_year": from_year})

    return {
        "value": float(to_value) / denom,
        "unit": "cpi_rate",
        "scale": 1,
        "type": "rate",
        "from": {"year": from_year},
        "to": {"year": to_year},
        "series_id": series_id,
    }


def register_actions(registry) -> None:
    registry.register(
        ActionDef(
            name="FX_LOOKUP",
            arg_keys=("series_id", "year"),
            executor=_exec_fx_lookup,
        )
    )
    registry.register(
        ActionDef(
            name="CPI_LOOKUP",
            arg_keys=("series_id", "from_year", "to_year"),
            executor=_exec_cpi_lookup,
        )
    )
