from __future__ import annotations

from pathlib import Path
from typing import Any

from TRACE.core.actions.types import ActionDef, ActionExecContext, ArgSpec
from TRACE.core.executor.support import ExecErrorCode, ExecPhase, exec_error
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
    raise exec_error(
        ExecErrorCode.BAD_ARGS,
        "Expected year int (or numeric string)",
        phase=ExecPhase.ACTION,
        got=x,
    )


def _load_fx_table(series_id: str, *, cache: dict[str, Any]) -> dict[str, Any]:
    key = f"trace_ufr::fx_table::{series_id}"
    if key not in cache:
        path = _fx_path(series_id)
        try:
            table = read_json(path)
        except FileNotFoundError as exc:
            raise exec_error(
                ExecErrorCode.MISSING_TABLE,
                "FX table not found",
                phase=ExecPhase.ACTION,
                op="FX_LOOKUP",
                series_id=series_id,
            ) from exc
        if table.get("series_id") != series_id:
            raise exec_error(
                ExecErrorCode.BAD_TABLE,
                "FX table series_id mismatch",
                phase=ExecPhase.ACTION,
                op="FX_LOOKUP",
                series_id=series_id,
                path=str(path),
            )
        cache[key] = table
    return cache[key]


def _load_cpi_table(series_id: str, *, cache: dict[str, Any]) -> dict[str, Any]:
    key = f"trace_ufr::cpi_table::{series_id}"
    if key not in cache:
        try:
            path = _cpi_path(series_id)
        except FileNotFoundError as exc:
            raise exec_error(
                ExecErrorCode.BAD_ARGS,
                f"Unknown CPI series_id: {series_id}",
                phase=ExecPhase.ACTION,
                op="CPI_LOOKUP",
                series_id=series_id,
            ) from exc
        table = read_json(path)
        if table.get("series_id") != series_id:
            raise exec_error(
                ExecErrorCode.BAD_TABLE,
                "CPI table series_id mismatch",
                phase=ExecPhase.ACTION,
                op="CPI_LOOKUP",
                series_id=series_id,
                path=str(path),
            )
        cache[key] = table
    return cache[key]


def _exec_fx_lookup(ctx: ActionExecContext, _: str, args: dict[str, Any]) -> dict[str, Any]:
    series_id = args["series_id"]
    year = _canon_year(args["year"])
    if not isinstance(series_id, str) or not series_id.strip():
        raise exec_error(
            ExecErrorCode.BAD_ARGS,
            "FX_LOOKUP requires series_id string",
            phase=ExecPhase.ACTION,
            op="FX_LOOKUP",
            arg="series_id",
            got=series_id,
        )

    table = _load_fx_table(series_id, cache=ctx.cache)
    rate = table.get("rate_by_year", {}).get(str(year))
    if rate is None:
        raise exec_error(
            ExecErrorCode.MISSING_TABLE_KEY,
            "FX year not found",
            phase=ExecPhase.ACTION,
            op="FX_LOOKUP",
            series_id=series_id,
            year=year,
        )

    frm = table.get("from")
    to = table.get("to")
    if not isinstance(frm, str) or not isinstance(to, str):
        raise exec_error(
            ExecErrorCode.BAD_TABLE,
            "FX table must include 'from' and 'to' currency strings",
            phase=ExecPhase.ACTION,
            op="FX_LOOKUP",
            series_id=series_id,
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
        raise exec_error(
            ExecErrorCode.BAD_ARGS,
            "CPI_LOOKUP requires series_id string",
            phase=ExecPhase.ACTION,
            op="CPI_LOOKUP",
            arg="series_id",
            got=series_id,
        )

    table = _load_cpi_table(series_id, cache=ctx.cache)
    index_by_year = table.get("index_by_year", {})
    to_value = index_by_year.get(str(to_year))
    from_value = index_by_year.get(str(from_year))
    if to_value is None:
        raise exec_error(
            ExecErrorCode.MISSING_TABLE_KEY,
            "CPI year not found",
            phase=ExecPhase.ACTION,
            op="CPI_LOOKUP",
            series_id=series_id,
            year=to_year,
        )
    if from_value is None:
        raise exec_error(
            ExecErrorCode.MISSING_TABLE_KEY,
            "CPI year not found",
            phase=ExecPhase.ACTION,
            op="CPI_LOOKUP",
            series_id=series_id,
            year=from_year,
        )
    denom = float(from_value)
    if denom == 0.0:
        raise exec_error(
            ExecErrorCode.DIV_ZERO,
            "CPI rate denom is zero",
            phase=ExecPhase.ACTION,
            op="CPI_LOOKUP",
            from_year=from_year,
        )

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
            arg_specs=(
                ArgSpec("series_id", "string", non_empty=True),
                ArgSpec("year", "number"),
            ),
            executor=_exec_fx_lookup,
        )
    )
    registry.register(
        ActionDef(
            name="CPI_LOOKUP",
            arg_specs=(
                ArgSpec("series_id", "string", non_empty=True),
                ArgSpec("from_year", "number"),
                ArgSpec("to_year", "number"),
            ),
            executor=_exec_cpi_lookup,
        )
    )
