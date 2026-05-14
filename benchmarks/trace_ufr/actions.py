from __future__ import annotations

from pathlib import Path
from typing import Any

from TRACE.core.actions.types import (
    ActionDef,
    ActionExecContext,
    ArgSpec,
    OutputSpec,
)
from TRACE.core.executor.support import (
    ExecErrorCode,
    ExecPhase,
    _get_q_period,
    _is_rate,
    _is_scalar,
    _rate_from,
    _rate_to,
    exec_error,
)
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


def _require_quantity(value: Any, *, op: str, arg: str) -> dict[str, Any]:
    if not isinstance(value, dict) or "value" not in value:
        raise exec_error(
            ExecErrorCode.TYPE_MISMATCH,
            f"{op} expected Quantity for {arg}",
            phase=ExecPhase.ACTION,
            op=op,
            arg=arg,
            expected="Quantity",
            got=value,
        )
    return value


def _is_currency_quantity(value: dict[str, Any]) -> bool:
    return value.get("type") in {"money", "per_share"}


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


def _exec_mul(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a = _require_quantity(args["a"], op="MUL", arg="a")
    b = _require_quantity(args["b"], op="MUL", arg="b")

    if _is_scalar(a) and _is_scalar(b):
        return {"value": float(a["value"]) * float(b["value"]), "unit": "", "scale": 1, "type": "scalar"}

    if a.get("type") == "money" and _is_scalar(b):
        return {**a, "value": float(a["value"]) * float(b["value"])}

    if _is_scalar(a) and b.get("type") == "money":
        return {**b, "value": float(a["value"]) * float(b["value"])}

    if _is_currency_quantity(a) and _is_rate(b, "fx_rate"):
        frm = _rate_from(b).get("currency")
        to = _rate_to(b).get("currency")
        if not isinstance(frm, str) or not isinstance(to, str):
            raise exec_error(
                ExecErrorCode.BAD_RATE,
                "fx_rate missing from/to.currency",
                phase=ExecPhase.ACTION,
                op="MUL",
                rate=b,
            )
        if a.get("unit") != frm:
            raise exec_error(
                ExecErrorCode.UNIT_MISMATCH,
                "FX rate from.currency must match money unit",
                phase=ExecPhase.ACTION,
                op="MUL",
                money_unit=a.get("unit"),
                rate_from=frm,
                rate=b,
            )
        return {**a, "value": float(a["value"]) * float(b["value"]), "unit": to}

    if a.get("type") == "money" and _is_rate(b, "cpi_rate"):
        fy = _rate_from(b).get("year")
        ty = _rate_to(b).get("year")
        if not isinstance(fy, int) or not isinstance(ty, int):
            raise exec_error(
                ExecErrorCode.BAD_RATE,
                "cpi_rate missing from/to.year ints",
                phase=ExecPhase.ACTION,
                op="MUL",
                rate=b,
            )

        period = _get_q_period(a)
        if period is None:
            raise exec_error(
                ExecErrorCode.MISSING_PERIOD,
                "CPI adjustment requires money to carry FY provenance (_period_kind/_period_value)",
                phase=ExecPhase.ACTION,
                op="MUL",
                money=a,
                rate=b,
            )
        period_kind, period_value = period
        if period_kind != "FY" or not isinstance(period_value, int):
            raise exec_error(
                ExecErrorCode.PERIOD_MISMATCH,
                "CPI adjustment requires FY int provenance on money",
                phase=ExecPhase.ACTION,
                op="MUL",
                money_period=period,
                rate=b,
            )
        if period_value != fy:
            raise exec_error(
                ExecErrorCode.PERIOD_MISMATCH,
                "CPI rate from_year must match money FY year",
                phase=ExecPhase.ACTION,
                op="MUL",
                money_year=period_value,
                rate_from_year=fy,
                rate=b,
            )
        return {**a, "value": float(a["value"]) * float(b["value"])}

    raise exec_error(
        ExecErrorCode.TYPE_MISMATCH,
        "MUL supports scalar*scalar, money*scalar, scalar*money, currency_quantity*fx_rate, money*cpi_rate",
        phase=ExecPhase.ACTION,
        op="MUL",
        a=a,
        b=b,
    )


def _exec_div(_: ActionExecContext, __: str, args: dict[str, Any]) -> dict[str, Any]:
    a = _require_quantity(args["a"], op="DIV", arg="a")
    b = _require_quantity(args["b"], op="DIV", arg="b")

    if a.get("type") == "rate" and a.get("unit") == "percent" and a.get("scale") == 1 and _is_scalar(b):
        denom = float(b["value"])
        if denom == 0.0:
            raise exec_error(
                ExecErrorCode.DIV_ZERO,
                "DIV by zero",
                phase=ExecPhase.ACTION,
                op="DIV",
                b=b,
            )
        return {"value": float(a["value"]) / denom, "unit": "", "scale": 1, "type": "scalar"}

    if _is_scalar(a) and _is_scalar(b):
        denom = float(b["value"])
        if denom == 0.0:
            raise exec_error(
                ExecErrorCode.DIV_ZERO,
                "DIV by zero",
                phase=ExecPhase.ACTION,
                op="DIV",
                b=b,
            )
        return {"value": float(a["value"]) / denom, "unit": "", "scale": 1, "type": "scalar"}

    if a.get("type") == "money" and b.get("type") == "money":
        if a.get("unit") != b.get("unit"):
            raise exec_error(
                ExecErrorCode.UNIT_MISMATCH,
                "DIV money currency mismatch",
                phase=ExecPhase.ACTION,
                op="DIV",
                a=a,
                b=b,
            )
        if a.get("scale") != b.get("scale"):
            raise exec_error(
                ExecErrorCode.SCALE_MISMATCH,
                "DIV money scale mismatch",
                phase=ExecPhase.ACTION,
                op="DIV",
                a=a,
                b=b,
            )
        denom = float(b["value"])
        if denom == 0.0:
            raise exec_error(
                ExecErrorCode.DIV_ZERO,
                "DIV by zero",
                phase=ExecPhase.ACTION,
                op="DIV",
                b=b,
            )
        return {"value": float(a["value"]) / denom, "unit": "", "scale": 1, "type": "scalar"}

    raise exec_error(
        ExecErrorCode.TYPE_MISMATCH,
        "DIV supports (percent/scalar)->scalar or (scalar/scalar)->scalar or (money/money)->scalar",
        phase=ExecPhase.ACTION,
        op="DIV",
        a=a,
        b=b,
    )


def register_actions(registry) -> None:
    registry.register(
        ActionDef(
            name="FX_LOOKUP",
            arg_specs=(
                ArgSpec("series_id", "string", non_empty=True),
                ArgSpec("year", "number"),
            ),
            summary="Load a yearly FX rate for a base and quote currency pair",
            output_spec=OutputSpec(
                category="quantity",
                summary="FX rate quantity",
                fixed_type="rate",
                fixed_unit="fx_rate",
                fixed_scale=1,
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
            summary="Load a CPI-based inflation adjustment rate between two years",
            output_spec=OutputSpec(
                category="quantity",
                summary="CPI adjustment rate quantity",
                fixed_type="rate",
                fixed_unit="cpi_rate",
                fixed_scale=1,
            ),
            executor=_exec_cpi_lookup,
        )
    )
    registry.register(
        ActionDef(
            name="MUL",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            summary="Apply TRACE-UFR financial multiplication semantics, including FX and CPI rates",
            output_spec=OutputSpec(
                category="quantity",
                summary="Scalar, scaled financial quantity, FX-converted quantity, or CPI-adjusted money",
            ),
            executor=_exec_mul,
        ),
        allow_override=True,
    )
    registry.register(
        ActionDef(
            name="DIV",
            arg_specs=(ArgSpec("a", "ref"), ArgSpec("b", "ref")),
            summary="Apply TRACE-UFR ratio semantics for scalar, percent, and matching-money division",
            output_spec=OutputSpec(
                category="quantity",
                summary="Scalar ratio produced by TRACE-UFR division rules",
                fixed_type="scalar",
                fixed_unit="",
                fixed_scale=1,
            ),
            executor=_exec_div,
        ),
        allow_override=True,
    )
