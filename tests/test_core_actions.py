from __future__ import annotations

import unittest

from TRACE.core.actions.builtin import (
    _exec_add,
    _exec_div,
    _exec_mul,
)
from TRACE.core.executor.support import ExecError, convert_scale

from tests.support import make_action_ctx


class CoreActionTests(unittest.TestCase):
    def test_add_returns_sum_for_matching_money_quantities(self) -> None:
        ctx = make_action_ctx()
        a = {"value": 10.0, "unit": "USD", "scale": 1_000_000.0, "type": "money"}
        b = {"value": 5.5, "unit": "USD", "scale": 1_000_000.0, "type": "money"}

        result = _exec_add(ctx, "n1", {"a": a, "b": b})

        self.assertEqual(
            result,
            {"value": 15.5, "unit": "USD", "scale": 1_000_000.0, "type": "money"},
        )

    def test_add_rejects_unit_mismatch(self) -> None:
        ctx = make_action_ctx()
        a = {"value": 10.0, "unit": "USD", "scale": 1.0, "type": "money"}
        b = {"value": 5.0, "unit": "EUR", "scale": 1.0, "type": "money"}

        with self.assertRaises(ExecError) as cm:
            _exec_add(ctx, "n1", {"a": a, "b": b})

        self.assertEqual(cm.exception.code, "E_unit_mismatch")
        self.assertEqual(cm.exception.data["phase"], "action")
        self.assertEqual(cm.exception.data["op"], "ADD")

    def test_mul_money_by_fx_rate_changes_currency(self) -> None:
        ctx = make_action_ctx()
        money = {"value": 10.0, "unit": "USD", "scale": 1.0, "type": "money"}
        rate = {
            "value": 0.8,
            "unit": "fx_rate",
            "scale": 1,
            "type": "rate",
            "from": {"currency": "USD"},
            "to": {"currency": "GBP"},
        }

        result = _exec_mul(ctx, "n1", {"a": money, "b": rate})

        self.assertEqual(result["unit"], "GBP")
        self.assertAlmostEqual(result["value"], 8.0)

    def test_mul_rejects_fx_rate_with_wrong_source_currency(self) -> None:
        ctx = make_action_ctx()
        money = {"value": 10.0, "unit": "USD", "scale": 1.0, "type": "money"}
        rate = {
            "value": 130.0,
            "unit": "fx_rate",
            "scale": 1,
            "type": "rate",
            "from": {"currency": "JPY"},
            "to": {"currency": "USD"},
        }

        with self.assertRaises(ExecError) as cm:
            _exec_mul(ctx, "n1", {"a": money, "b": rate})

        self.assertEqual(cm.exception.code, "E_unit_mismatch")

    def test_div_money_by_money_returns_scalar(self) -> None:
        ctx = make_action_ctx()
        a = {"value": 21.0, "unit": "USD", "scale": 1.0, "type": "money"}
        b = {"value": 7.0, "unit": "USD", "scale": 1.0, "type": "money"}

        result = _exec_div(ctx, "n1", {"a": a, "b": b})

        self.assertEqual(result["type"], "scalar")
        self.assertAlmostEqual(result["value"], 3.0)

    def test_convert_scale_rejects_zero_target(self) -> None:
        quantity = {"value": 10.0, "unit": "USD", "scale": 1000.0, "type": "money"}

        with self.assertRaises(ExecError) as cm:
            convert_scale(quantity, 0)

        self.assertEqual(cm.exception.code, "E_bad_args")
        self.assertEqual(cm.exception.data["phase"], "support")


if __name__ == "__main__":
    unittest.main()
