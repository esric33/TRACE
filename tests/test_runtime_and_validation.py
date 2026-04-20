from __future__ import annotations

import unittest

from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.core.executor.runtime import execute_dag
from TRACE.core.executor.support import (
    ExecError,
    ExecErrorCode,
    ExecPhase,
    exec_error,
    exec_error_to_dict,
)
from TRACE.providers.shared.dag_validator import validate_dag_obj


class RuntimeAndValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.benchmark_def = load_benchmark("trace_ufr")
        self.capsule = {"qid": "test-qid", "context": {"snippets": []}}

    def test_execute_dag_returns_output_for_valid_const_graph(self) -> None:
        dag = {
            "nodes": [
                {"id": "n1", "op": "CONST", "args": {"value": 2}},
                {"id": "n2", "op": "CONST", "args": {"value": 3}},
                {"id": "n3", "op": "ADD", "args": {"a": "ref:n1", "b": "ref:n2"}},
            ],
            "output": "ref:n3",
        }

        result = execute_dag(
            dag,
            self.benchmark_def,
            "provider",
            provider_ctx=type(
                "ProviderCtx",
                (),
                {"lookup_fn": lambda *_args, **_kwargs: {}, "extracts_by_snippet": {}},
            )(),
            oracle_ctx=None,
            capsule=self.capsule,
            cache={},
        )

        self.assertEqual(result["output"]["value"], 5.0)
        self.assertEqual(result["output"]["type"], "scalar")

    def test_execute_dag_rejects_unknown_ref(self) -> None:
        dag = {
            "nodes": [
                {"id": "n1", "op": "CONST", "args": {"value": 2}},
                {"id": "n2", "op": "ADD", "args": {"a": "ref:n1", "b": "ref:n9"}},
            ],
            "output": "ref:n2",
        }

        with self.assertRaises(ExecError) as cm:
            execute_dag(
                dag,
                self.benchmark_def,
                "provider",
                provider_ctx=type(
                    "ProviderCtx",
                    (),
                    {
                        "lookup_fn": lambda *_args, **_kwargs: {},
                        "extracts_by_snippet": {},
                    },
                )(),
                oracle_ctx=None,
                capsule=self.capsule,
                cache={},
            )

        self.assertEqual(cm.exception.code, "E_bad_ref")
        self.assertEqual(cm.exception.data["phase"], "runtime")
        self.assertEqual(cm.exception.data["ref"], "ref:n9")

    def test_execute_dag_rejects_bad_arg_shape(self) -> None:
        dag = {
            "nodes": [{"id": "n1", "op": "CONST", "args": {"unexpected": 2}}],
            "output": "ref:n1",
        }

        with self.assertRaises(ExecError) as cm:
            execute_dag(
                dag,
                self.benchmark_def,
                "provider",
                provider_ctx=type(
                    "ProviderCtx",
                    (),
                    {
                        "lookup_fn": lambda *_args, **_kwargs: {},
                        "extracts_by_snippet": {},
                    },
                )(),
                oracle_ctx=None,
                capsule=self.capsule,
                cache={},
            )

        self.assertEqual(cm.exception.code, "E_bad_args")

    def test_exec_error_helper_normalizes_standard_payload_fields(self) -> None:
        error = exec_error(
            ExecErrorCode.BAD_ARGS,
            "bad args",
            phase=ExecPhase.ACTION,
            op="CONST",
            arg="value",
            got="x",
            benchmark_id="trace_ufr",
        )

        self.assertEqual(
            error.data,
            {
                "phase": "action",
                "op": "CONST",
                "arg": "value",
                "got": "x",
                "benchmark_id": "trace_ufr",
            },
        )
        self.assertEqual(
            exec_error_to_dict(error),
            {
                "code": "E_bad_args",
                "message": "bad args",
                "data": {
                    "phase": "action",
                    "op": "CONST",
                    "arg": "value",
                    "got": "x",
                    "benchmark_id": "trace_ufr",
                },
            },
        )

    def test_validate_dag_obj_accepts_valid_graph(self) -> None:
        planner = {
            "dag": {
                "nodes": [
                    {
                        "id": "n1",
                        "op": "TEXT_LOOKUP",
                        "args": {"query": "company=Alphabet label=revenue period=FY 2022"},
                    },
                    {"id": "n2", "op": "GET_QUANTITY", "args": {"fact": "ref:n1"}},
                ],
                "output": "ref:n2",
            }
        }

        dag = validate_dag_obj(planner, benchmark_def=self.benchmark_def)

        self.assertEqual(dag["output"], "ref:n2")

    def test_validate_dag_obj_rejects_forward_reference(self) -> None:
        planner = {
            "dag": {
                "nodes": [
                    {"id": "n1", "op": "ADD", "args": {"a": "ref:n2", "b": "ref:n2"}},
                    {"id": "n2", "op": "CONST", "args": {"value": 1}},
                ],
                "output": "ref:n1",
            }
        }

        with self.assertRaisesRegex(ValueError, "appears later"):
            validate_dag_obj(planner, benchmark_def=self.benchmark_def)

    def test_validate_dag_obj_rejects_invalid_const_payload(self) -> None:
        planner = {
            "dag": {
                "nodes": [{"id": "n1", "op": "CONST", "args": {"value": "x"}}],
                "output": "ref:n1",
            }
        }

        with self.assertRaisesRegex(ValueError, "CONST.value must be a number"):
            validate_dag_obj(planner, benchmark_def=self.benchmark_def)


if __name__ == "__main__":
    unittest.main()
