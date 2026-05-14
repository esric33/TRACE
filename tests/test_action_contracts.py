from __future__ import annotations

import unittest
from dataclasses import replace

from TRACE.core.actions import ActionRegistry, build_registry_for_benchmark
from TRACE.core.actions.types import ActionDef, ArgSpec, OutputSpec
from TRACE.core.benchmarks.loader import load_benchmark
from TRACE.core.executor.runtime import execute_dag
from TRACE.core.executor.support import ExecError
from TRACE.providers.shared.dag_validator import validate_dag_obj
from TRACE.providers.shared.prompt import build_planner_prompt

from tests.support import load_trace_ufr_benchmark, make_action_ctx


class ActionContractTests(unittest.TestCase):
    def test_action_registry_requires_explicit_override_flag(self) -> None:
        registry = ActionRegistry()
        original = ActionDef(
            name="CONST",
            arg_specs=(ArgSpec("value", "number"),),
            summary="Original const",
            output_spec=OutputSpec(
                category="quantity",
                summary="Scalar quantity",
                fixed_type="scalar",
                fixed_unit="",
                fixed_scale=1,
            ),
            executor=lambda *_args, **_kwargs: {
                "value": 1.0,
                "unit": "",
                "scale": 1,
                "type": "scalar",
            },
        )
        override = replace(original, summary="Override const")

        registry.register(original)
        with self.assertRaisesRegex(ValueError, "action already registered: CONST"):
            registry.register(override)

        registry.register(override, allow_override=True)
        self.assertEqual(registry.require("CONST").summary, "Override const")

    def test_action_prompt_doc_includes_summary_and_output_contract(self) -> None:
        benchmark_def = load_trace_ufr_benchmark()
        registry = build_registry_for_benchmark(benchmark_def)
        add = registry.require("ADD")

        self.assertIn("Add two matching quantities", add.prompt_doc())
        self.assertIn("returns=", add.prompt_doc())
        self.assertEqual(add.output_spec.summary, "Quantity with the same type, unit, and scale as a")

    def test_planner_prompt_operator_block_comes_from_action_contracts(self) -> None:
        benchmark_def = load_trace_ufr_benchmark()
        registry = build_registry_for_benchmark(benchmark_def)
        capsule = {
            "question": "Is revenue greater than cost?",
            "context": {"snippets": [{"snippet_id": "s1", "text": "demo"}]},
        }

        prompt = build_planner_prompt(capsule, benchmark_def=benchmark_def)

        self.assertIn(registry.require("ADD").prompt_doc(), prompt)
        self.assertIn(registry.require("FX_LOOKUP").prompt_doc(), prompt)
        self.assertIn("exactly one atomic extracted fact", prompt)
        self.assertIn("one quantity for one label and one period", prompt)

    def test_execute_dag_rejects_action_output_that_violates_contract(self) -> None:
        base = load_trace_ufr_benchmark()

        def register_actions(registry) -> None:
            registry.register(
                ActionDef(
                    name="BROKEN",
                    arg_specs=(),
                    summary="Return a malformed payload",
                    output_spec=OutputSpec(
                        category="quantity",
                        summary="Scalar quantity",
                        fixed_type="scalar",
                        fixed_unit="",
                        fixed_scale=1,
                    ),
                    executor=lambda *_args, **_kwargs: {"bad": "payload"},
                )
            )

        benchmark_def = replace(
            base,
            allowed_actions={"BROKEN"},
            register_actions=register_actions,
        )

        with self.assertRaises(ExecError) as cm:
            execute_dag(
                {"nodes": [{"id": "n1", "op": "BROKEN", "args": {}}], "output": "ref:n1"},
                benchmark_def,
                "provider",
                provider_ctx=type(
                    "ProviderCtx",
                    (),
                    {"fact_fn": lambda *_args, **_kwargs: {}, "extracts_by_snippet": {}},
                )(),
                oracle_ctx=None,
                capsule={"qid": "test", "context": {"snippets": []}},
                cache={},
            )

        self.assertEqual(cm.exception.code, "E_bad_output")
        self.assertEqual(cm.exception.data["phase"], "runtime")
        self.assertEqual(cm.exception.data["op"], "BROKEN")

    def test_validator_uses_overridden_action_contract(self) -> None:
        base = load_trace_ufr_benchmark()

        def register_actions(registry) -> None:
            registry.register(
                ActionDef(
                    name="CONST",
                    arg_specs=(
                        ArgSpec("value", "number"),
                        ArgSpec("scale", "number"),
                    ),
                    summary="Const with explicit scale override",
                    output_spec=OutputSpec(
                        category="quantity",
                        summary="Scalar quantity",
                        fixed_type="scalar",
                        fixed_unit="",
                        fixed_scale=1,
                    ),
                    executor=lambda *_args, **_kwargs: {
                        "value": 1.0,
                        "unit": "",
                        "scale": 1,
                        "type": "scalar",
                    },
                ),
                allow_override=True,
            )

        benchmark_def = replace(
            base,
            allowed_actions={"CONST"},
            register_actions=register_actions,
        )

        with self.assertRaisesRegex(ValueError, "CONST args must be exactly"):
            validate_dag_obj(
                {
                    "dag": {
                        "nodes": [{"id": "n1", "op": "CONST", "args": {"value": 1}}],
                        "output": "ref:n1",
                    }
                },
                benchmark_def=benchmark_def,
            )

    def test_trace_ufr_overrides_core_mul_contract_and_runtime(self) -> None:
        benchmark_def = load_trace_ufr_benchmark()
        registry = build_registry_for_benchmark(benchmark_def)
        mul = registry.require("MUL")

        self.assertIn("TRACE-UFR financial multiplication semantics", mul.summary)

        prompt = build_planner_prompt(
            {"question": "Convert revenue to GBP", "context": {"snippets": []}},
            benchmark_def=benchmark_def,
        )
        self.assertIn(mul.prompt_doc(), prompt)

        result = mul.executor(
            make_action_ctx(),
            "n1",
            {
                "a": {"value": 10.0, "unit": "USD", "scale": 1.0, "type": "money"},
                "b": {
                    "value": 0.8,
                    "unit": "fx_rate",
                    "scale": 1,
                    "type": "rate",
                    "from": {"currency": "USD"},
                    "to": {"currency": "GBP"},
                },
            },
        )
        self.assertEqual(result["unit"], "GBP")
        self.assertAlmostEqual(result["value"], 8.0)

    def test_trace_dir_overrides_model_fact_and_executes_set_contains(self) -> None:
        benchmark_def = load_benchmark("trace_dir")
        registry = build_registry_for_benchmark(benchmark_def)

        self.assertEqual(registry.require("MODEL_FACT").output_spec.category, "relation_set")
        self.assertIn("SET_CONTAINS", registry.allowed_ops())

        capsule = {
            "qid": "trace_dir_set_contains",
            "context": {
                "snippets": [
                    {
                        "snippet_id": "s1",
                        "text": "Aspirin is used to treat fever and pain.",
                    }
                ]
            },
        }
        fever_fact = {
            "snippet_id": "s1",
            "label": "treats_condition",
            "subject": {"type": "drug", "value": "Aspirin"},
            "object": {"type": "condition", "value": "fever"},
        }
        pain_fact = {
            "snippet_id": "s1",
            "label": "treats_condition",
            "subject": {"type": "drug", "value": "Aspirin"},
            "object": {"type": "condition", "value": "pain"},
        }
        result = execute_dag(
            {
                "nodes": [
                    {"id": "n1", "op": "MODEL_FACT", "args": fever_fact},
                    {"id": "n2", "op": "MODEL_FACT", "args": pain_fact},
                    {"id": "n3", "op": "MAKE_SET", "args": {"items": ["ref:n1", "ref:n2"]}},
                    {"id": "n4", "op": "SET_CONTAINS", "args": {"set": "ref:n3", "item": "ref:n1"}},
                ],
                "output": "ref:n4",
            },
            benchmark_def,
            capsule=capsule,
            cache={},
        )

        self.assertEqual(result["output"], {"value": True, "unit": "bool", "scale": 1, "type": "bool"})

    def test_benchmark_prompt_hooks_can_override_shared_builders(self) -> None:
        benchmark_def = replace(
            load_trace_ufr_benchmark(),
            build_planner_prompt=lambda *_args: "PLANNER OVERRIDE",
        )

        self.assertEqual(
            build_planner_prompt(
                {"question": "demo", "context": {"snippets": []}},
                benchmark_def=benchmark_def,
            ),
            "PLANNER OVERRIDE",
        )


if __name__ == "__main__":
    unittest.main()
