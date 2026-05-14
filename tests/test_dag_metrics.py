from __future__ import annotations

from TRACE.reporting.dag_metrics import (
    dag_struct_metrics,
    fact_grounding_diagnostics,
    fact_grounding_metrics,
)


def test_dag_metrics_include_calculation_constants() -> None:
    gold = {
        "nodes": [
            {"id": "n1", "op": "MODEL_FACT", "args": {"query": "revenue"}},
            {"id": "n2", "op": "MODEL_FACT", "args": {"fact": "ref:n1"}},
            {"id": "n3", "op": "FX_LOOKUP", "args": {"series_id": "FX_EUR_USD", "year": 2024}},
            {"id": "n4", "op": "MUL", "args": {"a": "ref:n2", "b": "ref:n3"}},
        ],
        "output": "ref:n4",
    }
    pred = {
        "nodes": [
            {"id": "x1", "op": "MODEL_FACT", "args": {"query": "revenue"}},
            {"id": "x2", "op": "MODEL_FACT", "args": {"fact": "ref:x1"}},
            {"id": "x3", "op": "FX_LOOKUP", "args": {"series_id": "FX_KRW_USD", "year": 2024}},
            {"id": "x4", "op": "MUL", "args": {"a": "ref:x2", "b": "ref:x3"}},
        ],
        "output": "ref:x4",
    }

    metrics = dag_struct_metrics(gold, pred)

    assert metrics["dag_node_f1"] < 1.0
    assert metrics["dag_edge_f1"] < 1.0
    assert not metrics["dag_exact"]


def test_model_fact_struct_metrics_normalize_period_and_quantity_numbers() -> None:
    gold = {
        "nodes": [
            {
                "id": "n1",
                "op": "MODEL_FACT",
                "args": {
                    "snippet_id": "s1",
                    "label": "revenue",
                    "period": {"period": "FY", "value": 2019},
                    "quantity": {"value": 10, "unit": "USD", "scale": 1000, "type": "money"},
                },
            },
            {
                "id": "n2",
                "op": "MODEL_FACT",
                "args": {
                    "snippet_id": "s1",
                    "label": "gross_profit",
                    "period": {"period": "FY", "value": 2019},
                    "quantity": {"value": 2, "unit": "USD", "scale": 1000, "type": "money"},
                },
            },
            {"id": "n3", "op": "ADD", "args": {"a": "ref:n1", "b": "ref:n2"}},
        ],
        "output": "ref:n3",
    }
    pred = {
        "nodes": [
            {
                "id": "x1",
                "op": "MODEL_FACT",
                "args": {
                    "snippet_id": "s1",
                    "label": "gross_profit",
                    "period": {"period": "FY", "value": "2019"},
                    "quantity": {"value": 2.0, "unit": "USD", "scale": 1000.0, "type": "money"},
                },
            },
            {
                "id": "x2",
                "op": "MODEL_FACT",
                "args": {
                    "snippet_id": "s1",
                    "label": "revenue",
                    "period": {"period": "FY", "value": "2019"},
                    "quantity": {"value": 10.0, "unit": "USD", "scale": 1000.0, "type": "money"},
                },
            },
            {"id": "x3", "op": "ADD", "args": {"a": "ref:x1", "b": "ref:x2"}},
        ],
        "output": "ref:x3",
    }

    metrics = dag_struct_metrics(gold, pred)

    assert metrics["dag_exact"]
    assert metrics["dag_node_f1"] == 1.0
    assert metrics["dag_edge_f1"] == 1.0


def test_model_fact_struct_metrics_normalize_two_digit_financial_years() -> None:
    gold = {
        "nodes": [
            {
                "id": "n1",
                "op": "MODEL_FACT",
                "args": {
                    "snippet_id": "sony_2021_revenue",
                    "label": "revenue",
                    "period": {"period": "FY", "value": 2020},
                    "quantity": {"value": 10, "unit": "JPY", "scale": 1000000000, "type": "money"},
                },
            }
        ],
        "output": "ref:n1",
    }
    pred = {
        "nodes": [
            {
                "id": "x1",
                "op": "MODEL_FACT",
                "args": {
                    "snippet_id": "sony_2021_revenue",
                    "label": "revenue",
                    "period": {"period": "FY", "value": "FY20"},
                    "quantity": {"value": 10.0, "unit": "JPY", "scale": 1000000000.0, "type": "money"},
                },
            }
        ],
        "output": "ref:x1",
    }

    metrics = dag_struct_metrics(gold, pred)

    assert metrics["dag_exact"]
    assert metrics["dag_node_f1"] == 1.0
    assert metrics["dag_edge_f1"] == 0.0


def test_fact_metrics_normalize_two_digit_financial_years() -> None:
    capsule = {
        "gold": {
            "fact_map": {"n1": "sony_revenue_2020"},
            "dag": {
                "nodes": [
                    {
                        "id": "n1",
                        "op": "MODEL_FACT",
                        "args": {
                            "snippet_id": "sony_2021_revenue",
                            "label": "revenue",
                            "period": {"period": "FY", "value": 2020},
                            "quantity": {"value": 10, "unit": "JPY", "scale": 1000000000, "type": "money"},
                        },
                    }
                ],
                "output": "ref:n1",
            },
        }
    }
    trace = [
        {
            "node": "x1",
            "op": "MODEL_FACT",
            "args": {
                "snippet_id": "sony_2021_revenue",
                "label": "revenue",
                "period": {"period": "FY", "value": "FY20"},
                "quantity": {"value": 10.0, "unit": "JPY", "scale": 1000000000.0, "type": "money"},
            },
            "result": {},
        }
    ]

    metrics = fact_grounding_metrics(capsule, trace)

    assert metrics["fact_prec"] == 1.0
    assert metrics["fact_rec"] == 1.0


def test_fact_metrics_use_resolved_extraction_id() -> None:
    capsule = {
        "gold": {"fact_map": {"n1": "gold_extraction"}},
        "meta": {
            "extraction_ids": ["gold_extraction"],
            "snippet_ids": ["snippet_a"],
            "labels": ["revenue"],
            "periods": [{"period": "FY", "value": 2024}],
        },
    }
    trace = [
        {
            "op": "MODEL_FACT",
            "model_fact": {
                "snippet_id": "snippet_a",
                "label": "revenue",
                "period": {"period": "FY", "value": 2024},
                "quantity": {"value": 99, "unit": "USD", "scale": 1, "type": "money"},
            },
            "resolve_tag": {"status": "UNRESOLVED", "candidates": []},
        }
    ]

    metrics = fact_grounding_metrics(capsule, trace)

    assert metrics["fact_rec"] == 0.0
    assert metrics["fact_prec"] == 0.0
    assert metrics["fact_f1"] == 0.0


def test_relation_fact_metrics_resolve_by_subject_object_content() -> None:
    capsule = {
        "gold": {
            "fact_map": {"n1": "aspirin_treats_fever"},
            "dag": {
                "nodes": [
                    {
                        "id": "n1",
                        "op": "MODEL_FACT",
                        "args": {
                            "snippet_id": "aspirin_use",
                            "label": "treats_condition",
                            "subject": {"type": "drug", "value": "Aspirin"},
                            "object": {"type": "condition", "value": "fever"},
                        },
                    }
                ],
                "output": "ref:n1",
            },
        }
    }
    trace = [
        {
            "node": "x1",
            "op": "MODEL_FACT",
            "args": {
                "snippet_id": "aspirin_use",
                "label": "treats_condition",
                "subject": {"type": "drug", "value": "aspirin"},
                "object": {"type": "condition", "value": "Fever"},
            },
            "result": {},
        }
    ]

    metrics = fact_grounding_metrics(capsule, trace)

    assert metrics["fact_prec"] == 1.0
    assert metrics["fact_rec"] == 1.0
    assert metrics["fact_f1"] == 1.0


def test_relation_fact_metrics_normalize_simple_surface_variants() -> None:
    capsule = {
        "gold": {
            "fact_map": {"n1": "atorvastatin_side_effect"},
            "dag": {
                "nodes": [
                    {
                        "id": "n1",
                        "op": "MODEL_FACT",
                        "args": {
                            "snippet_id": "atorvastatin_use",
                            "label": "causes_side_effect",
                            "subject": {"type": "drug", "value": "Atorvastatin"},
                            "object": {"type": "effect", "value": "liver problem"},
                        },
                    }
                ],
                "output": "ref:n1",
            },
        }
    }
    trace = [
        {
            "node": "x1",
            "op": "MODEL_FACT",
            "args": {
                "snippet_id": "atorvastatin_use",
                "label": "causes_side_effect",
                "subject": {"type": "drug", "value": "atorvastatin"},
                "object": {"type": "effect", "value": "Liver problems"},
            },
            "result": {},
        }
    ]

    metrics = fact_grounding_metrics(capsule, trace)

    assert metrics["fact_prec"] == 1.0
    assert metrics["fact_rec"] == 1.0
    assert metrics["fact_f1"] == 1.0


def test_unresolved_fact_attempts_do_not_count_as_predicted_fact_precision() -> None:
    capsule = {
        "gold": {
            "fact_map": {"n1": "aspirin_treats_fever"},
            "dag": {
                "nodes": [
                    {
                        "id": "n1",
                        "op": "MODEL_FACT",
                        "args": {
                            "snippet_id": "aspirin_use",
                            "label": "treats_condition",
                            "subject": {"type": "drug", "value": "Aspirin"},
                            "object": {"type": "condition", "value": "fever"},
                        },
                    }
                ],
                "output": "ref:n1",
            },
        }
    }
    trace = [
        {
            "node": "x1",
            "op": "MODEL_FACT",
            "args": {
                "snippet_id": "aspirin_use",
                "label": "treats_condition",
                "subject": {"type": "drug", "value": "Aspirin"},
                "object": {"type": "condition", "value": "not in gold"},
            },
            "result": {},
        }
    ]

    metrics = fact_grounding_metrics(capsule, trace)
    diagnostics = fact_grounding_diagnostics(capsule, trace)

    assert metrics["fact_pred_n"] == 0
    assert metrics["fact_prec"] == 0.0
    assert metrics["fact_rec"] == 0.0
    assert diagnostics["fact_unresolved"] == 1
    assert diagnostics["fact_over_extraction"] == 1


def test_make_set_items_count_as_dag_dependencies() -> None:
    gold = {
        "nodes": [
            {"id": "n1", "op": "MODEL_FACT", "args": {"snippet_id": "s", "label": "treats_condition", "subject": {"type": "drug", "value": "A"}, "object": {"type": "condition", "value": "x"}}},
            {"id": "n2", "op": "MODEL_FACT", "args": {"snippet_id": "s", "label": "treats_condition", "subject": {"type": "drug", "value": "A"}, "object": {"type": "condition", "value": "y"}}},
            {"id": "n3", "op": "MAKE_SET", "args": {"items": ["ref:n1", "ref:n2"]}},
        ],
        "output": "ref:n3",
    }
    pred = {
        "nodes": [
            {"id": "x1", "op": "MODEL_FACT", "args": {"snippet_id": "s", "label": "treats_condition", "subject": {"type": "drug", "value": "A"}, "object": {"type": "condition", "value": "y"}}},
            {"id": "x2", "op": "MODEL_FACT", "args": {"snippet_id": "s", "label": "treats_condition", "subject": {"type": "drug", "value": "A"}, "object": {"type": "condition", "value": "x"}}},
            {"id": "x3", "op": "MAKE_SET", "args": {"items": ["ref:x1", "ref:x2"]}},
        ],
        "output": "ref:x3",
    }

    metrics = dag_struct_metrics(gold, pred)

    assert metrics["dag_exact"]
    assert metrics["dag_edge_f1"] == 1.0
