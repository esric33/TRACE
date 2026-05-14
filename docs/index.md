# TRACE Documentation

TRACE is a framework for generating and evaluating reasoning benchmarks where model outputs are executable directed acyclic graphs (DAGs), not free-form chain-of-thought.

```{toctree}
:maxdepth: 2
:caption: User Guide

quickstart
framework
capsules
benchmarks
actions
templates
generation
inference
reporting
extending
api/index
architecture_backlog
```

## What TRACE Provides

- Dataset construction from snippets, extracted facts, schemas, actions, and templates.
- Capsule generation with questions, snippet subsets, and gold executable DAGs.
- Model-planned DAG execution through provider backends.
- Evaluation of final answers, fact grounding, graph structure, and structured error codes.

## Included Benchmarks

- `trace_ufr`: unit-aware financial reasoning over company snippets, numeric quantities, FX tables, CPI tables, arithmetic, and comparisons.
- `trace_dir`: document/information-retrieval relation reasoning over medical snippets.
