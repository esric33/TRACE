# Actions

Actions are typed operations available to DAG nodes.

The main Python API is {py:class}`TRACE.core.actions.types.ActionDef`, with argument and output contracts represented by {py:class}`TRACE.core.actions.types.ArgSpec` and {py:class}`TRACE.core.actions.types.OutputSpec`.

The shared action registry includes operations such as:

- `MODEL_FACT`
- `CONVERT_SCALE`
- `CONST`
- `ADD`
- `MUL`
- `DIV`
- `GT`
- `LT`
- `EQ`
- `AND`
- `OR`

Benchmarks can register additional actions or override built-in actions. TRACE-UFR registers `FX_LOOKUP` and `CPI_LOOKUP`, and overrides multiplication/division semantics for financial quantities. See {py:func}`benchmarks.trace_ufr.actions.register_actions` for the benchmark-specific registration point and {py:func}`TRACE.core.actions.builtin.build_registry_for_benchmark` for the framework-level registry assembly.

Action definitions specify argument validation, output validation, prompt documentation, and executor behavior.

Reference pages:

- {doc}`api/actions`
- {doc}`api/trace_ufr`
- {doc}`api/trace_dir`
