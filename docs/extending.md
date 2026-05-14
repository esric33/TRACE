# Extending TRACE

## Add a Benchmark

Create a package under `benchmarks/<your_benchmark>/` and define a `benchmark.py` manifest. The manifest is loaded into {py:class}`TRACE.core.benchmarks.types.BenchmarkDef`. Add snippets, extracts, schemas, templates, and optional action/table/tool modules.

## Add an Action

1. Implement an executor that accepts an {py:class}`TRACE.core.actions.types.ActionExecContext`, node id, and args dictionary.
2. Register an {py:class}`TRACE.core.actions.types.ActionDef`.
3. Add the action name to the benchmark's `ALLOWED_ACTIONS`.
4. Add prompt guidance if the planner needs benchmark-specific rules.
5. Add tests for validation and execution behavior.

## Add a Template

1. Add a {py:class}`TRACE.generation.generation_types.Spec` in the benchmark's `templates/` package.
2. Define slots, constraints, question text, and gold operation structure.
3. Register the spec in the template registry.
4. Generate a small corpus to verify sampling and compilation.
5. Add focused tests for the new template.
