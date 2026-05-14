# Benchmarks

Benchmarks live under `benchmarks/<benchmark_id>/`.

Each benchmark package can define:

- `benchmark.py`: benchmark manifest and behavior hooks.
- `snippets/`: source snippets.
- `extracts/`: extracted facts or relations.
- `schemas/`: allowed labels and schema files.
- `templates/`: query and DAG templates.
- `actions.py`: benchmark-specific action registration.
- `tables/`: optional lookup tables.
- `tools/`: optional maintenance tools.

The current benchmark packages are:

- `benchmarks/trace_ufr/`
- `benchmarks/trace_dir/`

Benchmark loading is handled by `TRACE.core.benchmarks.loader.load_benchmark`.
