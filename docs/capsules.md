# Capsules

A capsule is the unit of TRACE evaluation.

Typical fields include:

- `qid`: stable question identifier.
- `question`: natural-language query.
- `context.snippets`: source snippets provided to the model.
- `gold.dag`: reference executable DAG.
- `gold.answer`: expected answer.
- `gold.fact_map`: provenance map used for grounding metrics.
- `meta`: benchmark, template, distractor, seed, and compilation metadata.

Capsules are written into distractor-specific directories such as `d=0/`, `d=3/`, and `d=10/`. A corpus-level `capsules.jsonl` index records the generated examples.
