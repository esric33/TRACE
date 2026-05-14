# TRACE-DIR Medical Relation Data

This directory contains scraped Wikipedia-derived medical snippets and normalized relation extracts.

Raw page-level scrape outputs live in `data/`. Run:

```bash
PYTHONPATH=src:. python -m benchmarks.trace_dir.tools.prepare_relation_extracts
```

to regenerate:

- `snippets/*.json`
- `extracts/*.json`
- `review.csv`

The normalized relation label set is intentionally small:

- `treats_condition`
- `contraindicated_for`
- `interacts_with`
- `causes_side_effect`

The converter drops source labels outside this set, including drug class, mechanism, and dosage-like facts. It maps source `medical_use` to `treats_condition`, source `adverse_effect` to `causes_side_effect`, and most source `contraindication_or_warning` facts to `contraindicated_for`. Warning facts from interaction sections that name another drug or drug class are mapped to `interacts_with`.
