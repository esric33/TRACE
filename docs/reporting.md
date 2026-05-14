# Reporting

TRACE writes per-capsule records and aggregate summaries.

Common outputs include:

- `results.jsonl`: leaf job results.
- `results_all.jsonl`: aggregated sweep results.
- `summary.json`: machine-readable aggregate metrics.
- `summary.md`: human-readable aggregate report.
- `meta.json`: run metadata.

Reported fields cover final answer correctness, provider/model metadata, execution status, structured error codes, fact grounding, graph metrics, distractor level, and template metadata.
