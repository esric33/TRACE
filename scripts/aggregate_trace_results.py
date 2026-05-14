#!/usr/bin/env python3
import json
from pathlib import Path
from collections import Counter
import pandas as pd

FILES = [
    Path("outputs/runs/run_others_d3/results_all.jsonl"),
    Path("outputs/runs/run_openai_52_d3_full/results_all.jsonl"),
]


def norm_error(row):
    if row.get("exec_error_code"):
        return row["exec_error_code"].replace("E_", "")
    if row.get("mismatch_kind"):
        return row["mismatch_kind"]
    if row.get("correct") is False:
        return "wrong_answer_no_exec_error"
    return None


rows = []
for fp in FILES:
    with fp.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                r = json.loads(line)
                r["source_file"] = str(fp)
                r["error_kind"] = norm_error(r)
                rows.append(r)

df = pd.DataFrame(rows)

# Basic sanity
print("Rows:", len(df))
print("Models:", sorted(df["model_tag"].dropna().unique()))

metric_cols = [
    "correct",
    "dag_node_prec",
    "dag_node_rec",
    "dag_node_f1",
    "dag_edge_prec",
    "dag_edge_rec",
    "dag_edge_f1",
    "fact_prec",
    "fact_rec",
    "fact_f1",
    "dag_exact",
    "trace_nodes",
    "dag_nodes_gold",
    "dag_nodes_pred",
    "dag_edges_gold",
    "dag_edges_pred",
]

for c in metric_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

summary = (
    df.groupby("model_tag")
    .agg(
        n=("qid", "count"),
        accuracy=("correct", "mean"),
        exec_fail_rate=("exec_error_code", lambda s: s.notna().mean()),
        dag_exact_rate=("dag_exact", "mean"),
        node_f1=("dag_node_f1", "mean"),
        edge_f1=("dag_edge_f1", "mean"),
        fact_f1=("fact_f1", "mean"),
        node_precision=("dag_node_prec", "mean"),
        node_recall=("dag_node_rec", "mean"),
        edge_precision=("dag_edge_prec", "mean"),
        edge_recall=("dag_edge_rec", "mean"),
        fact_precision=("fact_prec", "mean"),
        fact_recall=("fact_rec", "mean"),
        pred_nodes_mean=("dag_nodes_pred", "mean"),
        gold_nodes_mean=("dag_nodes_gold", "mean"),
    )
    .reset_index()
    .sort_values(["accuracy", "edge_f1"], ascending=False)
)

# Metrics split by correct/incorrect
split = (
    df.groupby(["model_tag", "correct"])
    .agg(
        n=("qid", "count"),
        node_f1=("dag_node_f1", "mean"),
        edge_f1=("dag_edge_f1", "mean"),
        fact_f1=("fact_f1", "mean"),
        node_precision=("dag_node_prec", "mean"),
        node_recall=("dag_node_rec", "mean"),
        edge_precision=("dag_edge_prec", "mean"),
        edge_recall=("dag_edge_rec", "mean"),
        fact_precision=("fact_prec", "mean"),
        fact_recall=("fact_rec", "mean"),
    )
    .reset_index()
    .sort_values(["model_tag", "correct"])
)

# Error counts per model, incorrect cases only
err_df = df[df["correct"] == False].copy()

error_counts = (
    err_df.groupby(["model_tag", "error_kind"])
    .size()
    .reset_index(name="n")
    .sort_values(["model_tag", "n"], ascending=[True, False])
)

error_rates = error_counts.merge(
    err_df.groupby("model_tag").size().reset_index(name="incorrect_n"),
    on="model_tag",
)
error_rates["rate_among_incorrect"] = error_rates["n"] / error_rates["incorrect_n"]

error_wide_counts = (
    error_counts.pivot(index="model_tag", columns="error_kind", values="n")
    .fillna(0)
    .astype(int)
    .reset_index()
)

error_wide_rates = (
    error_rates.pivot(
        index="model_tag", columns="error_kind", values="rate_among_incorrect"
    )
    .fillna(0)
    .reset_index()
)

# Optional: family-level breakdown
family_summary = (
    df.groupby(["model_tag", "family"])
    .agg(
        n=("qid", "count"),
        accuracy=("correct", "mean"),
        exec_fail_rate=("exec_error_code", lambda s: s.notna().mean()),
        node_f1=("dag_node_f1", "mean"),
        edge_f1=("dag_edge_f1", "mean"),
        fact_f1=("fact_f1", "mean"),
    )
    .reset_index()
    .sort_values(["model_tag", "family"])
)

outdir = Path("outputs/analysis")
outdir.mkdir(parents=True, exist_ok=True)

summary.to_csv(outdir / "model_summary.csv", index=False)
split.to_csv(outdir / "model_metrics_by_correctness.csv", index=False)
error_counts.to_csv(outdir / "error_counts_by_model.csv", index=False)
error_rates.to_csv(outdir / "error_rates_by_model.csv", index=False)
error_wide_counts.to_csv(outdir / "error_counts_by_model_wide.csv", index=False)
error_wide_rates.to_csv(outdir / "error_rates_by_model_wide.csv", index=False)
family_summary.to_csv(outdir / "model_family_summary.csv", index=False)

print("\n=== Model summary ===")
print(summary.to_string(index=False))

print("\n=== Error counts by model ===")
print(error_wide_counts.to_string(index=False))

print("\nSaved CSVs to:", outdir)
