# Framework

TRACE has three main stages.

## Dataset Construction

Benchmark authors define source snippets, extracted fact records, schemas, actions, constraints, and query templates. TRACE samples valid bindings, lowers templates into executable gold DAGs, and writes capsules.

## Planning and Execution

In `full` mode, a provider backend prompts a model to return a DAG plan using the benchmark's allowed action set. TRACE validates the plan and executes it node by node.

In `oracle` mode, TRACE executes the gold DAG directly. This is useful for validating corpus generation and executor semantics without model calls.

## Evaluation

TRACE evaluates final answer correctness, fact grounding, graph structure, and execution failures. Results are written as per-capsule JSONL plus aggregate summary files.
