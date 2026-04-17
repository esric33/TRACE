# TRACE Architecture Backlog

This document records the agreed architectural direction for the TRACE refactor.
It separates immediate refactor work from lower-priority follow-up work so that
design decisions do not get lost while implementation is in flight.

## Current Direction

- `src/TRACE/` should become the benchmark-agnostic engine.
- `benchmarks/<benchmark_name>/` should contain self-contained benchmark
  packages with assets, templates, actions, schemas, and benchmark-specific
  logic.
- `artifacts/` should be the canonical output root for corpora, runs, caches,
  and parity outputs.
- The template/spec DSL should stay shared across benchmarks.
- Record semantics, benchmark constraints, retrieval guidance, and
  benchmark-specific tooling should be benchmark-owned.

## Immediate Priority

### 1. Make the sampler benchmark-agnostic

Goal:
- Replace TRACE-UFR-specific sampler assumptions with a slot-based model.

Scope:
- Remove direct dependence on fields such as `company`, `metric_key`, and
  `metric_role` from core sampling logic.
- Replace benchmark-specific constraints like `SameCompany` with generic
  constraints such as `Same(slot)` and `Different(slot)`.
- Add a benchmark hook for custom constraint evaluation where generic slot
  equality is not enough.
- Introduce benchmark-owned record loading and slot derivation.

Why now:
- This is the largest remaining place where TRACE-UFR semantics still leak into
  core.
- A second benchmark will be hard to onboard cleanly until this is fixed.

### 2. Expand `BenchmarkDef` into a real behavior boundary

Goal:
- Make the benchmark manifest own benchmark behavior, not just benchmark paths.

Scope:
- Add hooks for record loading/normalization.
- Add hooks for prompt guidance inserts.
- Add hooks for benchmark-specific constraints.
- Add optional hooks for maintenance tooling and validation.

Why now:
- Without this, benchmark logic will keep leaking back into `src/TRACE/`.

### 3. Move prompt semantics behind benchmark-owned inserts

Goal:
- Keep the shared DAG/JSON scaffold in core, but move benchmark-specific
  retrieval and operator guidance into benchmark-owned prompt logic.

Scope:
- Keep output format rules and shared operator schema in core.
- Move lookup query instructions and benchmark-specific examples into benchmark
  hooks.
- Avoid hard-coding TRACE-UFR assumptions into shared prompt builders.

Why now:
- Prompt semantics are part of benchmark behavior, not core reasoning runtime.

### 4. Unify the public CLI surface

Goal:
- Provide a single benchmark-aware entrypoint under `TRACE.cli`.

Scope:
- Consolidate public commands around a single interface such as:
  - `TRACE.cli generate`
  - `TRACE.cli run`
  - `TRACE.cli run_sweep`
  - `TRACE.cli compare`
- Keep compatibility shims temporarily if needed.

Why now:
- The current split across `generation.*` and `execute.*` exposes historical
  internals rather than the intended architecture.

### 5. Make benchmarks more self-contained

Goal:
- Ensure a benchmark package owns the code and assets required to maintain it.

Scope:
- Keep benchmark-specific assets under `benchmarks/<id>/`.
- Move benchmark-specific prep/maintenance logic under benchmark ownership, or
  at minimum make that ownership explicit through the benchmark manifest.
- Treat `documents/` as a default optional benchmark asset directory.

Why now:
- This is required for clean onboarding of future benchmarks.

### 6. Separate lowering from corpus assembly/oracle gold orchestration

Goal:
- Keep the compiler focused on lowering, while preserving executor-driven gold
  generation.

Scope:
- Move snippet hydration and capsule-context assembly out of the lowering
  module.
- Keep final gold answers derived by executor/oracle semantics, but orchestrate
  that at the corpus-generation layer rather than inside lowering.

Why now:
- This improves architecture without changing the key parity guarantee.

## Deferred / Lower Priority

### 7. Clean up the repo root once the refactor stabilizes

Scope:
- Move or remove legacy top-level paths such as `data/`, `schemas/`, and
  `outputs/`.
- Keep the README aligned with the eventually canonical structure, not the
  transitional state.

Reason for deferral:
- The architecture is still moving. Cleaning this up now creates churn without
  much design value.

### 8. Remove the legacy shadow after parity confidence is high

Scope:
- Drop `legacy/TRACE/` after smoke and targeted parity checks are consistently
  passing and the refactor is stable enough for public use.

Reason for deferral:
- The legacy shadow is still useful as a safety net while the architecture is
  changing.

### 9. Revisit stronger abstractions once a second benchmark is implemented

Scope:
- Reassess what defaults belong in core vs benchmark packages after the next
  benchmark exposes real friction.
- Decide whether more advanced generic constraint/existence systems are worth
  adding beyond the first slot-based pass.
- Reassess whether benchmark-local tools should physically move under each
  benchmark package once the operational workflow settles.

Reason for deferral:
- The second benchmark will expose real edge cases better than speculation.

## Working Assumptions

- The core template/spec DSL remains shared.
- Benchmarks may define their own record structures and derived slots.
- Similar concepts across benchmarks should be expressed generically where
  possible, e.g. `Same("slot_name")` rather than hard-coded semantic classes.
- Benchmarks should be easy to author with sensible defaults, even if advanced
  hooks exist for more complex cases.

## Tracking Notes

- This document is the durable in-repo record of architecture work that is
  agreed but not yet implemented.
- GitHub issues should mirror the sections above, with high priority for the
  immediate work and lower priority for the deferred items.
