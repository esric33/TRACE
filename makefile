# =============================================================================
# TRACE / TRACE-UFR Refactor Harness
# =============================================================================

.PHONY: \
	test smoke3_offline smoke3_gpt trace-ufr generate_TRACE-UFR run_TRACE-UFR_all_models \
	main_run experiment_run

ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
SRC_DIR := $(ROOT)/src

ARTIFACTS_DIR := $(ROOT)/artifacts
REFACTOR_ARTIFACTS_DIR := $(ARTIFACTS_DIR)/refactor

BENCHMARK_ID ?= trace_ufr
BENCHMARK_DIR := $(ROOT)/benchmarks/$(BENCHMARK_ID)
BENCHMARK_EXTRACTS := $(BENCHMARK_DIR)/extracts
BENCHMARK_SNIPPETS := $(BENCHMARK_DIR)/snippets

export PYTHONPATH := $(SRC_DIR):$(PYTHONPATH)

PY ?= python

RUN_SWEEP_MOD ?= TRACE.cli.run_sweep
GENERATE_MOD ?= TRACE.cli.generate

SMOKE3_CORPUS_ID ?= smoke3
SMOKE3_N_TOTAL ?= 3
SMOKE3_SEED ?= 0
SMOKE3_D_ALL ?= 0
SMOKE3_MAX_COMPILE ?= 100
SMOKE3_GPT_MODEL ?= gpt-5.2
SMOKE3_GPT_MODE ?= full

TRACE_UFR_CORPUS_ID ?= TRACE-UFR
TRACE_UFR_N_TOTAL ?= 600
TRACE_UFR_SEED ?= 0
TRACE_UFR_D_ALL ?= 0 1 3 5 10
TRACE_UFR_MAX_COMPILE ?= 100
TRACE_UFR_MODES ?= full
TRACE_UFR_MAX_JOBS ?= 6
TRACE_UFR_RUN_EXTRA ?= --resume
BENCHMARK_PROFILE_SCHEMA_VERSION ?= 2

OPENAI_MODELS ?= gpt-5.2 gpt-5-mini gpt-5-nano
ANTHROPIC_MODELS ?= claude-opus-4-6 claude-sonnet-4-5 claude-haiku-4-5
GEMINI_MODELS ?= gemini-2.5-pro gemini-3-flash-preview gemini-2.5-flash

MAIN_RUN_ID ?= $(BENCHMARK_ID)-n600-d3-seed0-openai-gpt-5.2
MAIN_RUN_N_TOTAL ?= 600
MAIN_RUN_SEED ?= 0
MAIN_RUN_D_ALL ?= 3
MAIN_RUN_PROVIDER ?= openai
MAIN_RUN_MODELS ?= gpt-5.2
MAIN_RUN_MODES ?= full
MAIN_RUN_MAX_JOBS ?= 1
MAIN_RUN_INTERMEDIATE_ROOT ?= $(ROOT)/outputs/intermediate
MAIN_RUN_FINAL_ROOT ?= $(ROOT)/outputs/final-results

EXPERIMENT_ID ?= trace-both-n600-d3-seed0-nine-models
EXPERIMENT_ROOT ?= $(ROOT)/outputs/experiments
EXPERIMENT_BENCHMARKS ?= trace_ufr trace_dir
EXPERIMENT_N_TOTAL ?= 600
EXPERIMENT_SEED ?= 0
EXPERIMENT_D_ALL ?= 3
EXPERIMENT_PROVIDERS ?= openai anthropic gemini
EXPERIMENT_MAX_JOBS ?= 6
EXPERIMENT_MAX_SWEEPS ?= 3
EXPERIMENT_RUN_EXTRA ?=

REFACTOR_SMOKE3_CORPUS_DIR := $(REFACTOR_ARTIFACTS_DIR)/corpora/$(SMOKE3_CORPUS_ID)
REFACTOR_SMOKE3_RUN_DIR := $(REFACTOR_ARTIFACTS_DIR)/runs/run_smoke3_offline
REFACTOR_SMOKE3_GPT_RUN_DIR := $(REFACTOR_ARTIFACTS_DIR)/runs/run_smoke3_gpt
REFACTOR_TRACE_UFR_CORPUS_DIR := $(REFACTOR_ARTIFACTS_DIR)/corpora/$(TRACE_UFR_CORPUS_ID)
REFACTOR_TRACE_UFR_OPENAI_RUN_DIR := $(REFACTOR_ARTIFACTS_DIR)/runs/run_TRACE-UFR_openai
REFACTOR_TRACE_UFR_ANTHROPIC_RUN_DIR := $(REFACTOR_ARTIFACTS_DIR)/runs/run_TRACE-UFR_anthropic
REFACTOR_TRACE_UFR_GEMINI_RUN_DIR := $(REFACTOR_ARTIFACTS_DIR)/runs/run_TRACE-UFR_gemini
test:
	$(PY) -m unittest discover -s tests -v

generate_smoke3:
	@if [ -d "$(REFACTOR_SMOKE3_CORPUS_DIR)/d=0" ] || [ -f "$(REFACTOR_SMOKE3_CORPUS_DIR)/meta.json" ]; then \
		echo "Refactor corpus already exists at $(REFACTOR_SMOKE3_CORPUS_DIR) (skipping generation)"; \
	else \
		mkdir -p "$(REFACTOR_SMOKE3_CORPUS_DIR)"; \
		$(PY) -m $(GENERATE_MOD) \
			--benchmark $(BENCHMARK_ID) \
			--out "$(REFACTOR_SMOKE3_CORPUS_DIR)" \
			--distractors $(SMOKE3_D_ALL) \
			--n-total $(SMOKE3_N_TOTAL) \
			--seed $(SMOKE3_SEED) \
			--max-compile-attempts $(SMOKE3_MAX_COMPILE); \
	fi

smoke3_offline: generate_smoke3
	@rm -rf "$(REFACTOR_SMOKE3_RUN_DIR)"
	@mkdir -p "$(REFACTOR_SMOKE3_RUN_DIR)"
	$(PY) -m $(RUN_SWEEP_MOD) \
		--benchmark $(BENCHMARK_ID) \
		--corpus-dir "$(REFACTOR_SMOKE3_CORPUS_DIR)" \
		--out-dir "$(REFACTOR_SMOKE3_RUN_DIR)" \
		--modes oracle \
		--max-jobs 1

smoke3_gpt: generate_smoke3
	@mkdir -p "$(REFACTOR_SMOKE3_GPT_RUN_DIR)"
	$(PY) -m $(RUN_SWEEP_MOD) \
		--benchmark $(BENCHMARK_ID) \
		--corpus-dir "$(REFACTOR_SMOKE3_CORPUS_DIR)" \
		--out-dir "$(REFACTOR_SMOKE3_GPT_RUN_DIR)" \
		--modes $(SMOKE3_GPT_MODE) \
		--provider openai \
		--models $(SMOKE3_GPT_MODEL) \
		--max-jobs 1 \
		--resume

trace-ufr: generate_TRACE-UFR
	@test -f "$(REFACTOR_TRACE_UFR_CORPUS_DIR)/benchmark_profile.md"
	@test -f "$(REFACTOR_TRACE_UFR_CORPUS_DIR)/benchmark_profile.json"
	@echo "TRACE-UFR corpus: $(REFACTOR_TRACE_UFR_CORPUS_DIR)"
	@echo "Benchmark profile report: $(REFACTOR_TRACE_UFR_CORPUS_DIR)/benchmark_profile.md"
	@echo "Benchmark profile data: $(REFACTOR_TRACE_UFR_CORPUS_DIR)/benchmark_profile.json"

generate_TRACE-UFR:
	@if [ -d "$(REFACTOR_TRACE_UFR_CORPUS_DIR)/d=0" ] && [ -f "$(REFACTOR_TRACE_UFR_CORPUS_DIR)/meta.json" ] && [ -f "$(REFACTOR_TRACE_UFR_CORPUS_DIR)/benchmark_profile.md" ] && [ -f "$(REFACTOR_TRACE_UFR_CORPUS_DIR)/benchmark_profile.json" ] && grep -q '"schema_version": $(BENCHMARK_PROFILE_SCHEMA_VERSION)' "$(REFACTOR_TRACE_UFR_CORPUS_DIR)/benchmark_profile.json" && $(PY) -c 'import json; from importlib import import_module; meta=json.load(open("$(REFACTOR_TRACE_UFR_CORPUS_DIR)/meta.json", encoding="utf-8")); prof=json.load(open("$(REFACTOR_TRACE_UFR_CORPUS_DIR)/benchmark_profile.json", encoding="utf-8")); r=import_module("benchmarks.trace_ufr.templates.registry"); expected=$(TRACE_UFR_N_TOTAL) * $(words $(TRACE_UFR_D_ALL)); ok=prof.get("total_templates") == len(r.ALL_SPECS) and prof.get("total_queries") == expected and meta.get("n_total_per_distractor") == $(TRACE_UFR_N_TOTAL) and meta.get("template_balanced") is True; raise SystemExit(0 if ok else 1)'; then \
		echo "Refactor corpus and benchmark profile already exist at $(REFACTOR_TRACE_UFR_CORPUS_DIR) (skipping generation)"; \
	else \
		mkdir -p "$(REFACTOR_TRACE_UFR_CORPUS_DIR)"; \
		$(PY) -m $(GENERATE_MOD) \
			--benchmark $(BENCHMARK_ID) \
			--out "$(REFACTOR_TRACE_UFR_CORPUS_DIR)" \
			--distractors $(TRACE_UFR_D_ALL) \
			--n-total $(TRACE_UFR_N_TOTAL) \
			--seed $(TRACE_UFR_SEED) \
			--balance-templates \
			--max-compile-attempts $(TRACE_UFR_MAX_COMPILE) \
			--force; \
	fi

main_run:
	$(PY) scripts/main_run.py \
		--benchmark $(BENCHMARK_ID) \
		--run-id "$(MAIN_RUN_ID)" \
		--n-total $(MAIN_RUN_N_TOTAL) \
		--seed $(MAIN_RUN_SEED) \
		--distractors $(MAIN_RUN_D_ALL) \
		--provider $(MAIN_RUN_PROVIDER) \
		--models $(MAIN_RUN_MODELS) \
		--modes $(MAIN_RUN_MODES) \
		--max-jobs $(MAIN_RUN_MAX_JOBS) \
		--max-compile-attempts $(TRACE_UFR_MAX_COMPILE) \
		--intermediate-root "$(MAIN_RUN_INTERMEDIATE_ROOT)" \
		--final-root "$(MAIN_RUN_FINAL_ROOT)"

experiment_run:
	$(PY) scripts/experiment_run.py \
		--experiment-id "$(EXPERIMENT_ID)" \
		--root "$(EXPERIMENT_ROOT)" \
		--benchmarks $(EXPERIMENT_BENCHMARKS) \
		--n-total $(EXPERIMENT_N_TOTAL) \
		--seed $(EXPERIMENT_SEED) \
		--distractors $(EXPERIMENT_D_ALL) \
		--providers $(EXPERIMENT_PROVIDERS) \
		--openai-models $(OPENAI_MODELS) \
		--anthropic-models $(ANTHROPIC_MODELS) \
		--gemini-models $(GEMINI_MODELS) \
		--max-jobs $(EXPERIMENT_MAX_JOBS) \
		--max-sweeps $(EXPERIMENT_MAX_SWEEPS) \
		--max-compile-attempts $(TRACE_UFR_MAX_COMPILE) \
		$(EXPERIMENT_RUN_EXTRA)

run_TRACE-UFR_all_models: generate_TRACE-UFR
	@mkdir -p "$(REFACTOR_TRACE_UFR_OPENAI_RUN_DIR)" "$(REFACTOR_TRACE_UFR_ANTHROPIC_RUN_DIR)" "$(REFACTOR_TRACE_UFR_GEMINI_RUN_DIR)"
	$(PY) -m $(RUN_SWEEP_MOD) \
		--benchmark $(BENCHMARK_ID) \
		--corpus-dir "$(REFACTOR_TRACE_UFR_CORPUS_DIR)" \
		--out-dir "$(REFACTOR_TRACE_UFR_OPENAI_RUN_DIR)" \
		--modes $(TRACE_UFR_MODES) \
		--provider openai \
		--models $(OPENAI_MODELS) \
		--max-jobs $(TRACE_UFR_MAX_JOBS) \
		$(TRACE_UFR_RUN_EXTRA)
	$(PY) -m $(RUN_SWEEP_MOD) \
		--benchmark $(BENCHMARK_ID) \
		--corpus-dir "$(REFACTOR_TRACE_UFR_CORPUS_DIR)" \
		--out-dir "$(REFACTOR_TRACE_UFR_ANTHROPIC_RUN_DIR)" \
		--modes $(TRACE_UFR_MODES) \
		--provider anthropic \
		--models $(ANTHROPIC_MODELS) \
		--max-jobs $(TRACE_UFR_MAX_JOBS) \
		$(TRACE_UFR_RUN_EXTRA)
	$(PY) -m $(RUN_SWEEP_MOD) \
		--benchmark $(BENCHMARK_ID) \
		--corpus-dir "$(REFACTOR_TRACE_UFR_CORPUS_DIR)" \
		--out-dir "$(REFACTOR_TRACE_UFR_GEMINI_RUN_DIR)" \
		--modes $(TRACE_UFR_MODES) \
		--provider gemini \
		--models $(GEMINI_MODELS) \
		--max-jobs $(TRACE_UFR_MAX_JOBS) \
		$(TRACE_UFR_RUN_EXTRA)
