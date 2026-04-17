# =============================================================================
# TRACE / TRACE-UFR Refactor Harness
# =============================================================================

.PHONY: \
	smoke3_offline smoke3_gpt generate_TRACE-UFR run_TRACE-UFR_all_models \
	legacy_smoke3_offline legacy_generate_TRACE-UFR legacy_run_TRACE-UFR_offline \
	compare_smoke3_offline compare_TRACE-UFR_corpus compare_TRACE-UFR_offline

ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
SRC_DIR := $(ROOT)/src
LEGACY_DIR := $(ROOT)/legacy

ARTIFACTS_DIR := $(ROOT)/artifacts
LEGACY_ARTIFACTS_DIR := $(ARTIFACTS_DIR)/legacy
REFACTOR_ARTIFACTS_DIR := $(ARTIFACTS_DIR)/refactor

BENCHMARK_ID ?= trace_ufr
BENCHMARK_DIR := $(ROOT)/benchmarks/$(BENCHMARK_ID)
BENCHMARK_EXTRACTS := $(BENCHMARK_DIR)/extracts
BENCHMARK_SNIPPETS := $(BENCHMARK_DIR)/snippets
BENCHMARK_SCHEMA := $(BENCHMARK_DIR)/schemas/model_fact.json

LEGACY_EXTRACTS := $(ROOT)/data/extracts
LEGACY_SNIPPETS := $(ROOT)/data/snippets
LEGACY_SCHEMA := $(ROOT)/schemas/model_fact.json

export PYTHONPATH := $(SRC_DIR):$(PYTHONPATH)

PY ?= python
LEGACY_PYTHONPATH := $(LEGACY_DIR):$(SRC_DIR)

RUN_SWEEP_MOD ?= TRACE.cli.run_sweep
COMPARE_MOD ?= TRACE.cli.compare_parity

LEGACY_GEN_MOD ?= TRACE.generation.cli_generate_corpus
LEGACY_RUN_SWEEP_MOD ?= TRACE.execute.cli_run_sweep

SMOKE3_CORPUS_ID ?= smoke3
SMOKE3_N_TOTAL ?= 3
SMOKE3_SEED ?= 0
SMOKE3_D_ALL ?= 0
SMOKE3_MAX_COMPILE ?= 100
SMOKE3_GPT_MODEL ?= gpt-5.2
SMOKE3_GPT_MODE ?= full

TRACE_UFR_CORPUS_ID ?= TRACE-UFR
TRACE_UFR_N_TOTAL ?= 500
TRACE_UFR_SEED ?= 0
TRACE_UFR_D_ALL ?= 0 1 3 5 10
TRACE_UFR_MAX_COMPILE ?= 100
TRACE_UFR_MODES ?= full
TRACE_UFR_MAX_JOBS ?= 6
TRACE_UFR_RUN_EXTRA ?= --resume

OPENAI_MODELS ?= gpt-5.2 gpt-5-mini gpt-5-nano
ANTHROPIC_MODELS ?= claude-opus-4-6 claude-sonnet-4-5 claude-haiku-4-5
GEMINI_MODELS ?= gemini-2.5-pro gemini-3-flash-preview gemini-2.5-flash

LEGACY_SMOKE3_CORPUS_DIR := $(LEGACY_ARTIFACTS_DIR)/corpora/$(SMOKE3_CORPUS_ID)
LEGACY_SMOKE3_RUN_DIR := $(LEGACY_ARTIFACTS_DIR)/runs/run_smoke3_offline
LEGACY_TRACE_UFR_CORPUS_DIR := $(LEGACY_ARTIFACTS_DIR)/corpora/$(TRACE_UFR_CORPUS_ID)
LEGACY_TRACE_UFR_RUN_DIR := $(LEGACY_ARTIFACTS_DIR)/runs/run_TRACE-UFR_offline

REFACTOR_SMOKE3_CORPUS_DIR := $(REFACTOR_ARTIFACTS_DIR)/corpora/$(SMOKE3_CORPUS_ID)
REFACTOR_SMOKE3_RUN_DIR := $(REFACTOR_ARTIFACTS_DIR)/runs/run_smoke3_offline
REFACTOR_SMOKE3_GPT_RUN_DIR := $(REFACTOR_ARTIFACTS_DIR)/runs/run_smoke3_gpt
REFACTOR_TRACE_UFR_CORPUS_DIR := $(REFACTOR_ARTIFACTS_DIR)/corpora/$(TRACE_UFR_CORPUS_ID)
REFACTOR_TRACE_UFR_OPENAI_RUN_DIR := $(REFACTOR_ARTIFACTS_DIR)/runs/run_TRACE-UFR_openai
REFACTOR_TRACE_UFR_ANTHROPIC_RUN_DIR := $(REFACTOR_ARTIFACTS_DIR)/runs/run_TRACE-UFR_anthropic
REFACTOR_TRACE_UFR_GEMINI_RUN_DIR := $(REFACTOR_ARTIFACTS_DIR)/runs/run_TRACE-UFR_gemini
REFACTOR_CACHE_DIR := $(REFACTOR_ARTIFACTS_DIR)/cache
REFACTOR_CACHE_BASE := $(REFACTOR_CACHE_DIR)/lookups.json

legacy_generate_smoke3:
	@if [ -d "$(LEGACY_SMOKE3_CORPUS_DIR)/d=0" ] || [ -f "$(LEGACY_SMOKE3_CORPUS_DIR)/meta.json" ]; then \
		echo "Legacy corpus already exists at $(LEGACY_SMOKE3_CORPUS_DIR) (skipping generation)"; \
	else \
		mkdir -p "$(LEGACY_SMOKE3_CORPUS_DIR)"; \
		PYTHONPATH="$(LEGACY_PYTHONPATH)" $(PY) -m $(LEGACY_GEN_MOD) \
			--extracts "$(LEGACY_EXTRACTS)" \
			--snippets "$(LEGACY_SNIPPETS)" \
			--out "$(LEGACY_SMOKE3_CORPUS_DIR)" \
			--distractors $(SMOKE3_D_ALL) \
			--n-total $(SMOKE3_N_TOTAL) \
			--seed $(SMOKE3_SEED) \
			--max-compile-attempts $(SMOKE3_MAX_COMPILE); \
	fi

legacy_smoke3_offline: legacy_generate_smoke3
	@rm -rf "$(LEGACY_SMOKE3_RUN_DIR)"
	@mkdir -p "$(LEGACY_SMOKE3_RUN_DIR)"
	PYTHONPATH="$(LEGACY_PYTHONPATH)" $(PY) -m $(LEGACY_RUN_SWEEP_MOD) \
		--corpus-dir "$(LEGACY_SMOKE3_CORPUS_DIR)" \
		--out-dir "$(LEGACY_SMOKE3_RUN_DIR)" \
		--modes oracle \
		--extracts "$(LEGACY_EXTRACTS)" \
		--max-jobs 1

legacy_generate_TRACE-UFR:
	@if [ -d "$(LEGACY_TRACE_UFR_CORPUS_DIR)/d=0" ] || [ -f "$(LEGACY_TRACE_UFR_CORPUS_DIR)/meta.json" ]; then \
		echo "Legacy corpus already exists at $(LEGACY_TRACE_UFR_CORPUS_DIR) (skipping generation)"; \
	else \
		mkdir -p "$(LEGACY_TRACE_UFR_CORPUS_DIR)"; \
		PYTHONPATH="$(LEGACY_PYTHONPATH)" $(PY) -m $(LEGACY_GEN_MOD) \
			--extracts "$(LEGACY_EXTRACTS)" \
			--snippets "$(LEGACY_SNIPPETS)" \
			--out "$(LEGACY_TRACE_UFR_CORPUS_DIR)" \
			--distractors $(TRACE_UFR_D_ALL) \
			--n-total $(TRACE_UFR_N_TOTAL) \
			--seed $(TRACE_UFR_SEED) \
			--max-compile-attempts $(TRACE_UFR_MAX_COMPILE); \
	fi

legacy_run_TRACE-UFR_offline: legacy_generate_TRACE-UFR
	@rm -rf "$(LEGACY_TRACE_UFR_RUN_DIR)"
	@mkdir -p "$(LEGACY_TRACE_UFR_RUN_DIR)"
	PYTHONPATH="$(LEGACY_PYTHONPATH)" $(PY) -m $(LEGACY_RUN_SWEEP_MOD) \
		--corpus-dir "$(LEGACY_TRACE_UFR_CORPUS_DIR)" \
		--out-dir "$(LEGACY_TRACE_UFR_RUN_DIR)" \
		--modes oracle \
		--extracts "$(LEGACY_EXTRACTS)" \
		--max-jobs 1

generate_smoke3:
	@if [ -d "$(REFACTOR_SMOKE3_CORPUS_DIR)/d=0" ] || [ -f "$(REFACTOR_SMOKE3_CORPUS_DIR)/meta.json" ]; then \
		echo "Refactor corpus already exists at $(REFACTOR_SMOKE3_CORPUS_DIR) (skipping generation)"; \
	else \
		mkdir -p "$(REFACTOR_SMOKE3_CORPUS_DIR)"; \
		$(PY) -m TRACE.generation.cli_generate_corpus \
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
	@mkdir -p "$(REFACTOR_SMOKE3_GPT_RUN_DIR)" "$(REFACTOR_CACHE_DIR)"
	$(PY) -m $(RUN_SWEEP_MOD) \
		--benchmark $(BENCHMARK_ID) \
		--corpus-dir "$(REFACTOR_SMOKE3_CORPUS_DIR)" \
		--out-dir "$(REFACTOR_SMOKE3_GPT_RUN_DIR)" \
		--modes $(SMOKE3_GPT_MODE) \
		--provider openai \
		--models $(SMOKE3_GPT_MODEL) \
		--schema "$(BENCHMARK_SCHEMA)" \
		--cache "$(REFACTOR_CACHE_BASE)" \
		--max-jobs 1 \
		--resume

generate_TRACE-UFR:
	@if [ -d "$(REFACTOR_TRACE_UFR_CORPUS_DIR)/d=0" ] || [ -f "$(REFACTOR_TRACE_UFR_CORPUS_DIR)/meta.json" ]; then \
		echo "Refactor corpus already exists at $(REFACTOR_TRACE_UFR_CORPUS_DIR) (skipping generation)"; \
	else \
		mkdir -p "$(REFACTOR_TRACE_UFR_CORPUS_DIR)"; \
		$(PY) -m TRACE.generation.cli_generate_corpus \
			--benchmark $(BENCHMARK_ID) \
			--out "$(REFACTOR_TRACE_UFR_CORPUS_DIR)" \
			--distractors $(TRACE_UFR_D_ALL) \
			--n-total $(TRACE_UFR_N_TOTAL) \
			--seed $(TRACE_UFR_SEED) \
			--max-compile-attempts $(TRACE_UFR_MAX_COMPILE); \
	fi

run_TRACE-UFR_all_models: generate_TRACE-UFR
	@mkdir -p "$(REFACTOR_TRACE_UFR_OPENAI_RUN_DIR)" "$(REFACTOR_TRACE_UFR_ANTHROPIC_RUN_DIR)" "$(REFACTOR_TRACE_UFR_GEMINI_RUN_DIR)" "$(REFACTOR_CACHE_DIR)"
	$(PY) -m $(RUN_SWEEP_MOD) \
		--benchmark $(BENCHMARK_ID) \
		--corpus-dir "$(REFACTOR_TRACE_UFR_CORPUS_DIR)" \
		--out-dir "$(REFACTOR_TRACE_UFR_OPENAI_RUN_DIR)" \
		--modes $(TRACE_UFR_MODES) \
		--provider openai \
		--models $(OPENAI_MODELS) \
		--schema "$(BENCHMARK_SCHEMA)" \
		--cache "$(REFACTOR_CACHE_BASE)" \
		--max-jobs $(TRACE_UFR_MAX_JOBS) \
		$(TRACE_UFR_RUN_EXTRA)
	$(PY) -m $(RUN_SWEEP_MOD) \
		--benchmark $(BENCHMARK_ID) \
		--corpus-dir "$(REFACTOR_TRACE_UFR_CORPUS_DIR)" \
		--out-dir "$(REFACTOR_TRACE_UFR_ANTHROPIC_RUN_DIR)" \
		--modes $(TRACE_UFR_MODES) \
		--provider anthropic \
		--models $(ANTHROPIC_MODELS) \
		--schema "$(BENCHMARK_SCHEMA)" \
		--cache "$(REFACTOR_CACHE_BASE)" \
		--max-jobs $(TRACE_UFR_MAX_JOBS) \
		$(TRACE_UFR_RUN_EXTRA)
	$(PY) -m $(RUN_SWEEP_MOD) \
		--benchmark $(BENCHMARK_ID) \
		--corpus-dir "$(REFACTOR_TRACE_UFR_CORPUS_DIR)" \
		--out-dir "$(REFACTOR_TRACE_UFR_GEMINI_RUN_DIR)" \
		--modes $(TRACE_UFR_MODES) \
		--provider gemini \
		--models $(GEMINI_MODELS) \
		--schema "$(BENCHMARK_SCHEMA)" \
		--cache "$(REFACTOR_CACHE_BASE)" \
		--max-jobs $(TRACE_UFR_MAX_JOBS) \
		$(TRACE_UFR_RUN_EXTRA)

compare_smoke3_offline: legacy_smoke3_offline smoke3_offline
	$(PY) -m $(COMPARE_MOD) \
		--kind corpus \
		--legacy "$(LEGACY_SMOKE3_CORPUS_DIR)" \
		--refactor "$(REFACTOR_SMOKE3_CORPUS_DIR)"
	$(PY) -m $(COMPARE_MOD) \
		--kind jsonl \
		--legacy "$(LEGACY_SMOKE3_RUN_DIR)/results_all.jsonl" \
		--refactor "$(REFACTOR_SMOKE3_RUN_DIR)/results_all.jsonl"

compare_TRACE-UFR_corpus: legacy_generate_TRACE-UFR generate_TRACE-UFR
	$(PY) -m $(COMPARE_MOD) \
		--kind corpus \
		--legacy "$(LEGACY_TRACE_UFR_CORPUS_DIR)" \
		--refactor "$(REFACTOR_TRACE_UFR_CORPUS_DIR)"

compare_TRACE-UFR_offline: legacy_run_TRACE-UFR_offline generate_TRACE-UFR
	@rm -rf "$(REFACTOR_ARTIFACTS_DIR)/runs/run_TRACE-UFR_offline"
	@mkdir -p "$(REFACTOR_ARTIFACTS_DIR)/runs/run_TRACE-UFR_offline"
	$(PY) -m $(RUN_SWEEP_MOD) \
		--benchmark $(BENCHMARK_ID) \
		--corpus-dir "$(REFACTOR_TRACE_UFR_CORPUS_DIR)" \
		--out-dir "$(REFACTOR_ARTIFACTS_DIR)/runs/run_TRACE-UFR_offline" \
		--modes oracle \
		--max-jobs 1
	$(PY) -m $(COMPARE_MOD) \
		--kind jsonl \
		--legacy "$(LEGACY_TRACE_UFR_RUN_DIR)/results_all.jsonl" \
		--refactor "$(REFACTOR_ARTIFACTS_DIR)/runs/run_TRACE-UFR_offline/results_all.jsonl"
