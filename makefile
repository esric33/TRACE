# =============================================================================
# TRACE Experiments
# =============================================================================

.PHONY: smoke3_offline smoke3_gpt generate_smoke3 generate_TRACE-UFR run_TRACE-UFR_all_models

# --- Project defaults --------------------------------------------------------

ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
SRC_DIR := $(ROOT)/src
DATA_DIR := $(ROOT)/data

OUTPUTS_DIR := $(ROOT)/outputs
CORPORA_DIR := $(OUTPUTS_DIR)/corpora
RUNS_DIR := $(OUTPUTS_DIR)/runs
CACHE_DIR := $(RUNS_DIR)/cache

EXTRACTS_DIR ?= $(DATA_DIR)/extracts
SNIPPETS_DIR ?= $(DATA_DIR)/snippets
FACT_SCHEMA  ?= $(ROOT)/schemas/model_fact.json

export PYTHONPATH := $(SRC_DIR):$(PYTHONPATH)

PY             ?= python
GEN_CORPUS_MOD ?= TRACE.generation.cli_generate_corpus
RUN_SWEEP_MOD  ?= TRACE.execute.cli_run_sweep

# Shared cache base for retrieval/full runs.
CACHE_BASE ?= $(CACHE_DIR)/lookups.json

# --- TRACE-UFR corpus + run config ------------------------------------------

TRACE_UFR_CORPUS_ID   ?= TRACE-UFR
TRACE_UFR_CORPUS_DIR  ?= $(CORPORA_DIR)/$(TRACE_UFR_CORPUS_ID)
TRACE_UFR_N_TOTAL     ?= 500
TRACE_UFR_SEED        ?= 0
TRACE_UFR_MAX_COMPILE ?= 100
TRACE_UFR_D_ALL       ?= 0 1 3 5 10

TRACE_UFR_MODES     ?= full
TRACE_UFR_MAX_JOBS  ?= 6
TRACE_UFR_RUN_EXTRA ?= --resume

OPENAI_MODELS    ?= gpt-5.2 gpt-5-mini gpt-5-nano
ANTHROPIC_MODELS ?= claude-opus-4-6 claude-sonnet-4-5 claude-haiku-4-5
GEMINI_MODELS    ?= gemini-2.5-pro gemini-3-flash-preview gemini-2.5-flash

TRACE_UFR_OPENAI_RUN_ID    ?= run_TRACE-UFR_openai
TRACE_UFR_ANTHROPIC_RUN_ID ?= run_TRACE-UFR_anthropic
TRACE_UFR_GEMINI_RUN_ID    ?= run_TRACE-UFR_gemini

TRACE_UFR_OPENAI_RUN_DIR    ?= $(RUNS_DIR)/$(TRACE_UFR_OPENAI_RUN_ID)
TRACE_UFR_ANTHROPIC_RUN_DIR ?= $(RUNS_DIR)/$(TRACE_UFR_ANTHROPIC_RUN_ID)
TRACE_UFR_GEMINI_RUN_DIR    ?= $(RUNS_DIR)/$(TRACE_UFR_GEMINI_RUN_ID)

# --- Smoke config (small local sanity runs) ---------------------------------

SMOKE3_CORPUS_ID      ?= smoke3
SMOKE3_CORPUS_DIR     ?= $(CORPORA_DIR)/$(SMOKE3_CORPUS_ID)
SMOKE3_N_TOTAL        ?= 3
SMOKE3_SEED           ?= 0
SMOKE3_MAX_COMPILE    ?= 100
SMOKE3_D_ALL          ?= 0

SMOKE3_OFFLINE_RUN_ID ?= run_smoke3_offline
SMOKE3_GPT_RUN_ID     ?= run_smoke3_gpt
SMOKE3_OFFLINE_RUN_DIR ?= $(RUNS_DIR)/$(SMOKE3_OFFLINE_RUN_ID)
SMOKE3_GPT_RUN_DIR     ?= $(RUNS_DIR)/$(SMOKE3_GPT_RUN_ID)

SMOKE3_GPT_MODEL     ?= gpt-5.2
SMOKE3_GPT_MODE      ?= full
SMOKE3_RUN_EXTRA     ?= --resume

# --- Corpus generation -------------------------------------------------------

generate_smoke3:
	@if [ -d "$(SMOKE3_CORPUS_DIR)/d=0" ] || [ -f "$(SMOKE3_CORPUS_DIR)/meta.json" ]; then \
		echo "Corpus already exists at $(SMOKE3_CORPUS_DIR) (skipping generation)"; \
	else \
		mkdir -p "$(SMOKE3_CORPUS_DIR)"; \
		echo "Generating smoke corpus (N=$(SMOKE3_N_TOTAL) per d) -> $(SMOKE3_CORPUS_DIR)"; \
		$(PY) -m $(GEN_CORPUS_MOD) \
			--extracts "$(EXTRACTS_DIR)" \
			--snippets "$(SNIPPETS_DIR)" \
			--out "$(SMOKE3_CORPUS_DIR)" \
			--distractors $(SMOKE3_D_ALL) \
			--n-total $(SMOKE3_N_TOTAL) \
			--seed $(SMOKE3_SEED) \
			--max-compile-attempts $(SMOKE3_MAX_COMPILE); \
	fi

generate_TRACE-UFR:
	@if [ -d "$(TRACE_UFR_CORPUS_DIR)/d=0" ] || [ -f "$(TRACE_UFR_CORPUS_DIR)/meta.json" ]; then \
		echo "Corpus already exists at $(TRACE_UFR_CORPUS_DIR) (skipping generation)"; \
	else \
		mkdir -p "$(TRACE_UFR_CORPUS_DIR)"; \
		echo "Generating TRACE-UFR corpus (N=$(TRACE_UFR_N_TOTAL) per d) -> $(TRACE_UFR_CORPUS_DIR)"; \
		$(PY) -m $(GEN_CORPUS_MOD) \
			--extracts "$(EXTRACTS_DIR)" \
			--snippets "$(SNIPPETS_DIR)" \
			--out "$(TRACE_UFR_CORPUS_DIR)" \
			--distractors $(TRACE_UFR_D_ALL) \
			--n-total $(TRACE_UFR_N_TOTAL) \
			--seed $(TRACE_UFR_SEED) \
			--max-compile-attempts $(TRACE_UFR_MAX_COMPILE); \
		echo "$(TRACE_UFR_CORPUS_ID)" > "$(CORPORA_DIR)/latest_corpus.txt"; \
	fi

# --- Smoke runs --------------------------------------------------------------

smoke3_offline: generate_smoke3
	@mkdir -p "$(SMOKE3_OFFLINE_RUN_DIR)"
	@echo "Smoke offline run -> corpus=$(SMOKE3_CORPUS_ID) run=$(SMOKE3_OFFLINE_RUN_ID)"
	$(PY) -m $(RUN_SWEEP_MOD) \
		--corpus-dir "$(SMOKE3_CORPUS_DIR)" \
		--out-dir "$(SMOKE3_OFFLINE_RUN_DIR)" \
		--modes oracle \
		--extracts "$(EXTRACTS_DIR)" \
		--max-jobs 1 \
		$(SMOKE3_RUN_EXTRA)

smoke3_gpt: generate_smoke3
	@mkdir -p "$(SMOKE3_GPT_RUN_DIR)" "$(CACHE_DIR)"
	@echo "Smoke GPT run -> corpus=$(SMOKE3_CORPUS_ID) run=$(SMOKE3_GPT_RUN_ID) model=$(SMOKE3_GPT_MODEL) mode=$(SMOKE3_GPT_MODE)"
	$(PY) -m $(RUN_SWEEP_MOD) \
		--corpus-dir "$(SMOKE3_CORPUS_DIR)" \
		--out-dir "$(SMOKE3_GPT_RUN_DIR)" \
		--modes $(SMOKE3_GPT_MODE) \
		--provider openai \
		--models $(SMOKE3_GPT_MODEL) \
		--extracts "$(EXTRACTS_DIR)" \
		--schema "$(FACT_SCHEMA)" \
		--cache "$(CACHE_BASE)" \
		--max-jobs 1 \
		$(SMOKE3_RUN_EXTRA)

# --- Main TRACE-UFR run target ----------------------------------------------

run_TRACE-UFR_all_models: generate_TRACE-UFR
	@mkdir -p "$(TRACE_UFR_OPENAI_RUN_DIR)" "$(TRACE_UFR_ANTHROPIC_RUN_DIR)" "$(TRACE_UFR_GEMINI_RUN_DIR)" "$(CACHE_DIR)"
	@echo "TRACE-UFR OpenAI run -> run=$(TRACE_UFR_OPENAI_RUN_ID) modes=$(TRACE_UFR_MODES) models=$(OPENAI_MODELS)"
	$(PY) -m $(RUN_SWEEP_MOD) \
		--corpus-dir "$(TRACE_UFR_CORPUS_DIR)" \
		--out-dir "$(TRACE_UFR_OPENAI_RUN_DIR)" \
		--modes $(TRACE_UFR_MODES) \
		--provider openai \
		--models $(OPENAI_MODELS) \
		--extracts "$(EXTRACTS_DIR)" \
		--schema "$(FACT_SCHEMA)" \
		--cache "$(CACHE_BASE)" \
		--max-jobs $(TRACE_UFR_MAX_JOBS) \
		$(TRACE_UFR_RUN_EXTRA)

	@echo "TRACE-UFR Anthropic run -> run=$(TRACE_UFR_ANTHROPIC_RUN_ID) modes=$(TRACE_UFR_MODES) models=$(ANTHROPIC_MODELS)"
	$(PY) -m $(RUN_SWEEP_MOD) \
		--corpus-dir "$(TRACE_UFR_CORPUS_DIR)" \
		--out-dir "$(TRACE_UFR_ANTHROPIC_RUN_DIR)" \
		--modes $(TRACE_UFR_MODES) \
		--provider anthropic \
		--models $(ANTHROPIC_MODELS) \
		--extracts "$(EXTRACTS_DIR)" \
		--schema "$(FACT_SCHEMA)" \
		--cache "$(CACHE_BASE)" \
		--max-jobs $(TRACE_UFR_MAX_JOBS) \
		$(TRACE_UFR_RUN_EXTRA)

	@echo "TRACE-UFR Gemini run -> run=$(TRACE_UFR_GEMINI_RUN_ID) modes=$(TRACE_UFR_MODES) models=$(GEMINI_MODELS)"
	$(PY) -m $(RUN_SWEEP_MOD) \
		--corpus-dir "$(TRACE_UFR_CORPUS_DIR)" \
		--out-dir "$(TRACE_UFR_GEMINI_RUN_DIR)" \
		--modes $(TRACE_UFR_MODES) \
		--provider gemini \
		--models $(GEMINI_MODELS) \
		--extracts "$(EXTRACTS_DIR)" \
		--schema "$(FACT_SCHEMA)" \
		--cache "$(CACHE_BASE)" \
		--max-jobs $(TRACE_UFR_MAX_JOBS) \
		$(TRACE_UFR_RUN_EXTRA)

	@echo "Done."
	@echo "Corpus:    $(TRACE_UFR_CORPUS_DIR)"
	@echo "OpenAI:    $(TRACE_UFR_OPENAI_RUN_DIR)"
	@echo "Anthropic: $(TRACE_UFR_ANTHROPIC_RUN_DIR)"
	@echo "Gemini:    $(TRACE_UFR_GEMINI_RUN_DIR)"
