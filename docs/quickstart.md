# Quick Start

## Install

TRACE requires Python 3.12 or newer.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

## Run a No-API Smoke Test

```bash
make smoke3_offline
```

This generates a tiny `trace_ufr` corpus and executes the gold DAGs in `oracle` mode.

## Generate TRACE-UFR

```bash
python -m TRACE.cli generate \
  --benchmark trace_ufr \
  --out artifacts/refactor/corpora/TRACE-UFR \
  --distractors 0 1 3 5 10 \
  --n-total 600 \
  --seed 0 \
  --balance-templates \
  --max-compile-attempts 100 \
  --force
```

## Run Model Inference

Set the relevant provider key:

```bash
export OPENAI_API_KEY=...
export ANTHROPIC_API_KEY=...
export GOOGLE_API_KEY=...
```

Then run a sweep:

```bash
python -m TRACE.cli run_sweep \
  --benchmark trace_ufr \
  --corpus-dir artifacts/refactor/corpora/TRACE-UFR \
  --out-dir artifacts/refactor/runs/run_TRACE-UFR_openai \
  --modes full \
  --provider openai \
  --models gpt-5.2 \
  --max-jobs 1 \
  --resume
```
