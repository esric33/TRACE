# Inference

Run model-planned evaluation with:

```bash
python -m TRACE.cli run_sweep \
  --benchmark trace_ufr \
  --corpus-dir artifacts/refactor/corpora/TRACE-UFR \
  --out-dir artifacts/refactor/runs/run_TRACE-UFR_openai \
  --modes full \
  --provider openai \
  --models gpt-5.2 \
  --resume
```

Use `oracle` mode to execute gold DAGs without provider calls:

```bash
python -m TRACE.cli run_sweep \
  --benchmark trace_ufr \
  --corpus-dir artifacts/refactor/corpora/TRACE-UFR \
  --out-dir artifacts/refactor/runs/run_TRACE-UFR_oracle \
  --modes oracle
```

Supported full-mode providers are `openai`, `anthropic`, and `gemini`.
