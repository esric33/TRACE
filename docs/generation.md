# Dataset Generation

Generate a corpus with:

```bash
python -m TRACE.cli generate --benchmark trace_ufr --out artifacts/refactor/corpora/TRACE-UFR
```

Common options:

| Option | Meaning |
| --- | --- |
| `--benchmark` | Benchmark package to load. |
| `--out` | Output corpus directory. |
| `--distractors` | Distractor snippet counts to generate. |
| `--n-total` | Capsules per distractor setting. |
| `--seed` | Base random seed. |
| `--balance-templates` | Allocate examples evenly across templates. |
| `--p-family` | Family proportions. |
| `--w` | Per-family variant weights. |
| `--max-compile-attempts` | Maximum attempts to compile a valid capsule. |
| `--force` | Allow writing into a non-empty output directory. |

The generation step writes capsules, a corpus index, metadata, and benchmark profile files.
