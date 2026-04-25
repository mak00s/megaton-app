# Agent Gotchas

Short operational notes for AI agents and shell automation.

- Adobe Tags workspace exit code `4` means outside-library-scope resources/files were detected. In CSK this can be steady state and should not automatically block a workflow; inspect `severity`, `summary`, and `details` before failing the run.

## Adobe Tags Snapshot Workers

CSK benchmark for `python -m tags status` on a property with about 170 in-library resources:

| Workers | Wall time | Notes |
|---:|---:|---|
| 5 | 12.8s | Slow |
| 10 | 7.8s | `megaton_lib` generic default |
| 20 | 5.4s | CSK sweet spot |
| 30 | 5.5s | Saturated |
| 40 | 5.0s | Saturated; no 429 observed |

Keep the shared default at `10` for safety. CSK wrappers can opt in with `TAGS_SNAPSHOT_WORKERS=20`; add `# TAGS_SNAPSHOT_WORKERS=20` to the CSK analysis repo `.env.example` so future agents do not need to re-benchmark.
