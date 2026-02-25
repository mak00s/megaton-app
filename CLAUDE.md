# CLAUDE.md

All project instructions are in **AGENTS.md** (shared with Cursor / Codex).
Read AGENTS.md first — it covers directory structure, CLI usage, rules, and recipes.

## Claude Code specific notes

- When working from a **parent directory** (not megaton-app root), run CLI with:
  `python megaton-app/scripts/query.py --params path/to/query.json --json`
- `input/params.json` is the **Streamlit UI handoff file** — do not overwrite it for ad-hoc CLI queries.
  Write your query JSON to a separate file (e.g., `output/tmp_query.json`) and pass it via `--params`.
- For detailed JSON schema and all CLI options, see `docs/REFERENCE.md`.
