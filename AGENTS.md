# megaton-app

Analytics workflow orchestration toolkit for GA4, GSC, BigQuery, Adobe Analytics, Adobe Tags, Target, and Google Sheets.

## 1. Scope

- The normative purpose, ownership boundaries, and feature admission test are in `docs/architecture.md`.
- `megaton_lib/` is the reusable core; `scripts/` and `app/` are thin interfaces over it.
- Keep project-specific policy and business logic in the calling repo.
- Do not expand browser, Box, Gmail, or other adapters into generic SaaS automation.

## 2. Self-Maintenance

- Update this file only for durable, cross-repo rules.
- Do not add task notes, temporary inventories, or one-off troubleshooting here.
- Put product purpose, architectural boundaries, and scope decisions in `docs/architecture.md`.
- If a rule is really a user-facing workflow, put it in `docs/USAGE.md` or `docs/REFERENCE.md` instead.

## 3. Source of Truth

Use the source that owns the question:

- purpose, ownership, and scope: `docs/architecture.md`
- current runtime behavior: implementation under `megaton_lib/`, `scripts/`, and `app/`, then tests
- schemas and supported options: `docs/REFERENCE.md`
- setup and user workflows: `docs/USAGE.md`
- agent execution rules: this file

If implementation conflicts with `docs/architecture.md`, treat it as architecture debt rather than scope precedent.

## 4. Directory Guide

- `megaton_lib/`: shared library
- `scripts/query.py`: unified CLI for GA4 / GSC / BQ / AA queries
- `scripts/run_notebook.py`: notebook runner
- `scripts/audit.py`: audit CLI
- `app/`: Streamlit UI
- `configs/`: aliases and project config
- `credentials/`: local credential JSONs, gitignored
- `input/`: Streamlit handoff only
- `output/`: result files and job artifacts

## 5. Credential and Resource Discovery

- Do not hardcode mutable site inventories into AGENTS.
- Discover live resources with commands:
  - `python scripts/query.py --list-gsc-sites --json`
  - `python scripts/query.py --list-ga4-properties --json`
  - `python scripts/query.py --list-bq-datasets --json`
- Prefer aliases from `configs/sites.local.json` or `configs/sites.json` over raw URLs and property IDs.
- Service account JSONs live under `credentials/` and should stay local.

## 6. Query Workflow

### CLI first

Use `scripts/query.py` for ad-hoc extraction unless you are intentionally working at the library layer.

```bash
python scripts/query.py --params output/my_query.json --output output/result.csv
```

- `--params` accepts any JSON file path
- do not overwrite `input/params.json` for CLI-only work
- prefer `--output` over `--json` for non-trivial results
- reuse saved files when practical

### Query basics

- every query needs `schema_version: "1.0"`
- `source` must be one of `ga4`, `gsc`, `bq`, `aa`
- use `pipeline` to reduce result size at query time
- BigQuery jobs in this ecosystem usually require `location="asia-northeast1"`

## 7. Library Rules

- Sheets(gspread/batchUpdate/retry)の新機能・修正は **megaton 側(`megaton.gsheet_lowlevel` / `gsheet.py`)にまず実装**し、`megaton_lib.gspread_lowlevel` は再エクスポート shim のまま保つ(docs/sheets-consolidation.md)。
- Keep `megaton_lib/` generic and reusable.
- Do not move UI concerns into `megaton_lib/`.
- Prefer extending existing helpers over creating parallel variants.
- `show()` should be used for DataFrame inspection instead of dumping large tables to stdout.

## 8. Validation Policy

- Browser routing: unfamiliar-page exploration uses the agent's authorized browser tools; repeatable analytics jobs use a consumer CLI over shared helpers. Prefer a supported API when it meets the task.
- Discover the route with `python -m megaton_lib.browser_workflow guide`; check local prerequisites with `doctor` (browser launch requires `--check-browser`). Follow agent tool restrictions; never use the library to bypass them.
- Do not equate doctor success, page load, login, and analytics delivery. Record the actual evidence separately. Browser workflow details live in `docs/USAGE.md`.

- `megaton_lib.validation` is the shared-first home for Playwright, contracts, and AA beacon validation.
- Validation metadata should use `megaton_lib.validation.metadata.build_validation_run_metadata`.
- New validation entrypoints should start from `docs/templates/validation_thin_entrypoint.py`.
- Use `python scripts/check_validation_usage.py /path/to/repo` when inventorying validation usage across repos.

## 9. Testing

Run the practical subset for touched areas:

```bash
python -m pytest -q
python -m pytest -q -m unit
```

- after changing `megaton_lib/` or `app/`, run targeted tests at minimum
- keep mocks and fixtures in `tests/`

## 10. Adobe Auth

- Adobe Analytics, Reactor, and Target share the auth layer in `megaton_lib/audit/providers/adobe_auth.py`
- preferred local setup is credential JSON under `credentials/`
- supported fallback env vars remain:
  - `ADOBE_CLIENT_ID`
  - `ADOBE_CLIENT_SECRET`
  - `ADOBE_ORG_ID`
- tokens may be cached to disk and refreshed automatically

## 11. Gmail Drafts

- Gmailの下書き作成・返信・取得・更新・添付変更・検証は、原則として `megaton_lib.gmail_client` の下書き専用機能を使用する。CLIは `python -m megaton_lib.gmail_draft --help`。
- Gmailコネクタやブラウザ操作は、標準経路が利用できず、ユーザーが対象操作と代替手段を明示的に許可した場合のみ使用する。機能不足や認証エラーを理由に自動で切り替えない。
- 認証にはユーザーOAuthを使用し、サービスアカウントは使用しない。本ライブラリは送信機能を提供しない。代替手段の利用許可は送信の許可を含まない。
- アカウント設定と操作手順は `docs/USAGE.md`、scopeとJSON契約は `docs/REFERENCE.md` を参照する。

## 12. Google Docs Edits

- 指定されたレポート・提案書・議事録の対応範囲内の編集は `megaton_lib.docs_client` / `python -m megaton_lib.docs_edit --help` を使用する。
- 原本を直接変更するため、対象文書・タブ・差分を確認し、承認された保存済みplanだけを `apply --apply` で適用する。競合時は再計画・再確認し、結果不明時は再適用しない。
- ユーザーOAuthと期待メールアドレスを明示する。Gmail tokenやGA/GSCサービスアカウントへの自動流用、コネクタ/ブラウザへの自動切替は禁止。代替手段には対象操作についての明示許可が必要。
- planとget結果には本文が含まれる。Gitや共有ログへ入れない。認可・操作手順と対象外の編集は `docs/USAGE.md` / `docs/REFERENCE.md` を参照する。

## 13. Documentation

- `docs/architecture.md`: purpose, ownership, scope contract, and feature admission test
- `docs/USAGE.md`: setup and common workflows
- `docs/REFERENCE.md`: schema, CLI options, pipeline, auth, and library APIs
- `docs/PYTHON_API.md`: Python entry points (notebook facade, chain API, dates, report_run)
- `docs/CHANGELOG.md`: change history

Keep AGENTS short; keep detailed examples and full schema references in the docs.
