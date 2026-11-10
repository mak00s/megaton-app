# Architecture and Scope Contract

This document is the normative contract for what belongs in `megaton-app`.
Implementation and tests define current behavior; this document defines the
product boundary that new behavior must respect.

## Core Purpose

`megaton-app` is an **analytics workflow orchestration** toolkit, plus the
**shared guarded adapters** its consumers use to hand work off.

Tier 1 connects reusable analytics capabilities into workflows that configure,
acquire or execute, validate or audit, and deliver analytics data and
artifacts. Tier 2 is a small set of narrow, receipt-guarded adapters (browser
sessions, Box, Gmail drafts, Todoist handoffs, Google Docs edits) that several
consumer repositories share, including consumers whose own work is not
analytics. It is not a general-purpose SaaS automation framework: each adapter
exposes only the enumerated operations in its contract below.

## Repository Responsibilities

| Owner | Responsibility |
| --- | --- |
| `megaton` | Google analytics operations, single-query Python/CLI contracts, Notebook sessions and the shared Sheets foundation |
| `megaton-app` | Reusable analytics workflow orchestration, validation, audit, execution, and delivery; shared guarded adapters used by two or more consumers |
| Calling repositories | Project configuration, business policy, client-specific rules, and one-off workflows |
| External libraries or services | Generic SaaS clients and domain-neutral automation infrastructure |

Code that does not fit the `megaton-app` responsibility should be contributed
upstream, kept in the calling repository, or extracted into a separate package.

## Single Queries and Machine Interfaces

Megaton owns checkout-independent GA4/GSC/BigQuery single queries, explicit
authentication, result/error contracts and installed CLI entry points.
`megaton_lib.megaton_client` delegates GA4/GSC execution to `megaton.query` while
preserving its credential routing and existing DataFrame APIs. Adobe, relative
dates, site aliases, pipelines, save policies, batch/job management, audits,
validation and delivery remain here. Existing BQ parameterized/native and
explicit-credential GA4 paths retain their current contracts.

The delegated query limit now caps fetched rows. GSC alias pairs are rejected
and path conversion reaggregates metrics; these corrections are documented as
consumer-visible changes in 0.40.0. Notebook GA4 limit remains a page size.

Raw pagination completeness belongs in core, before any frame transformations.
App extraction policy fails closed on truncated/unknown evidence by default;
intentional partial extracts require explicit opt-in. Never reconstruct coverage
from the number of post-aggregation rows. Release a metadata-capable core before
publishing an app that depends on this guard.

MCP adapters call the same library APIs. They own transport authentication,
tool exposure and permissions. Project inventories, client rules and delivery
policy belong to consumers. Repository visibility does not define ownership;
this repo is public following sanitization.

The completeness-aware delegation requires Megaton 2.5.0 or newer. Existing installed app releases retain their
original Megaton requirement.

## Architectural Layers

1. **Interfaces**: `app/`, `scripts/`, and notebook entry points translate user
   intent into library calls. They remain thin and do not own business logic.
2. **Orchestration**: shared query, audit, validation, execution, and delivery
   flows coordinate capabilities and preserve workflow metadata.
3. **Analytics capabilities**: integrations and reusable operations for GA4,
   GSC, BigQuery, Adobe Analytics, Tags, Target, and related analytics systems.
4. **Adapters**: API, browser, storage, and messaging adapters serve the
   workflows above and the consumers' own handoffs. Each is limited to its
   enumerated contract; none is a general client for its service.
5. **Project policy**: site inventories, client rules, thresholds, routing
   decisions, and project-specific transformations stay outside this repo.

`megaton_lib/` is the reusable center. Interfaces may depend on it; reusable
workflow logic must not depend on Streamlit, checkout-local files, or a single
project's configuration.

## Packaging Contract

The repository, distribution, and import package currently have distinct
roles:

| Surface | Name | Contract |
| --- | --- | --- |
| Product repository | `megaton-app` | Develops the library and checkout-local interfaces together |
| Python distribution | `megaton-app` | Transitional distribution name used by existing consumers |
| Installed package | `megaton_lib` | The only Python package included in the wheel |
| Checkout interfaces | `app/`, `scripts/` | Run from a repository checkout; not installed as packages or console commands |

Installing `megaton-app` therefore installs the reusable library, not a
standalone application. Optional dependency groups such as `ui` provide the
dependencies needed to run the corresponding checkout interface; they do not
change the wheel contents.

Until `app/` and `scripts/` no longer depend on repository-relative config,
credentials, input, and output paths, they must remain outside the wheel. Do
not add application entry points that conceal those checkout assumptions.

Packaging changes must preserve these invariants:

- a wheel contains `megaton_lib`, distribution metadata, and license files
- a wheel does not contain `app`, `scripts`, `tests`, credentials, configs, or
  runtime input and output directories
- an installed wheel can import `megaton_lib` outside a repository checkout
- checkout interfaces depend on `megaton_lib`; the library does not depend on
  checkout interfaces

The intended later architecture is an installable `megaton_app` interface
package depending on the reusable library distribution. That split requires a
coordinated migration of existing consumers of the `megaton-app` distribution
name; it is not implied by the current wheel.

## Scope

Two tiers have separate admission tests. Tier 1 is the product's purpose; tier 2
exists so consumers do not copy guarded write logic into each repository.

### Tier 1: Analytics Workflow Orchestration

- analytics API integration and shared provider behavior
- query and workflow orchestration across supported analytics systems
- analytics data and implementation validation
- reusable analytics audits and evidence collection
- scheduled, batch, and notebook execution support
- delivery of analytics results, reports, evidence, and follow-up artifacts
- generic configuration and metadata contracts required by those workflows

### Tier 2: Shared Guarded Adapters

Browser, storage, email, task and document automation belongs here only when
all of the following are true. Supporting an analytics workflow is not required;
being shared and guarded is.

- two or more consumer repositories use it, or a second consumer is adopting it
  in the same change; a single consumer keeps the code in its own repository
- the operations are enumerated in this document and stay narrower than a
  general client for the external service
- every write is explicit (preview by default, then apply), leaves a durable
  private receipt, and is confirmed by readback; ambiguous writes never replay
- wording, recipients, task and document selection, deadlines, and the decision
  that work is complete stay in the consumer
- credentials and account identity are explicit and checked before writes
- browser/UI automation is used only when an API is unavailable or insufficient

The paragraphs below are the per-adapter contracts. Extending an adapter means
editing its contract here in the same review.

Playwright sessions and capture serve analytics validation and the consumers'
login-gated workflows. Box upload delivers results or evidence. Gmail
supports report delivery plus draft-only operations: explicit new drafts,
replies to caller-selected messages, and retrieval, update and verification of
caller-selected drafts. Ordinary correspondence can use this same mechanism;
message composition policy and recipient decisions remain in the consumer.
Sending, automatic replies, inbox monitoring, bulk processing and general mailbox
management are out of scope. User OAuth identity must be checked before writes;
service accounts, ADC and automatic connector/browser fallback are not supported.
Alternative Gmail tools require explicit user permission for the particular
operation when the standard path is unavailable; that permission never implies
permission to send. This contract does not expand other adapters' scope.

Todoist supports task handoff for caller-selected work: enumerate
and read candidate tasks, create a follow-up task, complete one explicitly
selected task or explicitly reviewed recurring occurrence, and verify the result. This supports workflow
handoffs across consumers (including Adobe issue filing), not general project
management. Wording, projects, deadlines, priority, deduplication keys
and the decision that work is complete belong in the consumer. A successful
query, draft or report run must never implicitly complete a task. Writes require
explicit apply, account-scoped durable private receipts and readback; ambiguous
writes never automatically replay. Recurring completion binds the original due date
and fingerprint, and verifies next due plus completed_count progression. An explicitly
approved occurrence-date restoration is allowed through the guarded restore operation;
it preserves the rule and verifies the exact date without completing the task. Explicit
fingerprint-guarded task renaming may change the title only and verifies schedule preservation.
General updates, deletion,
project administration, inbox monitoring and connector/browser fallback are out
of scope. Personal API tokens are explicitly configured, never discovered.

Google Docs supports a narrow contract for caller-selected reports,
proposals and meeting records: structured retrieval, a reviewed single text edit
in a plain body paragraph, revision-guarded application and text readback.
Ordinary document editing may use this mechanism; document selection, prose and
approval policy remain in consumers. User OAuth identity is checked by the
standard loader. General document authoring, formatting, Drive search, creation,
sharing, deletion and automatic connector/browser fallback are out of scope
except for the limited report mutations and temporary image copies below.
Unlike Gmail drafts, applying an edit changes the shared original immediately.
Dry-run is the default and saved plans must be reviewed before explicit apply.

Caller-selected analytics reports may additionally use reviewed structured body
mutations: paragraphs, headings, bullets, supported text/paragraph styles, inline
PNG/JPEG and unmerged rectangular tables. Consumer-owned anchors, prose, charts
and numeric values stay outside the library. These operations use a separate
versioned plan, approval digest, durable execution receipt, guarded batches and
structural readback; they do not replace entire documents. This is not a general
Google Docs editor. Unsupported rich content, suggestions and ambiguous anchors
must be rejected, not flattened. The earlier single-text-edit contract remains.

Temporary image hosting is a narrow delivery prerequisite: only new copies in
an explicitly selected Drive folder may be publicly shared after explicit
approval. Use independently verified user OAuth, never an implicit service
account/ADC fallback. Journal uploads and delete the copies after the attempt;
unknown upload/cleanup outcomes require manual inspection. Existing file
permissions must not change. No Drive search, source-file sharing or general
file-management API is exposed.

#### Existing Utilities

Small domain-neutral helpers that predate this test stay where they are:
`tz_utils`, `env_utils`, `json_cache`, `http_fetch`, `jptext`, `notify`,
`llm_advisor`, `llm_audit`, `login_gate` and `scrape_errors`. They are not precedent. A new
helper of this kind needs two consumers, like any tier 2 addition; otherwise it
belongs in the consumer.

#### Module Map

Import paths do not encode the tier; this table does. A test keeps it complete,
so a new module must be placed in a tier when it is added.

| Tier | Modules |
| --- | --- |
| 1 | `megaton_client`, `query_runner`, `query_normalization`, `params_validator`, `result_inspector`, `batch_runner`, `job_manager`, `site_aliases`, `credentials`, `audit/`, `validation/`, `report_run`, `report_run_summary`, `report_validation`, `report_work_log`, `run_journal`, `sheets`, `gspread_lowlevel`, `ga4_helpers`, `gsc_utils`, `bigquery_utils`, `traffic`, `table_utils`, `dates` and its shims (`date_utils`, `date_template`, `periods`), `csv_validation`, `notebook`, `analysis`, `gtm_client`, `gtm_review`, `cli_help`, `params_diff`, `docs_sites` |
| 2 | `playwright_browser`, `browser_workflow`, `box_api`, `box_ui`, `gmail_client`, `gmail_draft`, `report_gmail_draft`, `todoist_client`, `todoist_tasks`, `todoist_completion`, `docs_client`, `docs_edit`, `docs_mutate`, `docs_mutations`, `docs_mutation_io`, `docs_assets`, `google_workspace` |
| Existing utilities | the modules listed above |

### Out of Scope

- SaaS automation outside the tier 2 contracts above, and any adapter used by a
  single consumer
- general-purpose Box, Gmail, browser, CRM, or project-management clients
- project-specific business logic, approval policy, routing, or reporting rules
- mutable client inventories, credentials, and organization-specific IDs
- duplicate wrappers for capabilities owned by `megaton` or another upstream
  library
- an application framework intended to host arbitrary non-analytics workflows

## Architectural Invariants

Browser workflow ownership follows the same boundary: exploratory interaction
uses the agent's authorized tools, repeatable analytics execution uses a
consumer-owned CLI over shared sessions/validation/delivery adapters. Prefer APIs
where sufficient. Tool permissions still apply, regardless of implementation.
The shared browser guide and local doctor are discovery/preflight tools, not a
general browser agent, task planner, or arbitrary click/navigation server.

- Keep `megaton_lib/` generic and reusable across repositories.
- Keep `app/` and `scripts/` thin; shared behavior belongs in the library.
- Keep checkout-relative application state out of the library distribution.
- Prefer one canonical workflow over parallel convenience paths.
- Put Google Sheets low-level behavior in `megaton` first; keep the
  `megaton_lib.gspread_lowlevel` compatibility shim thin.
- Represent validation and audit results with shared contracts and metadata.
- Make destructive or externally visible actions explicit and reviewable.
- Do not use an existing out-of-scope helper as precedent for further scope
  expansion.

## Typing Policy

- Blocking type checks start with schema, configuration, job state, and result
  contracts where types express stable workflow guarantees.
- Add a module to the blocking scope only after it is clean, and do not remove
  it to accommodate later regressions.
- Permit intentional `Any` at pandas, Playwright, Streamlit, gspread, Google
  API, Adobe API, and other raw external-data boundaries.
- Do not make Notebook, CLI, or adapter APIs more verbose solely to satisfy a
  strict type-checking mode.
- Prefer explicit validation and narrowing at contract boundaries over broad
  ignores or unchecked casts.

## Feature Admission Test

Before adding a feature, decide its tier and answer these questions in the
issue or review.

Tier 1:

1. Which analytics workflow and stage does it support?
2. Is the behavior reusable across analytics projects?
3. Is `megaton-app` the correct owner rather than `megaton`, a calling repo, or
   a generic external package?
4. Can project IDs, client policy, and mutable inventories remain configuration?
5. Can the UI and CLI stay thin over a shared library implementation?
6. Are validation, audit metadata, side effects, and failure behavior explicit?

Tier 2:

1. Which two consumers use it, and does one of them currently hold a copy?
2. Which enumerated operations does it add, and what stays unsupported?
3. Is every write previewed, applied explicitly, receipted and read back?
4. Which decisions stay in the consumer?
5. Why is an API insufficient, if it automates a browser?

If the first three answers of the relevant tier are not clear, the feature does
not belong in this repository.

## Sources of Truth

- purpose, ownership, and scope: this document
- current runtime behavior: implementation and tests
- schemas and supported user-facing options: `docs/REFERENCE.md`
- setup and workflows: `docs/USAGE.md`
- agent execution rules: `AGENTS.md`

An implementation that violates this contract is architecture debt, not a new
scope precedent. Expanding the product boundary requires an explicit change to
this document in the same review.
