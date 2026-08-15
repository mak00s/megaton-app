# Architecture and Scope Contract

This document is the normative contract for what belongs in `megaton-app`.
Implementation and tests define current behavior; this document defines the
product boundary that new behavior must respect.

## Core Purpose

`megaton-app` is an **analytics workflow orchestration** toolkit.

It connects reusable analytics capabilities into workflows that configure,
acquire or execute, validate or audit, and deliver analytics data and
artifacts. It is not a general-purpose SaaS automation framework.

## Repository Responsibilities

| Owner | Responsibility |
| --- | --- |
| `megaton` | Notebook-facing analytics APIs and the shared low-level Google Sheets foundation |
| `megaton-app` | Reusable analytics workflow orchestration, validation, audit, execution, and delivery |
| Calling repositories | Project configuration, business policy, client-specific rules, and one-off workflows |
| External libraries or services | Generic SaaS clients and domain-neutral automation infrastructure |

Code that does not fit the `megaton-app` responsibility should be contributed
upstream, kept in the calling repository, or extracted into a separate package.

## Architectural Layers

1. **Interfaces**: `app/`, `scripts/`, and notebook entry points translate user
   intent into library calls. They remain thin and do not own business logic.
2. **Orchestration**: shared query, audit, validation, execution, and delivery
   flows coordinate capabilities and preserve workflow metadata.
3. **Analytics capabilities**: integrations and reusable operations for GA4,
   GSC, BigQuery, Adobe Analytics, Tags, Target, and related analytics systems.
4. **Adapters**: API, browser, storage, and messaging adapters support a named
   analytics workflow. They are not independent product surfaces.
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

### In Scope

- analytics API integration and shared provider behavior
- query and workflow orchestration across supported analytics systems
- analytics data and implementation validation
- reusable analytics audits and evidence collection
- scheduled, batch, and notebook execution support
- delivery of analytics results, reports, evidence, and follow-up artifacts
- generic configuration and metadata contracts required by those workflows

### Conditional Scope

Browser, storage, email, and other SaaS automation belongs here only when all
of the following are true:

- it supports a named analytics workflow stage
- an API is unavailable or insufficient for that workflow
- the behavior is reusable across analytics projects
- credentials, side effects, and human confirmation boundaries are explicit
- the adapter remains narrower than a general client for the external service

For example, Playwright capture used by analytics validation is in scope.
Box upload or Gmail draft behavior is in scope only as delivery for analytics
results or evidence. Expanding either into generic file management or mailbox
automation is out of scope.

### Out of Scope

- generic SaaS automation unrelated to an analytics workflow
- general-purpose Box, Gmail, browser, CRM, or project-management clients
- project-specific business logic, approval policy, routing, or reporting rules
- mutable client inventories, credentials, and organization-specific IDs
- duplicate wrappers for capabilities owned by `megaton` or another upstream
  library
- an application framework intended to host arbitrary non-analytics workflows

## Architectural Invariants

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

Before adding a feature, answer these questions in the issue or review:

1. Which analytics workflow and stage does it support?
2. Is the behavior reusable across analytics projects?
3. Is `megaton-app` the correct owner rather than `megaton`, a calling repo, or
   a generic external package?
4. Can project IDs, client policy, and mutable inventories remain configuration?
5. Can the UI and CLI stay thin over a shared library implementation?
6. Are validation, audit metadata, side effects, and failure behavior explicit?

If the first three answers are not clear, the feature does not belong in this
repository. Conditional adapters should document why API-based integration is
insufficient and where their analytics-specific boundary ends.

## Sources of Truth

- purpose, ownership, and scope: this document
- current runtime behavior: implementation and tests
- schemas and supported user-facing options: `docs/REFERENCE.md`
- setup and workflows: `docs/USAGE.md`
- agent execution rules: `AGENTS.md`

An implementation that violates this contract is architecture debt, not a new
scope precedent. Expanding the product boundary requires an explicit change to
this document in the same review.
