# Todoist Consumer Rollout

## Scope

The 2026-10-16 rollout covers twelve consumer repositories plus the shared
toolkit. Concrete customer/repository inventories and operational identifiers
are private: keep them in ignored local records, never in this public repo.
The public contract is about consumer roles, not client identities.

All consumers have root agent rules, a local Todoist runbook and private Receipt
ignore rules. The shared [runbook](todoist.md) owns common setup and recovery.
No nested agent-rule files or scheduled completion hooks are required.

## Consumer Roles

- Issue handoff: existing business entrypoints retain their acknowledgement gate
  and private journal. Filing completion is not issue resolution.
- Financial operations: human execution/approval remains mandatory. Verify task
  completion before appending the financial completion record.
- Report delivery: human review and actual sending precede finalization.
  Verify the selected task and source-message binding before removing its star.
- Analysis and automation: a successful query, generated report, draft, test or
  data refresh is not delivery, publication, production acceptance or a transaction.

Task selection, labels, deadlines and completion criteria remain consumer policy.
No tokens are discovered/copied and no external writes are part of a rollout test.

## Environment Strategy

Use a dedicated task venv with reviewed released app/core pins, separate from
unrelated analytics/browser environments. Consumers with existing task flows
also update their own requirements. Verify imports outside the source checkout;
a bare global Python is not proof of the expected runtime.

## Receipt Safety

The consumer adapters use account/workflow/period operation identities independent
of search refinements. Before writing they inspect sibling Receipts, including
old filter-keyed attempts, and serialize same-task writes using POSIX locks.
Unknown/acknowledged attempts block new aliases. Confirmed rejections permit a
separately reviewed corrected attempt; do not switch directories to evade guards.

Report finalization binds its source-message context before POST using a private
digest sidecar. A different source, corrupt binding or legacy Receipt with no
binding must stop instead of adopting an old completion. Preview/apply should pin
the same message ID. Dry-run invokes shared completion preflight and never creates
Receipts, sidecars or lockfiles.

These consumer-specific journal guards do not extend the released v0.41.0
low-level client/CLI contract.

## Offline Validation (2026-10-17)

- CLI/import checks from all thirteen roots passed at rollout.
- Financial consumer: 131 tests; reporting consumer: 361 tests.
- Existing issue consumer: 124 tests; shared task client/CLI: 57 tests (2026-10-16).
- Regressions cover changed filters after unknown writes, old alias Receipts,
  source-message mismatch through finalization, GET-only resume, malformed
  journals/missing bindings, concurrent aliases, rejection recovery and preflight.
- Checks do not establish live account access. No real task/mail/Sheets write or
  credential replication is performed.
