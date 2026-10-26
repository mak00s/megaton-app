# Todoist Consumer Runbook

Consumer scope and migration boundaries: [rollout inventory](todoist-consumers.md).

## Runtime

The canonical shared checkout is `~/Repos/megaton-app`. On this Mac, `.venv`
links to the shared uv-managed Python 3.13 runtime `~/.venvs/py313`, with the
local `megaton-app` checkout installed editable. Use the existing environment;
do not run `uv sync` or recreate it for task handoffs. Setup/rebuild is owned by
`~/Repos/dotfiles/shared-env.sh`. Check the actual module from a consumer root:

```bash
"$HOME/Repos/megaton-app/.venv/bin/python" -c 'import sys; import megaton_lib.todoist_client as t; print(sys.version, t.__file__)'
```

Other environments may use an explicitly selected runtime containing the reviewed
shared client. See the consumer's `docs/todoist.md`. No new scheduled task
completion hooks are installed.

From a consumer root (not from the shared source directory, which shadows installs):

```bash
TODOIST_PY="$HOME/Repos/megaton-app/.venv/bin/python"
"$TODOIST_PY" -c 'from importlib.metadata import version; import megaton_lib.todoist_client as t; print(version("megaton-app"), t.__file__)'
"$TODOIST_PY" -m megaton_lib.todoist_tasks --help
```

The module must come from the intended environment and reviewed source. Recurring
completion needs `complete --expected-occurrence` support; older releases reject
it. Never silently use a stale global Python.

The legacy `.venv-tasks` directory is retired on this Mac. Its remaining files
are not a supported runtime and must not be selected by agents or old commands.
Do not delete it without checking external callers; use the shared `.venv` above.

## Credentials and Selection

Supply `TODOIST_TOKEN` through the caller's approved local environment/secret
mechanism. The CLI does not read `.env`, discover tokens in other repos, authorize
accounts, or use a connector/browser fallback. Do not put the token on a command
line or in a shared log. For an interactive shell an explicitly supplied token
can be read without shell history (Bash syntax):

```bash
read -rs -p 'Todoist token: ' TODOIST_TOKEN; echo
export TODOIST_TOKEN
"$TODOIST_PY" -c 'from megaton_lib.todoist_client import TodoistClient; print(TodoistClient.from_env().account())'
```

Compare that account ID with the user's intended account; do not adopt any returned
account blindly. Then use the confirmed ID for `--expected-user-id`:

```bash
"$TODOIST_PY" -m megaton_lib.todoist_tasks list --expected-user-id ACCOUNT_ID --project-id PROJECT_ID
"$TODOIST_PY" -m megaton_lib.todoist_tasks get --expected-user-id ACCOUNT_ID --task-id TASK_ID
```

List/get output contains task text; keep it in protected local files if necessary.
Select one task unambiguously. A query, report, upload, draft or passing test does
not itself establish completion of delivery, payment, publication or acceptance.

## Preview, Apply, Verify

Use the reviewed fingerprint from `get`. Run the following preview first, show the
intended task/action, then apply only with the user's authorization:

```bash
"$TODOIST_PY" -m megaton_lib.todoist_tasks complete --expected-user-id ACCOUNT_ID \
  --task-id TASK_ID --expected-fingerprint FINGERPRINT
"$TODOIST_PY" -m megaton_lib.todoist_tasks complete --expected-user-id ACCOUNT_ID \
  --task-id TASK_ID --expected-fingerprint FINGERPRINT \
  --apply --receipt .todoist-receipts/ACCOUNT_ID/complete-OPERATION.json
"$TODOIST_PY" -m megaton_lib.todoist_tasks verify --expected-user-id ACCOUNT_ID \
  --receipt .todoist-receipts/ACCOUNT_ID/complete-OPERATION.json
```

Follow-up creation likewise needs reviewed content, project, due date and labels:

```bash
"$TODOIST_PY" -m megaton_lib.todoist_tasks create --expected-user-id ACCOUNT_ID \
  --content 'Review the completed report' --project-id PROJECT_ID
# After authorization, repeat that exact command with:
# --apply --receipt .todoist-receipts/ACCOUNT_ID/create-OPERATION.json
```

Receipt files contain task content: keep them private, durable, account-scoped and
Git-ignored. Do not reuse an attempt path for unrelated work. Operation names are
caller-selected; include the target period/milestone to avoid cross-month reuse.

## Recovery and Limits

- `not_attempted`: preview only; no write or persistent Receipt yet.
- `acknowledged` and `verified=true`: server acknowledged and readback matched.
- Failed readback: run `verify` only; do not repeat the write.
- `unknown`: inspect the task and Receipt; do not delete it or switch to a new
  path to write again. Resolve the uncertain result manually.
- `rejected`: preserve the Receipt. Correct the input/auth or wait for rate limits,
  review again, then use a new path only for an explicitly authorized new attempt.

Missing active tasks and HTTP 404 are not completion evidence. Shared completion
uses the original ID, project, saved time window and completed-task history.
Recurring completion requires `--expected-occurrence` equal to the reviewed
`due.date`, plus `--expected-fingerprint`. It closes only that occurrence, keeping
the recurring task active. Verification requires an advanced due date, unchanged
recurrence rule and increased server `completed_count`; date editing alone does
not prove completion. Keep one Receipt per occurrence and only verify it on retry.
Parents with active subtasks are rejected. Finish those subtasks
manually; never turn off the guard or substitute a raw close request. Updates,
deletion and general project administration are outside this adapter.

Successful readback records a local `verified_at` timestamp in the Receipt.
Later verification still checks the current state; the timestamp only preserves
evidence that the original transition was successfully verified. It does not
authorize another write or turn a later mismatch into success.

The API contract remains in [REFERENCE](REFERENCE.md#todoist-task-handoffs-megaton_libtodoist_client)
and [USAGE](USAGE.md#todoistで作業の完了・後続タスクを記録する).
