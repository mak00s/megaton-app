# Todoist Consumer Runbook

Consumer scope and migration boundaries: [rollout inventory](todoist-consumers.md).

## Runtime

The canonical shared checkout is `~/Repos/github_mak00s/megaton-app`. Keep ad-hoc task
operations separate from consumers' analytics/browser environments, rather than
upgrading all their dependencies for a task handoff. The task runtime is a
dedicated `.venv-tasks` inside that checkout that installs the published
package, not the checkout source. From the checkout root:

```bash
python3.12 -m venv .venv-tasks
printf '*\n' > .venv-tasks/.gitignore   # keep the venv out of Git
.venv-tasks/bin/python -m pip install \
  'megaton-app @ git+https://github.com/mak00s/megaton-app.git@v0.43.1' \
  'megaton @ git+https://github.com/mak00s/megaton.git@v2.6.2'
```

Do not recreate an existing venv just to run these commands. Use its Python to
install the pins, or inspect the installed version first. Consumer invoice and
megaton-notebooks automated flows use their own requirements and venv instead;
see their `docs/todoist.md`. No new scheduled task completion hooks are installed.

From a consumer root (not from the shared source directory, which shadows installs):

```bash
TODOIST_PY="$HOME/Repos/github_mak00s/megaton-app/.venv-tasks/bin/python"
"$TODOIST_PY" -c 'from importlib.metadata import version; import megaton_lib.todoist_client as t; print(version("megaton-app"), t.__file__)'
"$TODOIST_PY" -m megaton_lib.todoist_tasks --help
```

The version must be 0.43.1 or a deliberately reviewed successor and the module must
come from the intended environment. Local co-development may use an explicit
editable install of the canonical checkout; verify the module path again. Never
silently use a stale global Python.

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
Recurring tasks and parents with active subtasks are rejected. Finish those tasks
manually; never turn off the guard or substitute a raw close request. Updates,
deletion and general project administration are outside this adapter.

The API contract remains in [REFERENCE](REFERENCE.md#todoist-task-handoffs-megaton_libtodoist_client)
and [USAGE](USAGE.md#todoistで作業の完了・後続タスクを記録する).
