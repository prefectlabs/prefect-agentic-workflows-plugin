# Behavioral evaluations

The evaluations run a real agent with the `agentic-workflows` skill and this
checkout's MCP server, against a Prefect Cloud sandbox workspace. They are a
[Pydantic Evals](https://ai.pydantic.dev/evals/) dataset with one case for
each scenario in `scenarios/`. The task of each case is one conversation
between the agent and a simulated user. Its output is the outcome of that
conversation: the transcript, the plan files the agent wrote, and the state
of the run's flows in the sandbox. The evaluators check that outcome: the
plan, the tools the agent called in order and with which arguments, and the
flows it left in the workspace.

Each run starts Claude Code through the Claude Agent SDK, so it costs money,
and the same case can pass once and fail the next time. `uv run pytest` runs
only `tests/`, which includes the tests of the harness and of each scenario's
checks in `tests/eval_harness/`. Those start no agent and call no workspace.

## Running

The harness reads the sandbox from `PREFECT_API_URL` and `PREFECT_API_KEY`
and stops before any agent starts when either one is missing. The workspace
needs execution plans enabled and an object storage bucket. Use a sandbox
workspace: the cases create flows and a Secret block there.

The Claude Agent SDK includes its own copy of Claude Code. It uses
`ANTHROPIC_API_KEY` when it's set, and your Claude Code login otherwise. Run
from `server/`:

```sh
export PREFECT_API_URL=https://api.prefect.cloud/api/accounts/<id>/workspaces/<id>
export PREFECT_API_KEY=<a key for the sandbox>

uv run python -m evals                                    # every case, once
uv run python -m evals --case scheduled_edit              # one case
uv run python -m evals --case scheduled_edit --repeat 5   # a pass rate
uv run python -m evals --model claude-sonnet-5            # pick the agent's model
uv run python -m evals --summary report.md                # also write the report as Markdown
```

| Option | What it does |
|---|---|
| `--case NAME ...` | Runs only these cases. Pass it more than once, or with several names. |
| `--repeat N` | Runs each case `N` times. The summary has each case's pass rate. |
| `--model M` | The agent's model. The default is the Claude Agent SDK's. |
| `--max-concurrency N` | The most runs at the same time. The default is 3. |
| `--summary PATH` | Also writes the pass rates, the failed checks, and the report to `PATH` as Markdown. |
| `--output-dir PATH` | Where each run's directory goes. The default is a new temporary directory. |

The command prints the Pydantic Evals report, with a column of assertions for
each run, then one `FAILED` line for each failed check with the reason. It
exits with 1 when a check failed, when a run or an evaluator raised, or when
the harness couldn't delete a run's flows, and with 2 when the sandbox
settings are missing.

Each run has a directory in `<output-dir>/<session prefix>/`, such as
`eval-3f9a1c/2-scheduled_edit/`. It has `transcript.json`, with every tool
call and message, and the agent's working directory in `agent/`. The
transcript is written also when the agent raises.

The agent loads only the `prefect-agentic-workflows` server
(`strict_mcp_config`), only project settings, and only the `agentic-workflows`
skill (the SDK's `skills` option), so MCP servers and plugins from your own
Claude Code settings are not used. The command removes every `PREFECT_`
variable from its environment after it reads the sandbox settings. The
server and the agent get the sandbox's URL and key, and an empty
`PREFECT_HOME`, so your Prefect profiles are not used either.

## Flow names and cleanup

Each session picks a prefix such as `eval-3f9a1c-`, and each run gets a
numbered prefix inside it, such as `eval-3f9a1c-2-`. Runs that happen at the
same time, and repeats of one case, never find each other's flows. A case's
prompt gives the agent the full flow name, and a check fails when the agent
calls `get_or_create_flow` with a name that doesn't start with the run's
prefix.

`SandboxLifecycle` in `lifecycle.py` is the dataset's `CaseLifecycle`. Its
`setup` picks the run's prefix and directory and runs the case's `setup`
function. Its `teardown` deletes the flows with the run's prefix. Pydantic
Evals calls `teardown` also when the task or `setup` raised. After the last
run, the command deletes every flow with the session's prefix, to catch a
run whose cleanup failed. The harness lists the flows with
`POST /flows/filter` and deletes each one. Deleting a flow also deletes its
execution plan, its plan versions, and its schedules. The harness never
deletes a flow without the prefix. A session that is killed, such as a
cancelled CI job, can leave its flows in the sandbox. Delete those by hand by
their `eval-` prefix.

The `eval-github-token` Secret block, which the release-notes case
references, holds a placeholder value. The harness creates it when it's
missing and never deletes it.

## What a run does

1. `SandboxLifecycle.setup` makes the run's directory, picks its prefix, and
   runs the case's `setup` function, which adds state to the sandbox through
   the API, such as a flow with an active plan version and a schedule.
2. The task, `scenario_task` in `runner.py`, makes a working directory with
   the skill in `.claude/skills/agentic-workflows/` and the case's files.
3. It sends the case's prompt to the agent with `ClaudeSDKClient`. After
   each turn, the simulated user answers the agent's final message from the
   case's rules, and the runner sends that answer in the same session. The
   conversation ends when a rule says so, when no rule applies, or after
   `max_turns` turns.
4. It reads the tool calls from the SDK's messages and the plan files from
   `workflows/`. Then it reads the state of each flow with the run's prefix
   into `outcome.flows`: the active version and its plan, the plan versions
   from oldest to newest, and the schedules. The evaluators read these, not
   the live API.
5. The evaluators run. `CommonChecks` runs on every case: the agent's turns
   finished without error, every flow it created has the run's prefix, and
   it never called `start_run`. `ScenarioChecks` runs the case's own checks.
   Each check is one assertion in the report, with its detail as the reason.
6. `SandboxLifecycle.teardown` deletes the run's flows.

The report also has two metrics for each run: `agent_cost_usd`, the cost the
SDK reported, and `agent_turns`.

## CI

`.github/workflows/evals.yml` runs every case once with `claude-haiku-5-5` on
each pull request that changes `skills/` or `server/`. It uses the
`PREFECT_API_URL`, `PREFECT_API_KEY`, and `ANTHROPIC_API_KEY` repository
secrets, so it skips pull requests from forks. The job can fail without
blocking the pull request. Its summary has the pass rates, the failed checks,
and the report. Its `eval-transcripts` artifact has each run's transcript and
plan files. Run the workflow by hand to pick another model.

## Cases

The cases check authoring only. When the agent offers a test run after
publishing, the simulated user turns it down, and `CommonChecks` checks that
`start_run` was never called. Cases that start runs, such as a retried
`start_run` or an approval whose deadline passes, are left for a future set
of run cases. Until then, the server's unit tests in
`tests/test_tool_requests.py` cover how `start_run` sends the idempotency key
and reports a run that the same key already started.

| Case | Module in `scenarios/` | What it checks |
|---|---|---|
| `release_notes_conversion` | `release_notes_conversion.py` | Converting the `release-notes` fixture skill: the conversion report, one design approval, no cycle, a human-input node, and a publish that saves a version only after a passing validation. |
| `rejected_approval` | `rejected_approval.py` | The README quickstart workflow: the approval's rejected output leads to the agent node that revises the draft, and the plan has the `reply` and `category` outputs. |
| `unsupported_loop` | `unsupported_loop.py` | Converting the `post-review` fixture skill, which repeats until the reviewer is happy: the report flags the loop and proposes a substitute, the plan has two review passes and no cycle, and nothing is published before the user decides. |
| `scheduled_edit` | `scheduled_edit.py` | Changing a flow that has an active version and a schedule: the agent names the schedule before it asks to activate, activates the new version with the action items only after the yes, and leaves the schedule alone. |
| `no_infrastructure` | `no_infrastructure.py` | A workflow that needs Zendesk and Slack, for a user with no remote MCP servers: the infrastructure check comes first, and the agent offers a version without those tools. |

## Adding a case

1. Write a module in `scenarios/`, for example `scenarios/rejected_approval.py`.
   Start from `scenarios/release_notes_conversion.py`. The module defines the
   prompt, the simulated user's rules in `USER`, a `checks` function that
   takes the `Outcome` and returns a list of `Check`, and `SCENARIO`, a
   `Scenario` with the case name, the `ScenarioInputs`, and `checks`.
2. Add `SCENARIO` to `SCENARIOS` in `dataset.py`.
3. Add tests in `tests/eval_harness/` that run the scenario's `checks` on a
   hand-written `Outcome`, with `FlowState` values for its flows: one that
   passes, and one for each way the agent can fail.
   `test_behavior_scenarios.py` has examples. These tests run in CI and catch
   a check that can never fail.
4. Run the case a few times with `--case <name> --repeat 3` and read the
   transcripts of the failures.

`ScenarioInputs` has these fields:

| Field | What it holds |
|---|---|
| `prompt` | The first message to the agent. Each `{flow_prefix}` in it is replaced with the run's prefix. Write it as `f"... {FLOW_PREFIX}{FLOW_NAME} ..."` with `FLOW_PREFIX` from `scenario.py`. |
| `user` | The simulated user's rules. |
| `files` | Files or directories to copy into the working directory, by their path there. |
| `setup` | A function that gets the `SandboxApi` and the run's flow prefix, and adds state to the sandbox before the agent starts. Every flow it creates must have a name that starts with the prefix. |
| `max_turns` | The most agent turns in the conversation. The default is 12. |

After each agent turn, the simulated user takes the first `Rule` that applies
to the agent's final message and sends its `reply`. A rule applies when its
`pattern` matches the message, when the agent has called its `after_tool`,
when it hasn't yet called its `before_tool`, and while the rule has been used
fewer than `max_uses` times. A rule whose `reply` is None ends the
conversation. Give each rule a `label`: `outcome.transcript.replies_labeled`
counts how often the user answered with it, for example how many times the
agent asked for the design approval. Put the rules with the narrowest
conditions first, and end with a general rule for questions the script didn't
expect. A case that publishes starts its rules with `DECLINE_TEST_RUN` from
`scenario.py`.

Write checks with the helpers in `assertions.py`. Give each check of a case a
name that no other check of the case, and no check in `CommonChecks`, has:
Pydantic Evals adds a number to a repeated name.

- For the plan: `check_no_cycle`, `check_has_node_kind`, `check_branches`,
  `check_plan_inputs`, and `check_plan_outputs`. `node_kinds`, `branches`,
  `plan_inputs`, and `plan_outputs` return the same facts for a check of
  your own.
- For the tool calls: `check_called` (with `times` and a `where` filter on
  arguments), `check_never_called`, `check_called_in_order`,
  `check_publish_succeeded`, `check_published_only_after_valid`, and
  `check_only_after_reply`, which checks that calls came after the simulated
  user's reply. Pass it `outcome.transcript.first_reply(label)`, and
  `activating_calls(calls)` to check activations.
- For the sandbox: `check_flow_saved`, which checks that a flow has a plan
  version. `outcome.flows` has each flow of the run by its name without the
  prefix, as a `FlowState`.
- For what the agent said: `check_text_mentions`, with a regular expression
  for each part you expect. `outcome.transcript.final_message(turn)` is the
  agent's last message of a turn. A reply's `turn` is the turn it answered.
