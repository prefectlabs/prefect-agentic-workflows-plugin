# Behavioral evaluations

The scenarios in `scenarios/` are pytest tests that run a real agent with the
`agentic-workflows` skill and this checkout's MCP server, against a Prefect
Cloud sandbox workspace. Each test checks what the agent did: the plan file it
wrote, the tools it called in order and with which arguments, and the flows it
left in the workspace.

Each run starts Claude Code through the Claude Agent SDK, so it costs money,
and the same scenario can pass once and fail the next time. The default
`pytest` run only collects `tests/`, so a plain `uv run pytest` doesn't run
them. The `Server` workflow runs the tests of the harness and of each
scenario's checks in `tests/eval_harness/`, which start no agent and call no
workspace.

## Running

The harness reads the sandbox from `PREFECT_API_URL` and `PREFECT_API_KEY`
and stops before any agent starts when either one is missing. The workspace
needs execution plans enabled and an object storage bucket. Use a sandbox
workspace: the scenarios create flows and a Secret block there.

The Claude Agent SDK includes its own copy of Claude Code. It uses
`ANTHROPIC_API_KEY` when it's set, and your Claude Code login otherwise. Run
from `server/`:

```sh
export PREFECT_API_URL=https://api.prefect.cloud/api/accounts/<id>/workspaces/<id>
export PREFECT_API_KEY=<a key for the sandbox>

uv run pytest evals                                                # every scenario, once
uv run pytest evals/scenarios/test_scheduled_edit.py               # one scenario
uv run pytest evals/scenarios/test_scheduled_edit.py --count 5     # a pass rate
uv run pytest evals --agent-model claude-sonnet-5                  # pick the model
uv run pytest evals --pass-rate-file pass-rate.md                  # also write the table as Markdown
```

`--count` comes from `pytest-repeat`. After the tests, pytest prints a
`pass rate` section with the passed and total runs of each scenario. A
failed test lists every failed check and the run's directory under pytest's
temporary directory. That directory has `transcript.json`, with every tool
call and message, and the agent's working directory in `agent/`. pytest keeps
the directories of the last three sessions. Pass `--basetemp <dir>` to put
them somewhere you choose.

The agent loads only the `prefect-agentic-workflows` server
(`strict_mcp_config`), only project settings, and only the `agentic-workflows`
skill (the SDK's `skills` option), so MCP servers and plugins from your own
Claude Code settings are not used. The server and the agent get the sandbox's
URL and key, and an empty `PREFECT_HOME`, so your Prefect profiles are not
used either.

## Flow names and cleanup

Each pytest session picks a prefix such as `eval-3f9a1c-`, and each scenario
run gets a numbered prefix inside it, such as `eval-3f9a1c-2-`, so a repeat
with `--count` doesn't find the flows of the run before it. A scenario's
prompt gives the agent the full flow name, and a check fails when the agent
calls `get_or_create_flow` with a name that doesn't start with the run's
prefix.

When the session ends, even after failures, the harness lists the flows whose
names start with the session's prefix with `POST /flows/filter` and deletes
each one. Deleting a flow also deletes its execution plan, its plan versions,
and its schedules. The harness never deletes a flow without the prefix. A
session that is killed before it ends, such as a cancelled CI job, leaves its
flows in the sandbox. Delete those by hand by their `eval-` prefix.

The `eval-github-token` Secret block, which the release-notes scenario
references, holds a placeholder value. The harness creates it when it's
missing and never deletes it.

## What a run does

1. Makes a working directory with the skill in
   `.claude/skills/agentic-workflows/` and the scenario's files.
2. Runs the scenario's `setup`, which adds state to the sandbox through the
   API, such as a flow with an active plan version and a schedule.
3. Sends the scenario's prompt to the agent with `ClaudeSDKClient`. After
   each turn, the simulated user answers the agent's final message from the
   scenario's rules, and the runner sends that answer in the same session.
   The conversation ends when a rule says so, when no rule applies, or after
   `max_turns` turns.
4. Reads the tool calls from the SDK's messages and the plan files from
   `workflows/`. Then it reads the state of each flow with the run's prefix
   into `outcome.flows`: the active version and its plan, the plan versions
   from oldest to newest, and the schedules. The scenario's checks read
   these, not the live API.

## CI

`.github/workflows/evals.yml` runs every scenario once with
`claude-haiku-5-5` on each pull request that changes `skills/` or `server/`.
It uses the `PREFECT_API_URL`, `PREFECT_API_KEY`, and `ANTHROPIC_API_KEY`
repository secrets, so it skips pull requests from forks. The job can fail
without blocking the pull request. Its summary has the pass-rate table, and
its `eval-transcripts` artifact has each run's transcript and plan files. Run
the workflow by hand to pick another model.

## Scenarios

The scenarios check authoring only. When the agent offers a test run after
publishing, the simulated user turns it down, and each scenario that
publishes checks that `start_run` was never called. Scenarios that start
runs, such as a retried `start_run` or an approval whose deadline passes, are
left for a future set of run scenarios. Until then, the server's unit tests
in `tests/test_tool_requests.py` cover how `start_run` sends the idempotency
key and reports a run that the same key already started.

| Module in `scenarios/` | What it checks |
|---|---|
| `test_release_notes_conversion.py` | Converting the `release-notes` fixture skill: the conversion report, one design approval, no cycle, a human-input node, and a publish that saves a version only after a passing validation. |
| `test_rejected_approval.py` | The README quickstart workflow: the approval's rejected output leads to the agent node that revises the draft, and the plan has the `reply` and `category` outputs. |
| `test_unsupported_loop.py` | Converting the `post-review` fixture skill, which repeats until the reviewer is happy: the report flags the loop and proposes a substitute, the plan has two review passes and no cycle, and nothing is published before the user decides. |
| `test_scheduled_edit.py` | Changing a flow that has an active version and a schedule: the agent names the schedule before it asks to activate, activates the new version with the action items only after the yes, and leaves the schedule alone. |
| `test_no_infrastructure.py` | A workflow that needs Zendesk and Slack, for a user with no remote MCP servers: the infrastructure check comes first, and the agent offers a version without those tools. |

## Adding a scenario

1. Write a module in `scenarios/` whose name starts with `test_`, for example
   `scenarios/test_rejected_approval.py`. Start from
   `scenarios/test_release_notes_conversion.py`. The module defines a
   `prompt(flow_prefix)` function that names each flow with the prefix, the
   simulated user's rules in `USER`, a `checks` function, and a test that
   takes the `run_scenario` and `flow_prefix` fixtures, runs the
   conversation, and passes the checks to `assert_passed`.
2. Add tests in `tests/eval_harness/` that run the scenario's `checks` on a
   hand-written `Outcome`, with `FlowState` values for its flows: one that
   passes, and one for each way the agent can fail.
   `test_behavior_scenarios.py` has examples. These tests run in CI and catch
   a check that can never fail.
3. Run the scenario a few times with `--count` and read the transcripts of
   the failures.

`run_scenario(prompt, USER, ...)` takes these keyword arguments:

| Argument | What it holds |
|---|---|
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
expect. A scenario that publishes starts its rules with `DECLINE_TEST_RUN`
from `scenario.py`.

Write checks with the helpers in `assertions.py`:

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
