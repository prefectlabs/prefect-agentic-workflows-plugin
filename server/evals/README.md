# Behavioral evaluations

The harness in this directory runs a real agent with the `agentic-workflows`
skill and this checkout's MCP server, against a fake Prefect Cloud API. Then it
checks what the agent did: the plan file it wrote, and the tools it called in
order and with which arguments.

Each run starts Claude Code, so it costs money, and the same scenario can pass
once and fail the next time. CI doesn't run it. CI runs the tests of the fake
and of the checks in `tests/eval_harness/`, which start no agent.

## Running

You need Claude Code (`claude`) on your `PATH`, signed in. Run from `server/`:

```sh
uv run python -m evals --list                               # list the scenarios
uv run python -m evals release-notes-conversion             # run one scenario once
uv run python -m evals release-notes-conversion --repeat 5  # report a pass rate
```

The command prints a table with each scenario's runs, passes, and pass rate,
then every failed check. It saves each run's transcript, check results, and
plan files in `evals/results/<time>/<scenario>/attempt-<n>/`. Pass
`--keep-workspaces` to keep each run's working directory, `--model` to pick the
model, and `--agent` to use a different `claude` command.

No run calls Prefect Cloud. The fake listens on 127.0.0.1, and the runner
points the server and the agent at it with `PREFECT_API_URL`, a fake API key,
and an empty `PREFECT_HOME`. The agent loads only the `prefect-agentic-workflows`
server (`--strict-mcp-config`) and only project settings, so MCP servers and
plugins from your own Claude Code settings are not used.

## What a run does

1. Makes a temporary working directory with the skill in
   `.claude/skills/agentic-workflows/` and the scenario's files.
2. Starts a new `FakeCloud` from `fake_cloud.py`, and lets the scenario add
   state to it, such as Secret blocks, an existing flow, or node scripts.
3. Sends the scenario's `prompt` to `claude -p`. After each turn, the simulated
   user answers the agent's final message from the scenario's rules, and the
   runner resumes the same session with that answer. The conversation ends when
   a rule says so, when no rule applies, or at the scenario's `max_turns`.
4. Reads the tool calls from the agent's stream-json output and the plan files
   from `workflows/`, then runs the scenario's checks.

## The fake Cloud API

`FakeCloud` keeps flows, plan versions and the active version, schedules,
Secret blocks, and runs. `validate_plan` checks the plan's shape against the
copy of the schema in `schemas/`, then runs the graph checks: edges that point
at real plan inputs, nodes, and ports, no cycles, `exactly_one` output
selection on nodes other than agent nodes, and a `decision` enum on a
human-input node with more than one response output.

A run moves one node forward each time the agent reads it with `get_run`:

- An agent, deployment, or timer node completes with its first output and a
  value built from that output's schema.
- A human-input node waits with its form until `submit_human_input` answers
  it. Then it selects the output that the answer's `decision` names.
- A node whose inputs can no longer arrive is skipped.

Change this for a node with a `NodeScript` in `fake.scripts`, by node ID. It
can set the output and the value, make the node fail, or make a human-input
node's deadline pass.

## Adding a scenario

1. Write a module in `scenarios/`, for example `scenarios/rejected_approval.py`,
   that defines `SCENARIO = Scenario(...)`. Start from
   `scenarios/release_notes_conversion.py`.
2. Add `SCENARIO` to the list in `scenarios/__init__.py`.
3. Add a test in `tests/eval_harness/test_scenarios.py` that runs the
   scenario's `checks` on a hand-written `Outcome`: one that passes, and one
   for each way the agent can fail. This test runs in CI and catches a check
   that can never fail.
4. Run the scenario a few times with `--repeat` and read the transcripts of
   the failures.

A `Scenario` has these fields:

| Field | What it holds |
|---|---|
| `name`, `description` | The name to run it by, and one line for `--list`. |
| `prompt` | The user's first message. |
| `user` | The simulated user's `Rule`s, in order. |
| `checks` | A function from `Outcome` to a list of `Check`s. |
| `files` | Files or directories to copy into the working directory, by their path there. |
| `setup` | A function that adds state to the `FakeCloud` before the agent starts. |
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
expect.

Write checks with the helpers in `assertions.py`:

- For the plan: `check_no_cycle`, `check_has_node_kind`, `check_branches`,
  `check_plan_inputs`, and `check_plan_outputs`. `node_kinds`, `branches`,
  `plan_inputs`, and `plan_outputs` return the same facts for a check of
  your own.
- For the tool calls: `check_called` (with `times` and a `where` filter on
  arguments), `check_never_called`, `check_called_in_order`, and
  `check_published_only_after_valid`.
- For what the agent said: `check_text_mentions`, with a regular expression
  for each part you expect.

`outcome.fake` has the state the agent left in the fake, such as
`fake.runs[run_id].responses`, the answers it submitted to each form.
