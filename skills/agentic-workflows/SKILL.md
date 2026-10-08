---
name: agentic-workflows
description: Build a Prefect Cloud agentic workflow (an execution plan) and publish, run, and schedule it with the prefect-agentic-workflows MCP server. Use when the user wants a workflow that runs in Prefect Cloud, from a description, from an existing skill, or from scratch, or wants to edit, roll back, run, or schedule an existing execution plan.
---

# Agentic workflows

An execution plan is a JSON graph of agent, human-input, and deployment nodes attached to a Prefect Cloud flow. You design one with the user, save it as `workflows/<file-name>.plan.json` in their working directory, and publish it with the `prefect-agentic-workflows` MCP tools. Both are alpha.

## Start

When building or converting a workflow, or editing one to use a new system, run the [infrastructure check](references/infrastructure-check.md) first. Running, rolling back, or scheduling a published workflow skips it. Then:

- **Converting an existing skill:** follow [references/conversion.md](references/conversion.md).
- **The user describes the workflow:** fill the checklist below from the description and the defaults, and ask only about the gaps.
- **The user gives you nothing:** ask the checklist in one message, with each default beside its item. For a workflow with several branches, approvals, or systems, also ask per step what it decides, what it reads and hands on, and what happens when it fails or nobody answers.

| Checklist item | Default |
|---|---|
| Trigger | Started by hand. Schedule it after a test run works. |
| Inputs | A value that changes between runs is a plan input. A fixed instruction stays in the node objective. |
| Steps | Ask. |
| Tools | The systems the infrastructure check found reachable. |
| Approvals | One before any step that writes to an outside system. |
| Outputs | One plan output, `result`, from the last node. |

## Pipeline

1. **Summary.** Describe the nodes, branches, approvals, tools, Secret blocks, inputs, and outputs in plain language, with the reason for each node boundary. For a conversion, include the conversion report. The step is done when the user confirms it: the design approval.
2. **Draft.** Read [references/design-guidance.md](references/design-guidance.md) and start from its closest example. Call `get_schema`, and when `supported_schema_versions` lists a newer version, call it again with that version and write against it. Name the file after the flow, with every character other than letters, digits, `-`, and `_` replaced by `-`. Ask before overwriting a file that belongs to another flow.
3. **Validate.** Call `validate_plan` and fix the file until `valid` is true. When an error and the reference files disagree, the error is right.
4. **Publish.** Call `get_or_create_flow`, then `get_plan`. When the flow has an active version, activating the new one is a promotion. Call `publish_plan`, with `activate` false when the user only wants to save. A fix that changes the design goes back to step 1. A fix that only changes the document's shape goes back to step 3.
5. **Test run.** Offer one, and follow [Running](#running).
6. **Report.** Give the flow link, the published version ID, any run's results from `get_run_output` and the version it used (`snapshot.execution_plan_version_id`), and every platform limit that still affects the workflow. Build links from `get_workspace`. For a conversion, name every source step that isn't in the plan and why.

## Node-splitting rule

Start a new node only at a branch, a human approval, a change of tools or credentials, a deterministic step (a Deployment node), or a split forced by the 60-second agent limit. Everything else stays in one agent node.

## Approvals

Ask at these four points only, and act only on an explicit yes:

1. **Design**: the summary. It also covers the first activation on a flow.
2. **External effects**: every `start_run`. Say which outside systems the run can change. A user message that asks for the run counts, when every input value is known.
3. **Promotion**: activating over an existing active version, including a rollback. Say what changes, and which schedules from `list_schedules` will run the new version.
4. **Recurring runs**: every schedule create, update, or delete. State the schedule in plain words with its time zone and parameters, reading an existing one with `get_schedule` first.

## Running

Read the active plan with `get_plan`, collect a value for each required plan input, and include them in the external-effects approval. A run always uses the active version, so when the new version was saved without activating or failed to activate, tell the user which version the test would run. Create one idempotency key per run the user approved, and reuse it when a `start_run` call fails without a clear result. Watch with `get_run` and `wait_seconds` of 30 until the status is `completed`, `failed`, `cancelled`, or `blocked`.

The user answers every human-input form: show it in plain words and submit only their answer. When they ask to leave it unanswered, for example to test its expiry, submit nothing, stop watching, and tell them to come back after the form's `deadline_at`.

## Credentials

A plan references a credential by a Secret block's ID from `list_secret_blocks`. The block holds the whole header value, so for a bearer token it's `Bearer <token>`. When the block is missing, ask the user to create a **Secret** block in the Prefect Cloud UI under **Blocks**, or with `prefect block create secret`, then list the blocks again. Never ask the user to type a secret value. If they paste one, keep it out of the plan and tell them to store it in a block and rotate it.

## Later changes

Find a published workflow with `get_flow`, which never creates one.


- **Edit:** start from the active plan from `get_plan`. When the local plan file differs from it, ask the user which to build on. Then run the pipeline from step 1 with a summary of what changes.
- **Roll back:** pick a version with `list_plan_versions`, compare it with the active plan using `get_plan` with and without its `version_id`, get the promotion approval, and call `activate_plan_version`.
- **Schedule:** when creating a schedule or replacing its parameters, collect a value for each required input of the active plan. Get the recurring-runs approval, then use the schedule tools. A schedule runs whatever version is active when it fires.
