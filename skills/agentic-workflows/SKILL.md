---
name: agentic-workflows
description: Build a Prefect Cloud agentic workflow (an execution plan) and publish, run, and schedule it with the prefect-agentic-workflows MCP server. Use when the user wants a workflow that runs in Prefect Cloud, from a description, from an existing skill, or from scratch, or wants to edit, roll back, run, or schedule an existing execution plan.
---

# Agentic workflows

An execution plan is a JSON graph of agent, human-input, and deployment nodes attached to a Prefect Cloud flow. You build one with the user, save it as `workflows/<flow-name>.plan.json` in their working directory, and publish it with the `prefect-agentic-workflows` MCP server tools. This skill is alpha, like the server.

## Pick the entry path

Every path runs the infrastructure check in [references/infrastructure-check.md](references/infrastructure-check.md) before its first design question. The check tells the user which business tools the workflow can reach, and what that limits, before they spend time on the design.

- **The user hands you an existing skill to convert**, as a `SKILL.md` path, a directory, an installed skill's name, or pasted text. Follow [references/conversion.md](references/conversion.md): read the whole skill, run the infrastructure check, and write the conversion report. Then continue with the pipeline. The user decides the report's rows in the summary step.
- **The user describes the workflow.** Run the infrastructure check. Then map the description onto the checklist below. Fill every item the description answers, and fill the rest with the recommended default when it is safe. Then ask only about the gaps: items with no answer and no safe default. Name the defaults you chose in the summary. Continue with the pipeline.
- **The user gives you nothing to work from.** Run the infrastructure check, then the interview: ask the checklist in one message, with the recommended default beside each item, so the user can answer "defaults are fine" to most of it. Ask a second round only about answers that left gaps. When the workflow has more than one branch, more than one approval, or touches more than two systems, offer the deeper interview in [references/interview.md](references/interview.md). Continue with the pipeline.

| Checklist item | Recommended default |
|---|---|
| Trigger: what starts a run | Started by hand. Add a schedule after a test run works. |
| Inputs: values each run needs | A value the user changes between runs is a plan input, such as the repository to check. A fixed instruction stays in the node objective, such as "reply in under 100 words". |
| Steps: the work, in order | No default. Ask. |
| Tools and systems the steps touch | The systems the infrastructure check found reachable. |
| Human approval points | One approval before any step that writes to an outside system. |
| Outputs: what the run produces | One plan output, `result`, taken from the last node's output. |
| Success: how the user knows it worked | The test run completes and the final result is what the user expected. |

## Pipeline

Finish each step before starting the next.

1. **Summary.** Write a plain-language summary of the nodes, branches, human checkpoints, tools and Secret blocks, inputs, and outputs. Give the reason for each node boundary, using the node-splitting rule below. For a converted skill, show the conversion report with the summary, and include the step map with the proposed substitute for each row. Ask the user to decide each row and confirm the summary in the same reply. Revise the summary and the step map until they do. This is the design approval. The step is done when the user confirms the summary and every row of the step map has their decision.
2. **Draft.** Read [references/design-guidance.md](references/design-guidance.md) and [references/platform-limits.md](references/platform-limits.md). Start from the closest plan in [references/example-plans.md](references/example-plans.md). Call `get_schema`. If its `supported_schema_versions` lists a newer version than the `schema_version` it returned, call `get_schema` again with that version. Write the plan against the newest supported version. Call `list_secret_blocks` for every credential the plan needs, and follow [references/secret-blocks.md](references/secret-blocks.md) when one is missing. Write the plan to `workflows/<flow-name>.plan.json`, with no `layout`.
3. **Validate.** Call `validate_plan` with the file's contents. Fix the file and validate again. The step is done when `valid` is true and `errors` is empty. When an error and the reference files disagree, the error is right.
4. **Publish.** Call `get_or_create_flow` with the flow name. When `created` is false, call `get_plan` for the flow. If its `active_version` is not null, activating the new version is a promotion: get the promotion approval, and tell the user what changes from the active plan. Call `publish_plan` with the flow ID and the file's contents. Set `activate` to false when the user wants to save the version without activating it. When the result has `errors`, fix the file and go back to step 3. When it has `activation_error`, explain it, and call `activate_plan_version` for the same version once the cause is fixed. That call activates the version the user already approved, so the same approval covers it.
5. **Test run.** Offer a test run, and follow [Starting a run](#starting-a-run). The run uses the flow's active version. Watch the run with `get_run` and a `wait_seconds` of 30 until its status is `completed`, `failed`, `cancelled`, or `blocked`. When a node waits for human input, show the user its form in plain words and pass their answer to `submit_human_input`. When the run is `blocked` or fails, stop polling, read `diagnostics` and each node's `failure`, and tell the user what you found. Read results with `get_run_output`: a plan output by name, or a node's output with its `activation_id`.
6. **Report.** Give the user the flow and run links, the plan version the run used, the run's results, and every platform limit that still affects the workflow. The version is `snapshot.execution_plan_version_id` in the `get_run` result. Find that ID with `list_plan_versions` and give the user its version number. For a converted skill, check the published plan against the step map, and name any source step that is not in the plan and the decision that left it out. Build the links from the API URL in the user's Prefect profile (`prefect config view`): replace `https://api.prefect.cloud/api/accounts/<account>/workspaces/<workspace>` with `https://app.prefect.cloud/account/<account>/workspace/<workspace>`, then add `/flows/flow/<flow_id>` or `/runs/flow-run/<flow_run_id>`. When the profile is not available, give the IDs.

## Node-splitting rule

Start a new node only at one of these points:

- a branch point, where a decision leads to different next steps
- a human approval
- a change of tool set or credentials
- a deterministic step, which becomes a Deployment node
- a split forced by the 60-second agent node limit

Everything else stays in one agent node.

## Approval points

Ask for the user's approval at these four points, and at no others. Act only on an explicit yes. Each yes covers the one action you described.

1. **Design.** The user confirms the summary in pipeline step 1. This also covers activating a version on a flow with no active version, whether through `publish_plan` or `activate_plan_version`.
2. **External effects.** Every `start_run`, including a test run, because agent and deployment nodes can act on outside systems. Say which outside systems the run can change, and give the parameters.
3. **Promotion.** Activating a version on a flow that already has an active version: `publish_plan` with `activate` true, or `activate_plan_version`, including a rollback. First call `list_schedules`, and tell the user which schedules will start runs of the new version.
4. **Recurring runs.** Every `create_schedule`, `update_schedule`, and `delete_schedule`. First state the schedule in plain words, the next run time with its time zone, and the parameters. For a change or a deletion, read the current schedule with `get_schedule` and state it too.

## Starting a run

A run always uses the flow's active version. When the user saved the new version without activating it, tell them the test runs the active version and not the new one, and name the active version. When the flow has no active version, `start_run` fails, so activate a version first.

1. Ask for any input values, then get the external-effects approval with those values.
2. Create an idempotency key for this run, such as a new UUID, and keep it until the run has started.
3. Call `start_run` with the key.
4. When the call fails with no clear result, such as a timeout or a lost response, call `start_run` again with the same key and the same parameters. Use a new key only for a new run the user approved. A result with `created` false means the first call had already started the run, and `flow_run_id` is that run.

## Rules

- **The user answers every human-input form.** Relay the form, wait, and submit only the answer the user gives, even in a test run.
- **Credentials are Secret block references.** A plan refers to a credential by the block's ID from `list_secret_blocks`. Secret values stay out of the conversation: the user creates the block, and you never ask for or handle the value.

## After the first publish

- To edit a workflow, change `workflows/<flow-name>.plan.json` and run the pipeline from step 3. When the file isn't there, write it from the `plan` that `get_plan` returns. Plan versions are immutable, so each publish creates a new version.
- To roll back, call `list_plan_versions`, get the promotion approval, and call `activate_plan_version`.
- To schedule a workflow, get the recurring-runs approval, then use `create_schedule`. Read schedules with `list_schedules` and `get_schedule`, and change or pause one with `update_schedule`. A schedule runs the version that is active when it fires.
