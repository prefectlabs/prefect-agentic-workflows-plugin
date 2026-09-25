---
name: agentic-workflows
description: Build a Prefect Cloud agentic workflow (an execution plan) and publish, run, and schedule it with the prefect-agentic-workflows MCP server. Use when the user wants a workflow that runs in Prefect Cloud, from a description, from an existing skill, or from scratch, or wants to edit, roll back, run, or schedule an existing execution plan.
---

# Agentic workflows

An execution plan is a JSON graph of agent, human-input, and deployment nodes attached to a Prefect Cloud flow. You build one with the user, save it as `workflows/<flow-name>.plan.json` in their working directory, and publish it with the `prefect-agentic-workflows` MCP server tools. This skill is alpha, like the server.

## Pick the entry path

- **The user hands you an existing skill to convert**, as a `SKILL.md` path, a directory, an installed skill's name, or pasted text. Follow [references/conversion.md](references/conversion.md): read the whole skill, write the conversion report, and stop until the user decides every row. Then continue with the pipeline.
- **The user describes the workflow.** Map the description onto the checklist below. Fill every item the description answers, and fill the rest with the recommended default when it is safe. Then ask only about the gaps: items with no answer and no safe default. Name the defaults you chose in the summary. Continue with the pipeline.
- **The user gives you nothing to work from.** Run the interview: ask the checklist in one message, with the recommended default beside each item, so the user can answer "defaults are fine" to most of it. Ask a second round only about answers that left gaps. When the workflow has more than one branch, more than one approval, or touches more than two systems, offer the deeper interview in [references/interview.md](references/interview.md). Continue with the pipeline.

| Checklist item | Recommended default |
|---|---|
| Trigger: what starts a run | Started by hand. Add a schedule after a test run works. |
| Inputs: values each run needs | None. Hardcode constants in node objectives. |
| Steps: the work, in order | No default. Ask. |
| Tools and systems the steps touch | No default. Ask which remote MCP servers exist. |
| Human approval points | One approval before any step that writes to an outside system. |
| Outputs: what the run produces | The last node's output holds the final result. |
| Success: how the user knows it worked | The test run completes and the final result is what the user expected. |

## Pipeline

Finish each step before starting the next.

1. **Summary.** Write a plain-language summary of the nodes, branches, human checkpoints, tools and Secret blocks, inputs, and outputs. Give the reason for each node boundary, using the node-splitting rule below. For a converted skill, include the step map from the conversion report, with the user's decision for each row. The step is done when the user confirms the summary. Revise it until they do.
2. **Draft.** Read [references/design-guidance.md](references/design-guidance.md) and [references/platform-limits.md](references/platform-limits.md). Start from the closest plan in [references/example-plans.md](references/example-plans.md). Call `get_schema` and write the plan against the `schema_version` it returns. Call `list_secret_blocks` for every credential the plan needs, and follow [references/secret-blocks.md](references/secret-blocks.md) when one is missing. Write the plan to `workflows/<flow-name>.plan.json`, with no `layout`.
3. **Validate.** Call `validate_plan` with the file's contents. Fix the file and validate again. The step is done when `valid` is true and `errors` is empty. When an error and the reference files disagree, the error is right.
4. **Publish.** Call `get_or_create_flow` with the flow name. When `created` is false, call `get_plan` for the flow. If its `active_version` is not null, tell the user what changes from the active plan and ask whether to activate the new version now. Call `publish_plan` with the flow ID and the file's contents. Set `activate` to false when the user wants to review the version first. When the result has `errors`, fix the file and go back to step 3. When it has `activation_error`, explain it, and call `activate_plan_version` once the cause is fixed.
5. **Test run.** A run uses the flow's active version. Offer a test run. After the user says yes, ask for any input values, call `start_run`, and watch the run with `get_run` and a `wait_seconds` of 30 until its status is `completed`, `failed`, `cancelled`, or `blocked`. When a node waits for human input, show the user its form in plain words and pass their answer to `submit_human_input`. When the run is `blocked` or fails, stop polling, read `diagnostics` and each node's `failure`, and tell the user what you found. Read results with `get_run_output`: a plan output by name, or a node's output with its `activation_id`.
6. **Report.** Give the user the flow and run links, the run's results, and every platform limit that still affects the workflow. For a converted skill, check the published plan against the step map, and name any source step that is not in the plan and the decision that left it out. Build the links from the API URL in the user's Prefect profile (`prefect config view`): replace `https://api.prefect.cloud/api/accounts/<account>/workspaces/<workspace>` with `https://app.prefect.cloud/account/<account>/workspace/<workspace>`, then add `/flows/flow/<flow_id>` or `/runs/flow-run/<flow_run_id>`. When the profile is not available, give the IDs.

## Node-splitting rule

Start a new node only at one of these points:

- a branch point, where a decision leads to different next steps
- a human approval
- a change of tool set or credentials
- a deterministic step, which becomes a Deployment node
- a split forced by the 60-second agent node limit

Everything else stays in one agent node.

## Rules

- **Confirm before these actions:** activating a version on a flow that has an active plan (`publish_plan` with `activate` true, or `activate_plan_version`), starting a run (`start_run`), and deleting a schedule (`delete_schedule`). Ask, and act only on an explicit yes. The first activation on a flow with no active plan needs no extra confirmation, because the user confirmed the summary.
- **The user answers every human-input form.** Relay the form, wait, and submit only the answer the user gives, even in a test run.
- **Credentials are Secret block references.** A plan refers to a credential by the block's ID from `list_secret_blocks`. Secret values stay out of the conversation: the user creates the block, and you never ask for or handle the value.

## After the first publish

- To edit a workflow, change `workflows/<flow-name>.plan.json` and run the pipeline from step 3. When the file isn't there, write it from the `plan` that `get_plan` returns. Plan versions are immutable, so each publish creates a new version.
- To roll back, call `list_plan_versions`, then `activate_plan_version` after the user confirms.
- To schedule a workflow, use `create_schedule`, `list_schedules`, `get_schedule`, and `update_schedule`. A schedule runs the version that is active when it fires.
