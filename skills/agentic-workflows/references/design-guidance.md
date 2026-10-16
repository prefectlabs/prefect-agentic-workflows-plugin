# Design guidance

`get_schema` is the source of truth for field names and shapes. This file covers how to use them well and what the platform allows.

## A plan is a graph of outcomes

Each node makes one scoped decision and selects a named output, and edges leave from named outputs, so the selected output decides what runs next. Name outputs for the outcome they mean (`approved`, `summary_ready`), and keep the names: edges, plan outputs, and run history refer to them.

Every port has a JSON Schema. `validate_plan` checks schema compatibility only for plan outputs, so check yourself that each edge's target input accepts what its source output allows. Small object schemas with `required` fields work best. Leave out `layout`.

## Node kinds

- **AgentNode.** Write the `objective` as the decision: what to read, which tools to use, and which output to select with which fields. Give it only the MCP servers it needs. Use `output_selection` `exactly_one` for a branch, and `one_or_more` or `zero_or_more` for a fan-out where several next steps run.

  Give every agent node an output for when it can't do its job, such as `blocked` with a `reason`, and say in the objective when to select it. With only success outputs, an agent that hits a missing permission or bad input writes its error text into a success output, and the next node treats it as real data. An output with no outgoing edge is valid; selecting it ends that branch.

  Bound every list or search in the objective: a date range, a small page size (30 or fewer), and the tool's field selector when it has one. One unbounded list call can add over 100,000 tokens to the agent's context.
- **HumanInputNode.** Put the question in `human_input.form_schema`. With several outputs, the form needs a required `decision` property whose choices are the output names. A `deadline` with `on_expiry` selects an output when nobody answers, and that output isn't a `decision` choice.
- **DeploymentNode.** Runs a deployment by `deployment.id`, with `wait` set to `{"for": "child_flow_run", "until": "terminal"}`. Pass run parameters through an input named `parameters`. `get_deployment` shows the parameters and their defaults. Declare an output per end state to route on, such as `completed` or `failed`.

Use a Deployment node for work that must run the same way every time. When a later node needs that work's result, not only whether it succeeded, expose the work as a tool on a remote MCP server instead.

For a join after a fan-out, set `orchestration.evaluate_when` to `all_reachable_terminal` and take the branch results through a `list` or `source_map` input.

## Platform limits

Last verified: 2026-10-16, against the Prefect Cloud API. When an error from `validate_plan` or `publish_plan` disagrees with this list, follow the error and tell the user this file may be out of date.

- Plans run only in Prefect Cloud. The account needs execution plans enabled and the workspace needs an object storage bucket. Versions are immutable.
- Without `model`, an agent node runs Prefect Cloud's default model. `model` selects a model through one of the workspace's provider connections, with an optional `effort`.
- Each agent node attempt has its own sandbox, discarded when the attempt ends. The agent can run shell commands and read and write files there. `runtime.env` sets environment variables in it, and a name that looks like a credential must be a Secret block reference.
- An agent doesn't know the current date. Pass dates as plan inputs, or tell the agent to run `date -u` in its sandbox before any date math. Otherwise it guesses a date and reads the wrong time window.
- Don't write a JSON `null` anywhere in a plan. Publishing drops null values, and a version whose stored plan no longer validates, such as one whose `on_expiry.value` was `null`, starts runs that never progress and can't be cancelled. Use `{}` or `""`, with an output schema of the matching type.
- Tools come only from remote MCP servers over Streamable HTTP (`"type": "http"`), not stdio and not `/sse` URLs. A server URL has no credentials, query string, fragment, or `;` parameters.
- A sensitive header or query value, such as `Authorization` or any name containing `token`, `secret`, `password`, or `apikey`, must be `{"$ref": {"block_document_id": "<id>"}}`.
- No cycles and no mapping: a loop becomes a fixed number of steps or a human checkpoint, and a list is handled inside one node or by a fixed fan-out.
- Timer nodes and `evaluate_when` set to `manual` can't be activated. A root node can't use `any_upstream_terminal`.
- Human-input and Deployment nodes use `output_selection` `exactly_one`. Human-input nodes can't retry.
- A Deployment node passes on only the child run's ID and state. The user who publishes and runs the plan needs permission to run the deployment, which seeing it doesn't prove, so name this in the summary.
- Publishing checks that MCP hostnames resolve and that the publisher can see every referenced Secret block, so a plan that validates can still fail to publish.
- At most 32 plan outputs, with 256 fields across them.
- Another user can activate a different version between your check and your activation or run.

## Examples

Both use schema version 0.2, which adds plan `outputs`. Replace all-zero placeholder IDs before publishing.

- [examples/customer-feedback-reply.plan.json](examples/customer-feedback-reply.plan.json): an agent drafts a reply, a person approves or rejects it with notes, and a rejection gets one revision. No MCP server, Secret block, or deployment. Start here for approvals and branches.
- [examples/single-agent-with-mcp.plan.json](examples/single-agent-with-mcp.plan.json): one agent node uses a remote MCP server whose `Authorization` header references a Secret block holding `Bearer <token>`, and retries once on failure. Start here for tools and credentials.
