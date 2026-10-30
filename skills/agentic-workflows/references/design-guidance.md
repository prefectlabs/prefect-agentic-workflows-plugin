# Design guidance

`get_schema` is the source of truth for field names and shapes. This file covers how to use them well and what the platform allows.

## A plan is a graph of outcomes

Each node makes one scoped decision and selects a named output, and edges leave from named outputs, so the selected output decides what runs next. Name outputs for the outcome they mean (`approved`, `summary_ready`), and keep the names: edges, plan outputs, and run history refer to them.

Every port has a JSON Schema. `validate_plan` checks schema compatibility only for plan outputs, so check yourself that each edge's target input accepts what its source output allows. Small object schemas with `required` fields work best. Leave out `layout`.

## Node kinds

- **AgentNode.** Write the `objective` as the decision: what to read, which tools to use, and which output to select with which fields. Give it only the MCP servers it needs. Use `output_selection` `exactly_one` for a branch, and `one_or_more` or `zero_or_more` for a fan-out where several next steps run.

  Give every agent node an output for when it can't do its job, such as `blocked` with a `reason`, and say in the objective when to select it. With only success outputs, an agent that hits a missing permission or bad input writes its error text into a success output, and the next node treats it as real data. An output with no outgoing edge is valid; selecting it ends that branch.

  Bound every list or search in the objective: a date range, a small page size (30 or fewer), and the tool's field selector when it has one. One unbounded list call can add over 100,000 tokens to the agent's context.

  Have the agent compute exact facts with a script. Which items fall inside a range, a count, and a length limit are facts, not judgment. Tell the agent to write and run a short script for each one while it works, and to use the script's answer. A rule given only in prose, such as "leave out the commit the range starts from", tends to break again on a later run.

  When a later node checks an agent's result, put the same limits in that agent's objective, and have it measure its result with a script before it selects an output. Aim a little inside the check's limit. Otherwise the agent misses a limit it never saw, and the run ends on the check's failure branch.
- **HumanInputNode.** Put the question in `human_input.form_schema`. With several outputs, the form needs a required `decision` property whose choices are the output names. A `deadline` with `on_expiry` selects an output when nobody answers, and that output isn't a `decision` choice.
- **DeploymentNode.** Runs a deployment by `deployment.id`, with `wait` set to `{"for": "child_flow_run", "until": "terminal"}`. Pass run parameters through an input named `parameters`. `get_deployment` shows the parameters and their defaults. Declare an output per end state to route on, such as `completed` or `failed`.

Use a Deployment node for work that must run the same way every time. When a later node needs that work's result, not only whether it succeeded, expose the work as a tool on a remote MCP server instead.

## Checking an agent's result

Before an approval or an outside write, a second agent node can check the first one's result and select `passed` or `failed`. It catches made-up links, wrong ranges, and empty results before a person spends time reviewing them. Have it run scripted checks for the mechanical rules, and use MCP tools for checks against the source system. Write each check to fail on empty input, since "no links, so no bad links" passes an empty result. Its outputs carry only its findings.

Route each value from the node that produced it. Every later node that needs the draft, including the approval and the node that publishes it, takes it on an edge from the drafting node. The check's `passed` output joins as a second required input, so those nodes run only after the check passes. Don't ask an agent to pass a value through unchanged when a later node can take it on an edge instead: it can shorten or replace the value, and the later nodes then act on what it wrote.

A plan output is the exception. Its fields take whole node outputs and can't wait on an approval, so a value that depends on a decision needs a node on that path to produce it. Keep that node copy-only, and also expose the original as a plan output so the reviewed version is on record. [customer-feedback-reply.plan.json](examples/customer-feedback-reply.plan.json) does this with `category`.

For a join after a fan-out, set `orchestration.evaluate_when` to `all_reachable_terminal` and take the branch results through a `list` or `source_map` input.

## Platform limits

Last verified: 2026-10-27, against the Prefect Cloud API. When an error from `validate_plan` or `publish_plan` disagrees with this list, follow the error and tell the user this file may be out of date.

- Plans run only in Prefect Cloud. Versions are immutable.
- Set `model` on every agent node: a model through one of the workspace's provider connections, with an optional `effort`. [Models](../SKILL.md#models) says how to pick one.
- An agent node can run shell commands and read and write files while it works, but nothing it writes carries over to later nodes; pass results on through outputs. `runtime.env` sets environment variables for the agent, and a name that looks like a credential must be a Secret block reference.
- An agent doesn't know the current date. Pass dates as plan inputs, or tell the agent to run `date -u` before any date math. Otherwise it guesses a date and reads the wrong time window.
- Don't write a JSON `null` anywhere in a plan, such as an `on_expiry.value`. Use `{}` or `""`, with an output schema of the matching type.
- Tools come only from remote MCP servers over Streamable HTTP, not stdio and not `/sse` URLs. Reference a server through its MCP connection (`"type": "connection"`), as [mcp-connections.md](mcp-connections.md) shows. An inline server (`"type": "http"`) is for an account without MCP connections.
- An inline server URL has no credentials, query string, fragment, or `;` parameters. A sensitive header or query value, such as `Authorization` or any name containing `token`, `secret`, `password`, or `apikey`, must be `{"$ref": {"block_document_id": "<id>"}}`.
- No cycles and no mapping: a loop becomes a fixed number of steps or a human checkpoint, and a list is handled inside one node or by a fixed fan-out.
- Node `orchestration.on_failure.retry` and `orchestration.limits` validate, but the runtime doesn't apply them yet, so a failed agent attempt isn't retried. Use plan-level `max_active_nodes` and `max_active_child_flow_runs` to limit concurrency.
- Timer nodes and `evaluate_when` set to `manual` can't be activated. A root node can't use `any_upstream_terminal`.
- Human-input and Deployment nodes use `output_selection` `exactly_one`. Human-input nodes can't retry.
- A Deployment node passes on only the child run's ID and state. The user who publishes and runs the plan needs permission to run the deployment, which seeing it doesn't prove, so name this in the summary.
- A plan that validates can still fail to publish, for example when an MCP server's hostname doesn't resolve or you can't see a referenced Secret block or MCP connection.
- Another user can activate a different version between your check and your activation or run.

## Examples

Both use schema version 0.2, which adds plan `outputs`. Replace all-zero placeholder IDs before publishing.

- [examples/customer-feedback-reply.plan.json](examples/customer-feedback-reply.plan.json): an agent drafts a reply, a person approves or rejects it with notes, and a rejection gets one revision. No MCP server, Secret block, or deployment. Start here for approvals and branches.
- [examples/single-agent-with-mcp.plan.json](examples/single-agent-with-mcp.plan.json): one agent node uses an inline remote MCP server whose `Authorization` header references a Secret block holding `Bearer <token>`. Start here for tools, and replace the inline server with a connection reference when the workspace has MCP connections.
