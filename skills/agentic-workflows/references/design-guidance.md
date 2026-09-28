# Design guidance

Read this before you draft a plan. `get_schema` is the source of truth for field names and shapes. This file explains how to use them well, and [example-plans.md](example-plans.md) has complete plans that use them.

## A plan is a graph of outcomes

Each node makes one scoped decision and selects a named output. Edges leave from a named output, so the output a node selects decides which nodes run next. A branch is a node with one output per outcome, for example `needs_review` and `ok`, and one edge from each.

Name each output for the outcome it means (`approved`, `rejected`, `summary_ready`), not for how the node reached it.

## Document shape

- `inputs`: the values a caller passes to each run, each with a JSON Schema and `required`.
- `nodes`: a map from node ID to node. Every node has `kind`, `inputs`, `outputs`, and `orchestration`.
- `edges`: each edge has an `id`, a `from` (a plan input, or a node's named output), and a `to` (a node's named input).
- `outputs`: named plan outputs, built from node outputs. Only schema versions that declare `outputs` accept them.
- Leave out `layout`. Plans in this workflow never set node coordinates.

## Typed outputs are the contract between nodes

Every output port and input port has a JSON Schema. The downstream input schema must accept what the upstream output schema allows. `validate_plan` does not check this for edges between nodes. It checks it only for plan outputs, so compare the two schemas yourself for every edge. Use small object schemas with `required` fields, so each node states exactly what it hands on and the next node's objective can name those fields.

## Output names are durable

Edges, plan outputs, and run history refer to outputs by name. Renaming an output breaks routes and makes old runs harder to compare, so choose names you can keep, and add a new output instead of renaming an old one.

## Node kinds

- **AgentNode.** Write the `objective` as the decision: what to read from each input, what to do with which tools, and which output to select with which fields. Give it only the MCP servers it needs. Use `output_selection` `exactly_one` for a branch, and `one_or_more` or `zero_or_more` for a fan-out where several next steps can run.
- **HumanInputNode.** Put the question in `human_input.form_schema`. With one output, the form is a plain answer. With more than one output, the form needs a required top-level `decision` property whose string choices match the names of the outputs a person can choose, which excludes the expiry output. A `deadline` with `on_expiry` selects an output when nobody answers in time. The expiry output must not be one of the `decision` choices.
- **DeploymentNode.** Runs an existing Prefect deployment by `deployment.id`, with `wait` set to `{"for": "child_flow_run", "until": "terminal"}`. Pass run parameters through an input named `parameters`, whose object is merged over the deployment's default parameters. Declare an output named after each end state you want to route on, such as `completed` or `failed`. The selected output carries the child run's ID and state only.

## Use deployments for deterministic work

Work that must run the same way every time, like a script, a data load, or a build, belongs in a Deployment node. An agent node costs a model call, has a 60-second limit, and can vary between runs. When a later node needs the result of deterministic work, and not only its success or failure, expose that work as a tool on a remote MCP server and call it from an agent node.

## Readiness and joins

`orchestration.evaluate_when` sets when a node is ready. Check the schema for the current values. In practice:

- `all_required_inputs_produced` fits most nodes in a chain.
- A join after a fan-out waits for every branch that can still run, with `all_reachable_terminal`, and takes the branch results through a `list` or `source_map` input.
- `any_upstream_terminal` needs at least one incoming edge, so a root node can't use it.

`orchestration.on_upstream` sets whether a failed, crashed, cancelled, or timed-out upstream node blocks this node or lets it run. `orchestration.on_failure.retry` retries an agent or deployment node.

## Keep the graph small

Every node boundary needs a reason from the node-splitting rule in `SKILL.md`. Two steps that use the same tools and credentials, with no decision or approval between them, belong in one agent node.
