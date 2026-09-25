# Example plans

Three complete plans that pass `validate_plan`. Start a draft from the example closest to the workflow, then change it to fit. Each example uses schema version 0.2, which adds plan `outputs`. When `get_schema` returns a different `schema_version`, write that version, and remove `outputs` if the version doesn't declare them.

The examples hold placeholder IDs of all zeros. Replace each one before you publish: a Secret block ID comes from `list_secret_blocks`, and a deployment ID comes from the user. `validate_plan` accepts the placeholders, but `publish_plan` rejects a Secret block ID that the publisher can't see.

## A single agent node with an MCP server

[examples/single-agent-with-mcp.plan.json](examples/single-agent-with-mcp.plan.json)

One agent node reads a repository name from a plan input, lists open bug issues with the GitHub MCP server, and selects one `summary` output. The MCP server uses Streamable HTTP, and its `Authorization` header refers to a Secret block, whose value is the whole header value, such as `Bearer <token>`. The node retries twice on failure. The plan output `bug_report` returns the summary as the run's result.

Use this shape when the work needs one set of tools and has no branch or approval. That covers most workflows.

## An agent that branches to an approval, then runs a deployment

[examples/approval-then-deployment.plan.json](examples/approval-then-deployment.plan.json)

The agent node turns a request in plain words into two dates and selects one of two outputs. `ready` leads to the approval, and `needs_detail` ends the run with a question for the requester. The human-input form has a required `decision` whose choices, `approved` and `rejected`, match the node's response outputs. When nobody answers in 3 days, the deadline selects `expired`, which is not a `decision` choice.

The Deployment node has two inputs. `parameters` takes the dates from the agent's `ready` output and becomes the deployment's run parameters, so the `ready` schema sets `additionalProperties` to false and holds only parameter names. `approval` takes the `approved` output and is never read. It makes the deployment wait for the approval. When the reviewer rejects the request, `approval` receives no value, and the deployment doesn't run. The deployment's `completed` and `failed` outputs each carry the child run's ID and state, and the plan output joins them with a `one_of` source. Rename the two date fields to the parameter names of the user's deployment.

## A fan-out and join

[examples/fan-out-and-join.plan.json](examples/fan-out-and-join.plan.json)

One plan input feeds three agent nodes that review the same change for different concerns. They run at the same time, because each needs only the plan input. The `combine_reviews` node joins them:

- Its `reviews` input has shape `source_map` with `key_by` set to `edge`, so it receives an object keyed by edge ID. Name the edges for what they carry, so the objective can refer to each review by its edge ID.
- `evaluate_when` is `all_reachable_terminal`, so the join waits until every review has finished.
- `on_upstream` allows failed, crashed, cancelled, and timed-out reviews, so one failed review doesn't block the join. The join receives only the reviews that produced a value, and its objective says what to do when one is missing. The run still ends as `blocked` when a review fails, even after the join completes. That outcome comes from the failed review, not the join.

To fan out only to the branches that apply, give one agent node an output per branch and set its `output_selection` to `one_or_more`. The branches it doesn't select don't run, and the join doesn't wait for them.
