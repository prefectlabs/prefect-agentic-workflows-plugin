# Platform limits

Last verified: 2026-09-25, against the Prefect Cloud API.

These are the limits a plan must fit when you draft it. The platform changes, so when a `validate_plan` or `publish_plan` error disagrees with this file, follow the error and tell the user this file may be out of date.

## Where plans run

- Execution plans run only in Prefect Cloud. The account needs the `execution-plans` feature, and the workspace needs an object storage bucket.
- Plan versions are immutable. Each publish creates a new version.

## Agent nodes

- An agent node has at most 60 seconds to finish its work. Split longer work across nodes, or move it into a Deployment node.
- Prefect Cloud chooses the model. A plan can't set one.
- Tools come only from remote MCP servers over Streamable HTTP (`"type": "http"`). Stdio MCP servers are rejected, and so are server URLs with an `/sse` path segment.
- An MCP server URL has no credentials, path parameters (`;`), query string, or fragment. It must use HTTPS when the server has `headers` or `query` values.
- Sensitive headers and query values must reference a Secret block as `{"$ref": {"block_document_id": "<id>"}}`. This applies to `Authorization`, `Proxy-Authorization`, `Cookie`, `Set-Cookie`, and any name that contains `token`, `secret`, `password`, `credential`, `apikey`, `accesskey`, or `privatekey`. Case and separators such as `-` and `_` don't matter, so `X-Api-Key` counts. See [secret-blocks.md](secret-blocks.md).

## Graph shape

- No cycles. An edge can't lead back to an earlier node, so a repeat-until-done loop becomes a fixed number of steps or a human checkpoint.
- No mapping. A node can't run once per item of a list. Handle a list inside one agent node, or fan out to a fixed set of nodes.
- Timer nodes can't be activated, so a plan can't wait for a set time. Flag any timed wait to the user as not yet supported.
- Manual evaluation (`evaluate_when` set to `manual`) can't be activated.
- A root node, one with no incoming edges, can't use `evaluate_when` set to `any_upstream_terminal`.
- Human-input and Deployment nodes must use `output_selection` set to `exactly_one`. Only agent nodes can select more than one output.
- Human-input nodes can't set `on_failure.retry` or `limits`.

## Deployment nodes

- A Deployment node passes on only the child run's ID and state. It can't hand a return value to a later node. When a later node needs the result, expose the work as a tool on a remote MCP server.
- The user who publishes and runs the plan needs permission to run the deployment.

## Publishing

- A plan that passes `validate_plan` can still fail on `publish_plan`. Publishing also checks that every MCP server hostname resolves in DNS, and that the publisher can see every referenced Secret block. An API key without access to Secret blocks fails this check.
- A plan declares at most 32 plan outputs, with at most 256 fields across all of them.
