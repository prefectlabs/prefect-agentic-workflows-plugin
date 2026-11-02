# Explain a run

Use these steps when the user asks why a run failed, stalled, or ended on an unexpected output. They work for runs the user started in the Prefect Cloud UI or on a schedule, too.

## Find the run

1. When the user gives a run URL or ID, use it. The ID is the last path segment of a run URL.
2. Otherwise, find the flow with `get_flow` by its name, then call `list_runs`. For "the last failed run", pass `state_types` of `FAILED` and `CRASHED`. For the runs of a version the user just published, pass `version_id`. When several runs could be the one the user means, list them by name and start time, and ask.

## Find what went wrong

1. Call `get_run` with `compact`. Read `diagnostics`, each node's `status` and `failure`, and each node's `selected_outputs`.
2. For each agent node that failed, or that selected an output the design treats as failure, call `get_run_activity` with the node's `activation_id`. Pass `failed_only` first. When it returns no failed calls, call it again without `failed_only`, because the cause can be a call that succeeded with an unexpected result.
3. Base the answer on what the calls show: the tool, its `arguments`, and its `result`. Quote the error text from `result`, and say which node and attempt it came from. A node can retry: an attempt with the status `superseded` was followed by another attempt, and the last attempt decides the node's outcome.

## Say what the user can do

- A `failure_type` of `mcp_error` or an authentication error in `result` usually means the MCP connection's credential is missing a permission or has expired. [mcp-connections.md](mcp-connections.md) says how the user sets it up again.
- A failed call with `retryable` true, such as a rate limit or a timeout, can pass on the next run. Say so, and quote any fix the `result` names, such as an API key the provider asks for.
- When the tool calls succeeded and the node still failed or chose the wrong output, the cause is in the node's objective or its output schemas. Propose a plan change, and get the user's approval before you publish it.

## When the tool calls can't be read

- `content` of `withheld`: the user's role in the workspace can't view agent content. Tell the user that a workspace Admin or Developer can read the tool calls, or can give them that role. Don't guess the cause from the tool names alone.
- `content` of `unavailable`: Prefect Cloud can no longer decrypt the call's input and output, for example after the run was deleted. Report what the tool names and statuses show.
- No `attempts`, with `no_attempts` in `history.issues`: the workspace doesn't record agent sessions, the run is older than the account's run retention, or the node never started. Explain the failure from `get_run` alone, and say that the tool calls weren't available.
- `tool_calls` of null: the attempt has no session yet. Read the run again after it finishes.
