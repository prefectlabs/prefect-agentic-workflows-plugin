# Explain a run

Use these steps when the user asks why a run failed, stalled, or ended on an unexpected output. They work for runs the user started in the Prefect Cloud UI or on a schedule, too.

## Find the run

1. When the user gives a run URL or ID, use it. The ID is the last path segment of a run URL.
2. Otherwise, find the flow with `get_flow` by its name, then call `list_runs`. For "the last failed run", pass `state_types` of `FAILED` and `CRASHED`. For the runs of a version the user just published, pass `version_id`. When several runs could be the one the user means, list them by name and start time, and ask.

## Find what went wrong

1. Call `get_run` with `compact`. Read `diagnostics`, each node's `status` and `failure`, and each node's `selected_outputs`.
2. For each agent node that failed, or that selected an output the design treats as failure, call `get_run_activity` with the node's `activation_id`. It returns the node's model calls and tool calls in order. A model call's `output` shows what the model wrote and which tools it called. A tool call shows its `arguments` and `result`.
3. Look at the failed calls first, then at the last calls before the node ended. The cause can be a call that succeeded with an unexpected result, or a model call that chose the wrong output.
4. Content is cut to 300 characters, or 2,000 for a failed call. When the cut part can explain the failure, call `get_activity_record` with the attempt's `attempt_id` and the call's `record_id` to read it in full. A model call's full `input` has the node's request and the upstream outputs it received.
5. Base the answer on what the calls show. Quote the error text, and say which node and attempt it came from. A node can retry: an attempt with the status `superseded` was followed by another attempt, and the last attempt decides the node's outcome.

## Say what the user can do

- A model call with a `failure_type` such as `rate_limit`, `authentication`, or `server_error` failed at the model provider. An `authentication` failure means the provider rejected the API key of the workspace's provider connection.
- A `failure_type` of `mcp_error` or an authentication error in `result` usually means the MCP connection's credential is missing a permission or has expired. [mcp-connections.md](mcp-connections.md) says how the user sets it up again.
- A failed call with `retryable` true, such as a rate limit or a timeout, can pass on the next run. Say so, and quote any fix the `result` names, such as an API key the provider asks for.
- When the calls succeeded and the node still failed or chose the wrong output, the cause is in the node's objective or its output schemas. Propose a plan change, and get the user's approval before you publish it.

## When the calls can't be read

- `content` of `withheld`: the user's role in the workspace can't view agent content. Tell the user that a workspace Admin or Developer can read the calls, or can give them that role. Don't guess the cause from the tool names alone.
- `content` of `unavailable`: Prefect Cloud can no longer decrypt the call's input and output, for example after the run was deleted. Report what the tool names and statuses show.
- No `attempts`, with `no_attempts` in `history.issues`: the workspace doesn't record agent sessions, the run is older than the account's run retention, or the node never started. Explain the failure from `get_run` alone, and say that the calls weren't available.
- `activity` of null: the attempt has no session yet. Read the run again after it finishes.
- `activity_left_out` above 0: the result left out older calls that succeeded, to fit. The failed calls and the latest calls are still listed.
