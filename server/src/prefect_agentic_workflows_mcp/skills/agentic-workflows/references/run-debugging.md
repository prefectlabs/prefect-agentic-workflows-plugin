# Explain a run

Use these steps when the user asks why a run failed, stalled, or ended on an unexpected output. They work for runs the user started in the Prefect Cloud UI or on a schedule, too.

## Find the run

1. When the user gives a run URL or ID, use it. The ID is the last path segment of a run URL.
2. Otherwise, find the flow with `get_flow` by its name, then call `list_runs`. For "the last failed run", pass `state_types` of `FAILED` and `CRASHED`. For the runs of a version the user just published, pass `version_id`. When several runs could be the one the user means, list them by name and start time, and ask.

## Find what went wrong

1. Call `get_run` with `compact`. Read `diagnostics`, each node's `status` and `failure`, and each node's `selected_outputs`.
2. For each node whose `failure` code is `expectation_violated`, `expectation_inconclusive`, or `verification_unavailable`, follow [A node that failed its checks](#a-node-that-failed-its-checks). For a node with `items`, follow [A mapped node that failed](#a-mapped-node-that-failed) first.
3. For each other agent node that failed, or that selected an output the design treats as failure, call `get_run_activity` with the node's `activation_id`. It returns the node's model calls and tool calls in order. A model call's `output` shows what the model wrote and which tools it called. A tool call shows its `arguments` and `result`.
4. Look at the failed calls first, then at the last calls before the node ended. The cause can be a call that succeeded with an unexpected result, or a model call that chose the wrong output.
5. Content is cut to 300 characters, or 2,000 for a failed call. When the cut part can explain the failure, call `get_activity_record` with the attempt's `attempt_id` and the call's `record_id` to read it in full. A model call's full `input` has the node's request and the upstream outputs it received.
6. Base the answer on what the calls show. Quote the error text, and say which node and attempt it came from. A node can retry: an attempt with the status `superseded` or `verification_retry` was followed by another attempt, and the last attempt decides the node's outcome.

## A mapped node that failed

A mapped node runs one item for each element of a list, and lists them in `items`. It has no `activation_id` of its own, so read each item by the item's `activation_id`.

- `invalid_mapped_value`: the value the node maps isn't an array or an object, or an item's `map.key` is missing, isn't a string or an integer, or repeats another item's key. No item ran. Read the node that produced the list, or the plan input the run started with, and fix it or the key path.
- `too_many_mapped_items`: the list had more than 100 items, and no item ran. Bound the list where it's made, as [design-guidance.md](design-guidance.md#mapped-nodes) says.
- `output_size_limit_exceeded`: the items' values for one output add up to more than 1 MiB. Prefect stops starting items once they do, so some items can show `not_started`. Make each item's output smaller, or map over fewer items.
- Any other code is the code of the first item that failed, crashed, or timed out. Prefect started no more items after it, and the items that were running finished. Find the items whose `status` is `failed`, `crashed`, or `timed_out`, name them by `key`, and explain each one from [Find what went wrong](#find-what-went-wrong) step 2 or 3 with its `activation_id`. Items with the status `not_started` never ran.

The node passes on no outputs when it fails, even from items that completed. Read a completed item's output with `get_run_output` and the item's `activation_id`. To keep one bad item from failing the batch, the node needs a `blocked` output that its objective tells the agent to select.

## A node that failed its checks

Prefect checks the expectations on an agent node's selected outputs, and the node fails when a check doesn't pass. None of its outputs reach later nodes, and the run fails.

1. Call `get_run_checks` with the node's `activation_id`. When `checks` is empty, call it once more: a check is listed a moment after it ends.
2. Read the checks with the highest `attempt_number`, which decided the node's outcome. An earlier attempt's checks show what the agent was told to fix before it ran again.
3. Tell the user each expectation that wasn't `satisfied`, by its `expectation_text`, with its `verdict` and its `reason` in the check's own words. When `content` is `withheld`, a workspace Admin or Developer can read the reason.
4. To see what the agent did, call `get_run_activity` with the same `activation_id`.

Then say what the user can do:

- `expectation_violated`: the output missed the expectation on every attempt the node's `on_failure.retry` allows. The fix is in the node's objective or tools when the agent could have met the expectation, and in the expectation's text when it asks for something the workflow doesn't do.
- `expectation_inconclusive`: the agent's tool calls didn't show whether the expectation was met. Change the objective so the agent reads the state the expectation asks about with a tool call, or reword the expectation to something a tool call can show.
- `verification_unavailable`: Prefect couldn't finish a check. Quote the check's error `detail`, and say that running the workflow again can pass.

A change to an objective or an expectation is a plan change, so get the user's approval before you publish it.

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
