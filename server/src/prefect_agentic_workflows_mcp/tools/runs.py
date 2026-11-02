"""Tools that start and list execution-plan runs, watch them, read outputs, and
answer forms.

A run always uses the flow's active plan version at the time it starts. There
is no tool to cancel a run, because Prefect Cloud can only cancel runs of plans
made entirely of human-input nodes.
"""

from asyncio import sleep
from time import monotonic
from typing import Annotated, Any, Literal
from uuid import UUID

from fastmcp import FastMCP
from pydantic import Field

from prefect_agentic_workflows_mcp.parameters import FlowRunId, WorkspaceId
from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import (
    NO_BUCKET_DETAIL,
    RequestTimedOutError,
    WorkspaceApi,
    read_json,
    read_json_with_retry_hint,
    response_detail,
    retry_after_seconds,
)

FlowId = Annotated[
    UUID,
    Field(description="ID of the flow to run, from `get_or_create_flow`."),
]

StateType = Literal[
    "SCHEDULED",
    "PENDING",
    "RUNNING",
    "PAUSED",
    "CANCELLING",
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "CRASHED",
]

# The summaries route reads at most 100 runs in one request.
MAX_LISTED_RUNS = 100
MAX_WAIT_SECONDS = 30
POLL_INTERVAL_SECONDS = 2.0
# `get_run` returns at once for these: the run either can't change on its own
# or needs someone to act before it can.
STOP_WAITING_STATUSES = frozenset({"completed", "failed", "cancelled", "blocked"})
FINAL_OUTPUT_STATUSES = frozenset({"failed", "skipped", "unavailable"})


def awaits_input(run: dict[str, Any]) -> bool:
    """Return whether any node is waiting for a person to answer a form."""
    return any(
        (node.get("wait") or {}).get("kind") == "human_input"
        for node in run.get("nodes") or []
    )


def settled(run: dict[str, Any]) -> bool:
    """Return whether a run needs no more watching.

    That is a finished or blocked run whose plan outputs are built, or a run
    with a form waiting for an answer. Prefect Cloud builds plan outputs a
    moment after the run finishes, so they can still be `waiting` then.
    """
    if awaits_input(run):
        return True
    return run.get("status") in STOP_WAITING_STATUSES and not any(
        output.get("status") == "waiting" for output in run.get("outputs") or []
    )


def compact_run(run: dict[str, Any]) -> dict[str, Any]:
    """Return the parts of a run observation needed to follow its progress.

    Leaves out edges, task runs, and output result metadata, which make up
    most of a full observation.
    """
    return {
        "flow_run_id": (run.get("snapshot") or {}).get("flow_run_id"),
        "status": run.get("status"),
        "nodes": [
            {
                "node": node.get("node"),
                "status": node.get("status"),
                "activation_id": node.get("activation_id"),
                "selected_outputs": [
                    output.get("output")
                    for output in node.get("outputs") or []
                    if output.get("status") == "selected"
                ],
                "failure": node.get("failure"),
                "wait": node.get("wait"),
            }
            for node in run.get("nodes") or []
        ],
        "outputs": [
            {
                "output": output.get("output"),
                "status": output.get("status"),
                "failure_code": output.get("failure_code"),
            }
            for output in run.get("outputs") or []
        ],
        "diagnostics": run.get("diagnostics") or [],
    }


def run_list_entry(flow_run: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    """Return one `list_runs` entry from a flow run and its run summary.

    Leaves out each plan output's provenance, which `get_run` returns.
    """
    return {
        "flow_run_id": flow_run["id"],
        "name": flow_run["name"],
        "state_type": flow_run["state_type"],
        "state_name": flow_run["state_name"],
        "created": flow_run["created"],
        "start_time": flow_run["start_time"],
        "status": summary.get("status"),
        "execution_plan_version_id": summary.get("execution_plan_version_id"),
        "outputs": [
            {
                "output": output["output"],
                "status": output["status"],
                "failure_code": output.get("failure_code"),
            }
            for output in summary.get("outputs", [])
        ],
        "last_nodes": summary.get("last_nodes", []),
        "unavailable_reason": summary.get("unavailable_reason"),
    }


def progress(run: dict[str, Any]) -> tuple[Any, ...]:
    """Return the parts of a run observation that `get_run` waits to change.

    These are the run's status, each node's status, and each plan output's
    status. A new human-input form shows up as a node status change, because
    the node becomes suspended.
    """
    nodes = run.get("nodes") or []
    outputs = run.get("outputs") or []
    return (
        run.get("status"),
        tuple(
            (node.get("node"), node.get("activation_id"), node.get("status"))
            for node in nodes
        ),
        tuple((output.get("output"), output.get("status")) for output in outputs),
    )


def output_result(
    *,
    available: bool,
    value: Any = None,
    output_status: str | None = None,
    reason: str | None = None,
    failure_code: str | None = None,
    detail: str | None = None,
    retry_after_seconds: float | None = None,
) -> dict[str, Any]:
    """Return a `get_run_output` result. Every result has the same keys.

    `final` is true when reading the output again can't give a different
    answer: the value is available, or the output failed, was skipped, or
    is unavailable for good. An output that is `waiting` or `pending`, or
    one that Prefect Cloud couldn't read this time, can still arrive.
    """
    return {
        "available": available,
        "value": value,
        "output_status": output_status,
        "reason": reason,
        "failure_code": failure_code,
        "detail": detail,
        "retry_after_seconds": retry_after_seconds,
        "final": available or output_status in FINAL_OUTPUT_STATUSES,
    }


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the run tools to `mcp`."""

    @tool(mcp, read_only=False, destructive=True)
    async def start_run(
        flow_id: FlowId,
        parameters: Annotated[
            dict[str, Any] | None,
            Field(
                description=(
                    "Values for the plan's inputs, keyed by input name. "
                    "Prefect Cloud checks them against the active plan."
                ),
            ),
        ] = None,
        name: Annotated[
            str | None,
            Field(description="Name of the run. Omit it to get a generated name."),
        ] = None,
        idempotency_key: Annotated[
            str | None,
            Field(
                description=(
                    "Key that makes a retry safe. A second call with the same "
                    "key returns the run the first call started and starts no "
                    "new run."
                ),
            ),
        ] = None,
        workspace_id: WorkspaceId = None,
    ) -> dict[str, Any]:
        """Start a run of a flow's active execution plan.

        Ask the user to confirm before you call this tool. Returns `created`,
        which is false when a run with the same `idempotency_key` already
        existed, `flow_run_id`, and the `flow_run`. Watch the run with
        `get_run`.
        """
        body: dict[str, Any] = {"parameters": parameters or {}}
        if name is not None:
            body["name"] = name
        if idempotency_key is not None:
            body["idempotency_key"] = idempotency_key
        path = f"/flows/{flow_id}/execution-plan/runs"
        response = await api.request("POST", path, json=body, workspace_id=workspace_id)
        flow_run = read_json_with_retry_hint(response, "POST", path)
        return {
            "created": response.status_code == 201,
            "flow_run_id": flow_run["id"],
            "flow_run": flow_run,
        }

    @tool(mcp, read_only=True)
    async def list_runs(
        flow_id: Annotated[
            UUID,
            Field(description="ID of the flow, from `get_flow`."),
        ],
        state_types: Annotated[
            list[StateType] | None,
            Field(
                min_length=1,
                description=(
                    "Only runs in one of these Prefect state types, such as "
                    "`FAILED` and `CRASHED` for failed runs. Omit it for runs "
                    "in any state."
                ),
            ),
        ] = None,
        version_id: Annotated[
            UUID | None,
            Field(
                description=(
                    "Only runs of this plan version, from `list_plan_versions`. "
                    "Omit it for runs of every version."
                ),
            ),
        ] = None,
        limit: Annotated[
            int,
            Field(ge=1, le=MAX_LISTED_RUNS, description="How many runs to return."),
        ] = 10,
        workspace_id: WorkspaceId = None,
    ) -> dict[str, Any]:
        """List a flow's execution-plan runs, with what each produced.

        Use it to find a run the user started in the Prefect Cloud UI or on a
        schedule, such as the latest failed run. The runs are in the order of
        the flow's Runs tab in Prefect Cloud: the latest expected start time
        first. Scheduled runs that haven't started yet come before the others,
        with the `state_type` `SCHEDULED`. Returns `runs`, each with
        `flow_run_id`, `name`, the Prefect `state_type` and `state_name`,
        `created`, `start_time`, and the run's execution-plan `status` and
        `execution_plan_version_id`. `outputs` lists each plan output's
        `status` and any `failure_code`. Read an output's value with
        `get_run_output`. `last_nodes` lists the completed nodes whose
        selected outputs went to no other node, with those outputs, which is
        usually the outcome the run ended on. When Prefect Cloud can't read a
        run's plan, `unavailable_reason` says why and the execution-plan
        fields are null. Read one run's nodes and failures with `get_run`.
        """
        body: dict[str, Any] = {"sort": "EXPECTED_START_TIME_DESC", "limit": limit}
        if state_types is not None:
            body["state"] = {"type": {"any_": state_types}}
        if version_id is not None:
            body["execution_plan_version_ids"] = [str(version_id)]
        runs_path = f"/flows/{flow_id}/execution-plan/runs"
        flow_runs: list[dict[str, Any]] = await api.call(
            "POST", f"{runs_path}/filter", json=body, workspace_id=workspace_id
        )
        if not flow_runs:
            return {"runs": []}
        summaries_path = f"{runs_path}/summaries"
        response = await api.request(
            "POST",
            summaries_path,
            json={"flow_run_ids": [flow_run["id"] for flow_run in flow_runs]},
            workspace_id=workspace_id,
        )
        summaries = {
            summary["flow_run_id"]: summary
            for summary in read_json_with_retry_hint(response, "POST", summaries_path)
        }
        return {
            "runs": [
                run_list_entry(flow_run, summaries.get(flow_run["id"], {}))
                for flow_run in flow_runs
            ]
        }

    async def read_run(
        flow_run_id: UUID,
        timeout: float | None = None,
        workspace_id: WorkspaceId = None,
    ) -> dict[str, Any]:
        path = f"/flow_runs/{flow_run_id}/execution-plan"
        if timeout is None:
            response = await api.request("GET", path, workspace_id=workspace_id)
        else:
            response = await api.request(
                "GET", path, timeout=timeout, workspace_id=workspace_id
            )
        return read_json_with_retry_hint(response, "GET", path)

    @tool(mcp, read_only=True)
    async def get_run(
        flow_run_id: FlowRunId,
        wait_seconds: Annotated[
            float | None,
            Field(
                ge=0,
                description=(
                    "Wait up to this many seconds for the run's status or a "
                    f"node's status to change before returning. Waits longer "
                    f"than {MAX_WAIT_SECONDS} seconds are cut to "
                    f"{MAX_WAIT_SECONDS}. Omit it to return at once."
                ),
            ),
        ] = None,
        until: Annotated[
            Literal["change", "settled"],
            Field(
                description=(
                    "'settled' waits through node changes until the run ends "
                    "with its outputs built, or a form waits."
                ),
            ),
        ] = "change",
        compact: Annotated[
            bool,
            Field(
                description="Return only statuses, failures, waits, and diagnostics.",
            ),
        ] = False,
        workspace_id: WorkspaceId = None,
    ) -> dict[str, Any]:
        """Read the progress of an execution-plan run.

        Returns the run as Prefect Cloud reports it.
        `snapshot.execution_plan_version_id` is the plan version the run
        uses, which was the flow's active version when the run started.
        `status` is the run's status: `running`, `awaiting_external_progress`,
        `blocked`, `completed`, `failed`, or `cancelled`. `nodes` lists each
        node with its `status`, `activation_id`, `outputs`, and any
        `failure`. A node that waits for a person has a `wait` with `kind`
        set to `human_input`, the `form_schema` to show the user, and a
        `deadline_at`. `outputs` lists the plan outputs and their status, and
        `diagnostics` explains why a run is stuck or failed.

        With `wait_seconds`, the tool checks the run every few seconds and
        returns as soon as the run's status or any node's status changes,
        when the run finishes or is blocked, when a node is already waiting
        for human input, or when the wait runs out. `wait` then reports
        `seconds_waited` and `status_changed`. Without `wait_seconds`, `wait`
        is null. To watch a run to its end, repeat 30-second waits with
        `until='settled'` and `compact=true`.

        When a node waits for human input, show its form to the user and
        submit only what the user answers, with `submit_human_input`.
        """
        shape = compact_run if compact else dict
        run = await read_run(flow_run_id, workspace_id=workspace_id)
        if wait_seconds is None:
            return {**shape(run), "wait": None}

        def done(run: dict[str, Any]) -> bool:
            if until == "settled":
                return settled(run)
            return run["status"] in STOP_WAITING_STATUSES or awaits_input(run)

        started = monotonic()
        deadline = started + min(wait_seconds, MAX_WAIT_SECONDS)
        before = progress(run)
        changed = False
        while not done(run):
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            await sleep(min(POLL_INTERVAL_SECONDS, remaining))
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            # Bound the read by the time left, so a slow response can't stretch
            # the call past the wait the caller asked for.
            try:
                run = await read_run(
                    flow_run_id, timeout=remaining, workspace_id=workspace_id
                )
            except RequestTimedOutError:
                # The wait ran out while Cloud was answering, which is the
                # same as a wait with no change. Return the last observation.
                break
            if progress(run) != before:
                changed = True
                if until == "change":
                    break
        return {
            **shape(run),
            "wait": {
                "seconds_waited": monotonic() - started,
                "status_changed": changed,
            },
        }

    @tool(mcp, read_only=True)
    async def get_run_output(
        flow_run_id: FlowRunId,
        output_name: Annotated[
            str,
            Field(
                # The plan identifier pattern. The name goes into the request
                # path, and this pattern rules out `/`, `?`, `.`, and `..`.
                pattern=r"^[A-Za-z_][A-Za-z0-9_.-]*$",
                description=(
                    "Name of a plan output, or of a node output when you pass "
                    "`activation_id`."
                ),
            ),
        ],
        activation_id: Annotated[
            UUID | None,
            Field(
                description=(
                    "ID of one node activation, from a node's `activation_id` "
                    "in `get_run`. Omit it to read a plan output."
                ),
            ),
        ] = None,
        workspace_id: WorkspaceId = None,
    ) -> dict[str, Any]:
        """Read a plan output of a run, or one node activation's output.

        Returns `available` and, when it is true, the output's `value`.
        When the output can't be read yet or at all, `available` is false and
        `output_status`, `reason`, and `detail` say why, as Prefect Cloud
        reports them. A `failed` plan output also has a `failure_code`, such as
        `required_field_missing`.

        `final` says whether the answer can still change. When `final` is
        false, the output can still arrive: `output_status` is `waiting` or
        `pending` while the run or node is still working, and it is null when
        Prefect Cloud couldn't read the output this time. Read it again after
        `retry_after_seconds` when that is set, or after `get_run` shows
        progress. When `final` is true and `available` is false, the output
        will never arrive, for example because its node failed (`failed`) or
        didn't produce it (`skipped`).
        """
        run_path = f"/flow_runs/{flow_run_id}/execution-plan"
        if activation_id is None:
            path = f"{run_path}/outputs/{output_name}"
        else:
            path = f"{run_path}/activations/{activation_id}/outputs/{output_name}"
        response = await api.request("GET", path, workspace_id=workspace_id)

        if response.status_code == 409 and NO_BUCKET_DETAIL in response_detail(
            response
        ):
            # A missing bucket is a workspace problem, not an output that can
            # still arrive, so raise the message that says how to fix it.
            read_json(response, "GET", path)
        if response.status_code in (202, 409, 503):
            try:
                body = response.json()
            except ValueError:
                body = None
            if not isinstance(body, dict):
                body = {}
            return output_result(
                available=False,
                output_status=body.get("output_status"),
                reason=body.get("reason"),
                failure_code=body.get("failure_code"),
                detail=response_detail(response),
                retry_after_seconds=retry_after_seconds(response),
            )
        value = read_json_with_retry_hint(response, "GET", path)
        return output_result(available=True, value=value, output_status="available")

    @tool(mcp, read_only=False, destructive=True)
    async def submit_human_input(
        flow_run_id: FlowRunId,
        activation_id: Annotated[
            UUID,
            Field(
                description=(
                    "ID of the waiting node activation, from the node's "
                    "`wait.activation_id` in `get_run`."
                ),
            ),
        ],
        response: Annotated[
            dict[str, Any],
            Field(
                description=(
                    "The user's answer as a JSON object that matches the "
                    "node's `wait.form_schema`."
                ),
            ),
        ],
        workspace_id: WorkspaceId = None,
    ) -> dict[str, Any]:
        """Submit the user's answer to a human-input form in a run.

        Show the form from `get_run` to the user and submit only the answer
        the user gives. Never fill in a form yourself, even for a test run.
        A form takes one answer. Prefect Cloud rejects a second answer, an
        answer after the form's deadline, and an answer that doesn't match
        the form. After you submit, watch the run with `get_run`.
        """
        path = (
            f"/flow_runs/{flow_run_id}/execution-plan/activations/"
            f"{activation_id}/human-input/responses"
        )
        result = await api.request(
            "POST", path, json={"response": response}, workspace_id=workspace_id
        )
        read_json_with_retry_hint(result, "POST", path)
        return {
            "submitted": True,
            "flow_run_id": str(flow_run_id),
            "activation_id": str(activation_id),
        }
