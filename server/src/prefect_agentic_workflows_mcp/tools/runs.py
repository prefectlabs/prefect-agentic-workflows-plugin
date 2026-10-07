"""Tools that start execution-plan runs, watch them, read outputs, and answer forms.

A run always uses the flow's active plan version at the time it starts. There
is no tool to cancel a run, because Prefect Cloud can only cancel runs of plans
made entirely of human-input nodes.
"""

from asyncio import sleep
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from time import monotonic
from typing import Annotated, Any
from uuid import UUID

import httpx
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import (
    HttpMethod,
    RequestTimedOutError,
    WorkspaceApi,
    read_json,
    response_detail,
)

FlowId = Annotated[
    UUID,
    Field(description="ID of the flow to run, from `get_or_create_flow`."),
]

FlowRunId = Annotated[
    UUID,
    Field(description="ID of the run, from `start_run`."),
]

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


def progress(run: dict[str, Any]) -> tuple[Any, ...]:
    """Return the parts of a run observation that `get_run` waits to change.

    These are the run's status and each node's status. A new human-input
    form shows up as a node status change, because the node becomes
    suspended.
    """
    nodes = run.get("nodes") or []
    return (
        run.get("status"),
        tuple(
            (node.get("node"), node.get("activation_id"), node.get("status"))
            for node in nodes
        ),
    )


def retry_after_seconds(response: httpx.Response) -> float | None:
    """Return the `Retry-After` header of a response in seconds, or None.

    The header holds either a number of seconds or an HTTP date.
    """
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(float(value), 0.0)
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max((retry_at - datetime.now(timezone.utc)).total_seconds(), 0.0)


def format_seconds(seconds: float) -> str:
    return f"{seconds:g}"


def read_json_with_retry_hint(
    response: httpx.Response, method: HttpMethod, path: str
) -> Any:
    """Return the JSON body of a response, like `read_json`.

    When an error response has a `Retry-After` header, the error message
    says how long to wait before trying again.
    """
    try:
        return read_json(response, method, path)
    except ToolError as exc:
        retry_after = retry_after_seconds(response)
        if retry_after is None:
            raise
        raise ToolError(
            f"{exc} Retry in {format_seconds(retry_after)} seconds."
        ) from exc


def output_result(
    *,
    available: bool,
    value: Any = None,
    output_status: str | None = None,
    reason: str | None = None,
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
        response = await api.request("POST", path, json=body)
        flow_run = read_json_with_retry_hint(response, "POST", path)
        return {
            "created": response.status_code == 201,
            "flow_run_id": flow_run["id"],
            "flow_run": flow_run,
        }

    async def read_run(
        flow_run_id: UUID, timeout: float | None = None
    ) -> dict[str, Any]:
        path = f"/flow_runs/{flow_run_id}/execution-plan"
        if timeout is None:
            response = await api.request("GET", path)
        else:
            response = await api.request("GET", path, timeout=timeout)
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
        is null.

        When a node waits for human input, show its form to the user and
        submit only what the user answers, with `submit_human_input`.
        """
        run = await read_run(flow_run_id)
        if wait_seconds is None:
            return {**run, "wait": None}

        started = monotonic()
        deadline = started + min(wait_seconds, MAX_WAIT_SECONDS)
        before = progress(run)
        changed = False
        while run["status"] not in STOP_WAITING_STATUSES and not awaits_input(run):
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
                run = await read_run(flow_run_id, timeout=remaining)
            except RequestTimedOutError:
                # The wait ran out while Cloud was answering, which is the
                # same as a wait with no change. Return the last observation.
                break
            if progress(run) != before:
                changed = True
                break
        return {
            **run,
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
    ) -> dict[str, Any]:
        """Read a plan output of a run, or one node activation's output.

        Returns `available` and, when it is true, the output's `value`.
        When the output can't be read yet or at all, `available` is false and
        `output_status`, `reason`, and `detail` say why, as Prefect Cloud
        reports them.

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
        response = await api.request("GET", path)

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
        result = await api.request("POST", path, json={"response": response})
        read_json_with_retry_hint(result, "POST", path)
        return {
            "submitted": True,
            "flow_run_id": str(flow_run_id),
            "activation_id": str(activation_id),
        }
