"""A tool that reads the tool calls of a run's agent nodes.

When a workspace records agent sessions, Prefect Cloud stores each attempt of
an agent node as a session, and each tool call as one span of that session in
OTLP JSON. The agent session routes serve them.
"""

from asyncio import gather
from datetime import datetime, timezone
from typing import Annotated, Any
from uuid import UUID

from fastmcp import FastMCP
from pydantic import Field

from prefect_agentic_workflows_mcp.parameters import FlowRunId, WorkspaceId
from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import (
    WorkspaceApi,
    read_json_with_retry_hint,
)

# The agent session routes return at most 100 items in one page.
PAGE_LIMIT = 100
# Limits that keep one call's result from filling the agent's context.
MAX_TOOL_CALLS_PER_ATTEMPT = 200
MAX_CONTENT_CHARACTERS = 2_000
# With `failed_only`, a read looks through this many tool calls of an attempt
# for failures, because the call that failed a node is usually one of its last.
MAX_TOOL_CALLS_SEARCHED = 2_000
# The marker that the collector also appends to a value it cut.
TRUNCATED_MARKER = "…[truncated]"

ACTIVITY_ATTRIBUTE = "prefect.agent.activity."
SPAN_STATUS_ERROR = 2


def span_attributes(span: dict[str, Any]) -> dict[str, Any]:
    """Return the attributes of an OTLP JSON span as a dict of plain values."""
    return {
        attribute["key"]: next(iter(attribute["value"].values()), None)
        for attribute in span["attributes"]
    }


def span_time(unix_nano: str | None) -> str | None:
    """Return an OTLP JSON time in nanoseconds as an ISO 8601 time in UTC."""
    if unix_nano is None:
        return None
    return datetime.fromtimestamp(int(unix_nano) / 1e9, tz=timezone.utc).isoformat()


def cut_content(value: str | None) -> str | None:
    """Return `value` cut to `MAX_CONTENT_CHARACTERS`, marked when cut."""
    if value is None or len(value) <= MAX_CONTENT_CHARACTERS:
        return value
    return value[:MAX_CONTENT_CHARACTERS] + TRUNCATED_MARKER


def tool_call_entry(item: dict[str, Any]) -> dict[str, Any]:
    """Return one tool call from an item of a session's activity.

    A call with only an in-flight copy of its span has no end time and no
    status yet.
    """
    span = item["operation"]
    attributes = span_attributes(span)
    if item["kind"] == "operation_started":
        status = "in_flight"
    elif span["status"]["code"] == SPAN_STATUS_ERROR:
        status = "error"
    else:
        status = "ok"
    return {
        "tool": attributes.get(f"{ACTIVITY_ATTRIBUTE}tool"),
        "mcp_server": attributes.get(f"{ACTIVITY_ATTRIBUTE}mcp_server"),
        "started_at": span_time(span["startTimeUnixNano"]),
        "ended_at": span_time(span.get("endTimeUnixNano")),
        "status": status,
        "failure_type": attributes.get(f"{ACTIVITY_ATTRIBUTE}failure_type"),
        "retryable": attributes.get(f"{ACTIVITY_ATTRIBUTE}retryable"),
        "content": item["content"],
        "arguments": cut_content(attributes.get("gen_ai.tool.call.arguments")),
        "result": cut_content(attributes.get("gen_ai.tool.call.result")),
    }


def merged_history(histories: list[dict[str, Any]]) -> dict[str, Any]:
    """Return one `history` for every page of a run's agent executions.

    The route says what each page can vouch for, so the run's history is
    `partial` when any page is, and has the issues of every page.
    """
    coverages = [history["coverage"] for history in histories]
    issues = {
        issue["code"]: issue for history in histories for issue in history["issues"]
    }
    return {
        "coverage": "partial" if "partial" in coverages else coverages[-1],
        "issues": list(issues.values()),
    }


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the run activity tool to `mcp`."""

    async def read_page(
        path: str, params: dict[str, Any], workspace_id: WorkspaceId
    ) -> dict[str, Any]:
        response = await api.request(
            "GET", path, params=params, workspace_id=workspace_id
        )
        return read_json_with_retry_hint(response, "GET", path)

    async def read_executions(
        flow_run_id: UUID, workspace_id: WorkspaceId
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Return every agent execution of a run, and the history of all pages."""
        path = f"/flow_runs/{flow_run_id}/agent-executions"
        executions: list[dict[str, Any]] = []
        histories: list[dict[str, Any]] = []
        params: dict[str, Any] = {"limit": PAGE_LIMIT}
        while True:
            page = await read_page(path, params, workspace_id)
            executions.extend(page["executions"])
            histories.append(page["history"])
            if page["next_cursor"] is None:
                return executions, merged_history(histories)
            params = {"limit": PAGE_LIMIT, "cursor": page["next_cursor"]}

    async def read_tool_calls(
        session_id: str, failed_only: bool, workspace_id: WorkspaceId
    ) -> tuple[list[dict[str, Any]], bool]:
        """Return a session's first tool calls, and whether any were left out.

        With `failed_only`, return the failed calls among the session's first
        `MAX_TOOL_CALLS_SEARCHED` calls.
        """
        path = f"/agent-sessions/{session_id}/activity"
        params: dict[str, Any] = {"operation": "execute_tool", "limit": PAGE_LIMIT}
        search_limit = (
            MAX_TOOL_CALLS_SEARCHED if failed_only else MAX_TOOL_CALLS_PER_ATTEMPT
        )
        calls: list[dict[str, Any]] = []
        calls_read = 0
        while True:
            page = await read_page(path, params, workspace_id)
            calls_read += len(page["activity"])
            for call in map(tool_call_entry, page["activity"]):
                if not failed_only or call["status"] == "error":
                    calls.append(call)
            more = page["next_cursor"] is not None
            if (
                not more
                or calls_read >= search_limit
                or len(calls) >= MAX_TOOL_CALLS_PER_ATTEMPT
            ):
                cut = more or len(calls) > MAX_TOOL_CALLS_PER_ATTEMPT
                return calls[:MAX_TOOL_CALLS_PER_ATTEMPT], cut
            params = {**params, "cursor": page["next_cursor"]}

    async def attempt_entry(
        execution: dict[str, Any], failed_only: bool, workspace_id: WorkspaceId
    ) -> dict[str, Any]:
        attempt = execution["execution"]
        invocation = attempt["invocation"]
        session = execution["session"]
        tool_calls: list[dict[str, Any]] | None = None
        tool_calls_cut = False
        if session["status"] == "available":
            tool_calls, tool_calls_cut = await read_tool_calls(
                session["id"], failed_only, workspace_id
            )
        return {
            "attempt_id": attempt["id"],
            "node_id": invocation["node_id"],
            "activation_id": invocation["activation_id"],
            "task_run_id": invocation["task_run_id"],
            "attempt_ordinal": attempt["attempt_ordinal"],
            "status": execution["status"],
            "started_at": execution["started_at"],
            "ended_at": execution["ended_at"],
            "tool_calls": tool_calls,
            "tool_calls_cut": tool_calls_cut,
        }

    @tool(mcp, read_only=True)
    async def get_run_activity(
        flow_run_id: FlowRunId,
        activation_id: Annotated[
            UUID | None,
            Field(
                description=(
                    "ID of one node activation, from a node's `activation_id` "
                    "in `get_run`. Omit it to read every agent node of the run."
                ),
            ),
        ] = None,
        failed_only: Annotated[
            bool,
            Field(description="Return only the tool calls that failed."),
        ] = False,
        workspace_id: WorkspaceId = None,
    ) -> dict[str, Any]:
        """Read the tool calls that a run's agent nodes made.

        Use it to explain why a run or a node failed: read the run with
        `get_run`, then pass a failed agent node's `activation_id` with
        `failed_only`, and base the answer on the failed calls' results.

        Returns `attempts`, one for each attempt of an agent node, in start
        order, with its `node_id`, `activation_id`, `status`, and times. An
        attempt with the status `superseded` was followed by a retry.
        `tool_calls` lists the attempt's tool calls in start order, each with
        the `tool`, the `mcp_server` when Prefect Cloud recorded one, and a
        `status` of `ok`, `error`, or `in_flight`. A failed call has a
        `failure_type`, such as `tool_error`, `mcp_error`, or `timeout`, and
        `retryable`. `arguments` and `result` are the call's input and output
        as text, cut to 2,000 characters with `…[truncated]` at the end.
        `tool_calls` is null when Prefect Cloud has no session for the
        attempt yet. The list holds at most 200 calls. Without `failed_only`,
        they are the attempt's first 200 calls. With `failed_only`, they are
        the failed calls among its first 2,000. `tool_calls_cut` is true when
        the attempt has calls that the list leaves out.

        Each call's `content` says whether `arguments` and `result` are
        there. `withheld` means the user's role lacks the "View agent
        prompts, outputs, and tool data" permission, which the Admin and
        Developer roles have. Tell the user that, instead of guessing at the
        cause. `unavailable` means Prefect Cloud could no longer decrypt them.

        `history` says whether the list is complete. When `attempts` is
        empty and `history.issues` has `no_attempts`, Prefect Cloud recorded
        no agent sessions for the run: the run has no agent nodes or hasn't
        started one, it is older than the account's run retention, or the
        workspace doesn't record agent sessions.
        """
        executions, history = await read_executions(flow_run_id, workspace_id)
        if activation_id is not None:
            executions = [
                execution
                for execution in executions
                if execution["execution"]["invocation"]["activation_id"]
                == str(activation_id)
            ]
        attempts = await gather(
            *(
                attempt_entry(execution, failed_only, workspace_id)
                for execution in executions
            )
        )
        return {"history": history, "attempts": list(attempts)}
