"""Tools that read the model calls and tool calls of a run's agent nodes.

When a workspace records agent sessions, Prefect Cloud stores each attempt of
an agent node as a session, and each model call and tool call as one span of
that session in OTLP JSON. The agent session routes serve them.
"""

import json
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
# The most items that `get_run_activity` reads from one attempt's session.
MAX_ITEMS_READ = 2_000
# The longest `get_run_activity` result, in characters of compact JSON. Claude
# Code warns about a tool result above 10,000 tokens and cuts one at 25,000.
MAX_RESULT_CHARACTERS = 40_000
# The longest content value of an item in `get_run_activity`. A failed item
# keeps more, because its content usually explains the failure.
PREVIEW_CHARACTERS = 300
FAILED_PREVIEW_CHARACTERS = 2_000
# The marker that the collector also appends to a value it cut.
TRUNCATED_MARKER = "…[truncated]"

ACTIVITY_ATTRIBUTE = "prefect.agent.activity."
SPAN_STATUS_ERROR = 2
MODEL_CALL = "chat"
TOOL_CALL = "execute_tool"
# The content attributes of each operation, and the field that returns each.
CONTENT_FIELDS = {
    MODEL_CALL: {
        "gen_ai.system_instructions": "system_instructions",
        "gen_ai.input.messages": "input",
        "gen_ai.output.messages": "output",
    },
    TOOL_CALL: {
        "gen_ai.tool.call.arguments": "arguments",
        "gen_ai.tool.call.result": "result",
    },
}


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


def cut_content(value: str | None, limit: int) -> str | None:
    """Return `value` cut to `limit` characters, marked when cut."""
    if value is None or len(value) <= limit:
        return value
    return value[:limit] + TRUNCATED_MARKER


def model_output_text(output: str) -> str:
    """Return the text and the tool calls of a model call's output messages.

    The collector cuts a long value, which leaves it invalid JSON, so a value
    that doesn't parse is returned as it is.
    """
    try:
        messages = json.loads(output)
    except json.JSONDecodeError:
        return output
    lines = []
    for message in messages:
        for part in message["parts"]:
            if part["type"] == "text":
                lines.append(part["content"])
            elif part["type"] == "tool_call":
                # pydantic-ai records arguments as JSON text or as an object.
                arguments = part.get("arguments", "")
                if not isinstance(arguments, str):
                    arguments = json.dumps(arguments)
                lines.append(f"{part['name']}({arguments})")
    return "\n".join(lines)


def activity_entry(item: dict[str, Any]) -> dict[str, Any]:
    """Return one model call or tool call from an item of a session's activity.

    A call with only an in-flight copy of its span has no end time and no
    status yet.
    """
    span = item["operation"]
    attributes = span_attributes(span)
    operation = attributes[f"{ACTIVITY_ATTRIBUTE}operation"]
    if item["kind"] == "operation_started":
        status = "in_flight"
    elif span["status"]["code"] == SPAN_STATUS_ERROR:
        status = "error"
    else:
        status = "ok"
    limit = FAILED_PREVIEW_CHARACTERS if status == "error" else PREVIEW_CHARACTERS
    entry = {
        "record_id": item["id"],
        "operation": operation,
        "status": status,
        "started_at": span_time(span["startTimeUnixNano"]),
        "ended_at": span_time(span.get("endTimeUnixNano")),
        "failure_type": attributes.get(f"{ACTIVITY_ATTRIBUTE}failure_type"),
        "retryable": attributes.get(f"{ACTIVITY_ATTRIBUTE}retryable"),
        "content": item["content"],
    }
    if operation == MODEL_CALL:
        output = attributes.get("gen_ai.output.messages")
        return {
            **entry,
            "model": attributes.get(f"{ACTIVITY_ATTRIBUTE}model"),
            "output": None
            if output is None
            else cut_content(model_output_text(output), limit),
        }
    return {
        **entry,
        "tool": attributes.get(f"{ACTIVITY_ATTRIBUTE}tool"),
        "mcp_server": attributes.get(f"{ACTIVITY_ATTRIBUTE}mcp_server"),
        "arguments": cut_content(attributes.get("gen_ai.tool.call.arguments"), limit),
        "result": cut_content(attributes.get("gen_ai.tool.call.result"), limit),
    }


def json_length(value: Any) -> int:
    """Return the length of `value` as compact JSON, the form a tool returns."""
    return len(json.dumps(value, separators=(",", ":"), ensure_ascii=False))


def fit_activity(history: dict[str, Any], attempts: list[dict[str, Any]]) -> None:
    """Leave items out of `attempts` until the result fits the character limit.

    The failed items are kept first, then the other items, the latest first in
    each group, until the next item doesn't fit. Each attempt's
    `activity_left_out` counts the items that its list leaves out.
    """
    activities = [attempt["activity"] or [] for attempt in attempts]
    for attempt, activity in zip(attempts, activities, strict=True):
        if attempt["activity"] is not None:
            attempt["activity"] = []
            attempt["activity_left_out"] = len(activity)
    room = MAX_RESULT_CHARACTERS - json_length(
        {"history": history, "attempts": attempts}
    )
    positions = [
        (index, position)
        for index, activity in enumerate(activities)
        for position in range(len(activity))
    ]
    positions.sort(
        key=lambda place: (activities[place[0]][place[1]]["status"] == "error", place),
        reverse=True,
    )
    kept: set[tuple[int, int]] = set()
    for index, position in positions:
        # One more character for the comma between items.
        length = json_length(activities[index][position]) + 1
        if length > room:
            break
        room -= length
        kept.add((index, position))
    for index, (attempt, activity) in enumerate(zip(attempts, activities, strict=True)):
        if attempt["activity"] is None:
            continue
        attempt["activity"] = [
            entry
            for position, entry in enumerate(activity)
            if (index, position) in kept
        ]
        attempt["activity_left_out"] = len(activity) - len(attempt["activity"])


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
    """Add the run activity tools to `mcp`."""

    async def read_json(
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
            page = await read_json(path, params, workspace_id)
            executions.extend(page["executions"])
            histories.append(page["history"])
            if page["next_cursor"] is None:
                return executions, merged_history(histories)
            params = {"limit": PAGE_LIMIT, "cursor": page["next_cursor"]}

    async def read_activity(
        session_id: str, workspace_id: WorkspaceId
    ) -> tuple[list[dict[str, Any]], bool]:
        """Return a session's first model calls and tool calls, and whether the
        session has more than `MAX_ITEMS_READ`."""
        path = f"/agent-sessions/{session_id}/activity"
        params: dict[str, Any] = {
            "operation": f"{MODEL_CALL},{TOOL_CALL}",
            "limit": PAGE_LIMIT,
        }
        activity: list[dict[str, Any]] = []
        while True:
            page = await read_json(path, params, workspace_id)
            activity.extend(map(activity_entry, page["activity"]))
            more = page["next_cursor"] is not None
            if not more or len(activity) >= MAX_ITEMS_READ:
                return activity, more
            params = {**params, "cursor": page["next_cursor"]}

    async def attempt_entry(
        execution: dict[str, Any], workspace_id: WorkspaceId
    ) -> dict[str, Any]:
        attempt = execution["execution"]
        invocation = attempt["invocation"]
        session = execution["session"]
        activity: list[dict[str, Any]] | None = None
        unread = False
        if session["status"] == "available":
            activity, unread = await read_activity(session["id"], workspace_id)
        return {
            "attempt_id": attempt["id"],
            "node_id": invocation["node_id"],
            "activation_id": invocation["activation_id"],
            "task_run_id": invocation["task_run_id"],
            "attempt_ordinal": attempt["attempt_ordinal"],
            "status": execution["status"],
            "started_at": execution["started_at"],
            "ended_at": execution["ended_at"],
            "activity": activity,
            "activity_left_out": 0,
            "activity_unread": unread,
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
        workspace_id: WorkspaceId = None,
    ) -> dict[str, Any]:
        """Read the model calls and tool calls that a run's agent nodes made.

        Use it to explain why a run or a node failed: read the run with
        `get_run`, then pass a failed agent node's `activation_id`. Read one
        call in full with `get_activity_record`.

        Returns `attempts`, one for each attempt of an agent node, in start
        order, with its `node_id`, `activation_id`, `status`, and times. An
        attempt with the status `superseded` was followed by a retry.

        `activity` lists the attempt's calls in start order. Each has a
        `record_id`, an `operation` of `chat` for a model call or
        `execute_tool` for a tool call, and a `status` of `ok`, `error`, or
        `in_flight`. A failed call has a `failure_type`, such as
        `tool_error`, `mcp_error`, `timeout`, or `rate_limit`, and
        `retryable`. A model call has the `model` and its `output`: the text
        it wrote and the tools it called. A tool call has the `tool`, the
        `mcp_server` when Prefect Cloud recorded one, and its `arguments` and
        `result` as text. Content is cut to 300 characters, or 2,000 for a
        failed call, with `…[truncated]` at the end. `activity` is null when
        Prefect Cloud has no session for the attempt yet.

        The result is at most 40,000 characters. To fit, it leaves out calls,
        and keeps the failed calls and the latest calls first.
        `activity_left_out` counts the calls that an attempt's list leaves
        out. Pass an `activation_id` to read fewer attempts.
        `activity_unread` is true when the attempt made more than 2,000 calls,
        and the list covers only the first 2,000.

        Each call's `content` says whether its content is there. `withheld`
        means the user's role lacks the "View agent prompts, outputs, and
        tool data" permission, which the Admin and Developer roles have. Tell
        the user that, instead of guessing at the cause. `unavailable` means
        Prefect Cloud could no longer decrypt it.

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
        attempts = list(
            await gather(
                *(attempt_entry(execution, workspace_id) for execution in executions)
            )
        )
        fit_activity(history, attempts)
        return {"history": history, "attempts": attempts}

    @tool(mcp, read_only=True)
    async def get_activity_record(
        attempt_id: Annotated[
            UUID,
            Field(description="ID of the attempt, from `get_run_activity`."),
        ],
        record_id: Annotated[
            UUID,
            Field(
                description=(
                    "ID of one model call or tool call, from an `activity` "
                    "item's `record_id` in `get_run_activity`."
                ),
            ),
        ],
        workspace_id: WorkspaceId = None,
    ) -> dict[str, Any]:
        """Read the full content of one model call or tool call.

        Use it when an item of `get_run_activity` is cut and the cut part
        can explain the failure.

        A model call (`operation` of `chat`) returns `system_instructions`,
        `input`, and `output` as the JSON text of the model's messages.
        `input` has the node's request and every earlier message of the
        attempt. A tool call (`operation` of `execute_tool`) returns its
        `arguments` and `result`. Prefect Cloud keeps at most 16 KiB of each
        value, and ends a longer one with `…[truncated]`. `content` says
        whether the values are there, as in `get_run_activity`.
        """
        path = f"/agent-sessions/{attempt_id}/records/{record_id}/revisions/{record_id}"
        record = await read_json(path, {}, workspace_id)
        [resource_spans] = record["otlp"]["resourceSpans"]
        [scope_spans] = resource_spans["scopeSpans"]
        [span] = scope_spans["spans"]
        attributes = span_attributes(span)
        operation = attributes[f"{ACTIVITY_ATTRIBUTE}operation"]
        return {
            "record_id": record["record_id"],
            "operation": operation,
            "content": record["content"],
            **{
                field: attributes.get(key)
                for key, field in CONTENT_FIELDS[operation].items()
            },
        }
