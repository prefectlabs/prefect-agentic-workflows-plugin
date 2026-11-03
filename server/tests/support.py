"""Constants, payloads, and helpers shared by the mocked tests."""

import json
from typing import Any, Literal

import httpx
from fastmcp.client.client import CallToolResult
from fastmcp.client.elicitation import (
    ElicitationHandler,
    ElicitRequestParams,
    ElicitResult,
    RequestContext,
)
from mcp.types import ElicitRequestURLParams, TextContent

ACCOUNT_ID = "11111111-1111-1111-1111-111111111111"
WORKSPACE_ID = "22222222-2222-2222-2222-222222222222"
WORKSPACE_API_URL = (
    f"https://api.prefect.cloud/api/accounts/{ACCOUNT_ID}/workspaces/{WORKSPACE_ID}"
)
WORKSPACE_API_PATH = httpx.URL(WORKSPACE_API_URL).path
API_KEY = "pnu_test_key"
ME_WORKSPACES_URL = "https://api.prefect.cloud/api/me/workspaces"
WORKSPACE_NAME = "test-account/test-workspace"

FLOW_ID = "33333333-3333-3333-3333-333333333333"
VERSION_ID = "44444444-4444-4444-4444-444444444444"
OLDER_VERSION_ID = "55555555-5555-5555-5555-555555555555"
FLOW_RUN_ID = "66666666-6666-6666-6666-666666666666"
SNAPSHOT_ID = "77777777-7777-7777-7777-777777777777"
ACTIVATION_ID = "88888888-8888-8888-8888-888888888888"
SCHEDULE_ID = "99999999-9999-9999-9999-999999999999"
# A session id is its agent attempt's id, a UUIDv7.
SESSION_ID = "01a19e47-660c-7358-a65c-883f5130bbde"
RECORD_ID = "01a19e47-d1a4-7c06-9a98-373c4c6fb620"

PLAN_PATH = f"/flows/{FLOW_ID}/execution-plan"
VERSIONS_PATH = f"{PLAN_PATH}/versions"
SCHEDULES_PATH = f"{PLAN_PATH}/schedules"
RUNS_PATH = f"{PLAN_PATH}/runs"
RUN_PATH = f"/flow_runs/{FLOW_RUN_ID}/execution-plan"
EXECUTIONS_PATH = f"/flow_runs/{FLOW_RUN_ID}/agent-executions"

PLAN: dict[str, Any] = {
    "schema_version": "0.1",
    "kind": "execution_plan",
    "nodes": {"summarize": {"kind": "agent", "prompt": "Summarize the failures."}},
}


def me_workspace(
    account_id: str = ACCOUNT_ID, workspace_id: str = WORKSPACE_ID
) -> dict[str, Any]:
    """Return one entry of `GET /api/me/workspaces` in the shape Cloud returns."""
    return {
        "account_id": account_id,
        "account_name": "Test Account",
        "account_handle": "test-account",
        "workspace_id": workspace_id,
        "workspace_name": "Test Workspace",
        "workspace_handle": "test-workspace",
    }


def schema_response(version: str = "0.1") -> dict[str, Any]:
    """Return a `GET /execution-plans/schema` body in the shape Cloud returns."""
    return {
        "schema_version": version,
        "schema": {
            "type": "object",
            "properties": {"schema_version": {"const": version}},
            "required": ["schema_version", "kind", "nodes"],
        },
        "supported_schema_versions": [version],
        "current_schema_version": version,
        "is_current": True,
        "is_deprecated": False,
        "document_shape_only": True,
        "validation_guidance": "Call POST /execution-plans/validate before publishing.",
    }


def flow_response(name: str = "daily-summary", **overrides: Any) -> dict[str, Any]:
    """Return a flow in the shape Cloud returns from the flow routes."""
    return {"id": FLOW_ID, "name": name, "tags": [], "labels": {}, **overrides}


def version_response(version_id: str = VERSION_ID) -> dict[str, Any]:
    """Return a plan version in the shape Cloud returns from the version routes."""
    return {
        "id": version_id,
        "flow_id": FLOW_ID,
        "schema_version": "0.1",
        "semantic_hash": f"hash-{version_id[:4]}",
        "created": "2026-09-25T12:00:00Z",
        "created_by": {"type": "USER", "display_value": "marvin"},
        "plan": PLAN,
        "output_schemas": {},
    }


def version_summary(version_id: str = VERSION_ID) -> dict[str, Any]:
    """Return a version as the tools return it, without the plan document."""
    version = version_response(version_id)
    return {
        key: value
        for key, value in version.items()
        if key not in ("plan", "layout", "output_schemas")
    }


def active_state_summary(version_id: str | None = VERSION_ID) -> dict[str, Any]:
    """Return a flow's active state as the tools return it, without the plan."""
    if version_id is None:
        return {"active_version": None}
    state = active_state_response(version_id)
    return {
        "active_version": {
            key: value
            for key, value in state["active_version"].items()
            if key not in ("plan", "layout", "output_schemas")
        }
    }


def active_state_response(version_id: str | None = VERSION_ID) -> dict[str, Any]:
    """Return the active-plan state Cloud returns for a flow."""
    if version_id is None:
        return {"active_version": None}
    return {
        "active_version": {
            **version_response(version_id),
            "activated": "2026-09-25T12:01:00Z",
        }
    }


def schedule_response(**overrides: Any) -> dict[str, Any]:
    """Return a schedule body in the shape Cloud returns."""
    return {
        "id": SCHEDULE_ID,
        "flow_id": FLOW_ID,
        "name": "weekday-mornings",
        "active": True,
        "schedule": {"type": "cron", "cron": "0 9 * * 1-5", "timezone": "UTC"},
        "parameters": {"channel": "#ops"},
        "next_scheduled_time": "2026-09-28T09:00:00Z",
        "last_created_flow_run_id": FLOW_RUN_ID,
        "last_error": "Parameter 'channel' is required.",
        **overrides,
    }


def flow_run_response(**overrides: Any) -> dict[str, Any]:
    """Return a `POST .../execution-plan/runs` body in the shape Cloud returns."""
    return {
        "id": FLOW_RUN_ID,
        "flow_id": FLOW_ID,
        "name": "crimson-otter",
        "parameters": {},
        "idempotency_key": None,
        "state_type": "RUNNING",
        "state_name": "Running",
        "created": "2026-11-02T22:00:00Z",
        "start_time": "2026-11-02T22:00:01Z",
        "execution_plan_snapshot_id": SNAPSHOT_ID,
        **overrides,
    }


def node_observation(status: str = "running", **overrides: Any) -> dict[str, Any]:
    """Return one node of a run observation in the shape Cloud returns."""
    return {
        "node": "summarize",
        "kind": "AgentNode",
        "status": status,
        "activation_id": ACTIVATION_ID,
        "outputs": [{"output": "done", "status": "pending"}],
        **overrides,
    }


def run_observation(
    status: str = "running", nodes: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    """Return a `GET /flow_runs/{id}/execution-plan` body in the shape Cloud returns."""
    return {
        "snapshot": {
            "id": SNAPSHOT_ID,
            "flow_run_id": FLOW_RUN_ID,
            "execution_plan_version_id": VERSION_ID,
        },
        "status": status,
        "nodes": nodes if nodes is not None else [node_observation()],
        "edges": [],
        "outputs": [{"output": "summary", "status": "waiting"}],
        "diagnostics": [],
    }


def run_summary(flow_run_id: str = FLOW_RUN_ID, **overrides: Any) -> dict[str, Any]:
    """Return one run summary from `POST .../execution-plan/runs/summaries`."""
    return {
        "flow_run_id": flow_run_id,
        "execution_plan_version_id": VERSION_ID,
        "status": "completed",
        "outputs": [
            {
                "output": "reply",
                "status": "available",
                "provenance": [{"node": "draft", "output": "drafted"}],
            }
        ],
        "last_nodes": [{"node": "draft", "selected_outputs": ["drafted"]}],
        **overrides,
    }


def agent_execution(
    session_id: str = SESSION_ID,
    activation_id: str = ACTIVATION_ID,
    session_status: str = "available",
    **overrides: Any,
) -> dict[str, Any]:
    """Return one row of `GET /flow_runs/{id}/agent-executions` as Cloud returns it."""
    return {
        "execution": {
            "id": session_id,
            "attempt_ordinal": 1,
            "invocation": {
                "activation_id": activation_id,
                "execution_plan_version_id": VERSION_ID,
                "flow_id": FLOW_ID,
                "flow_run_id": FLOW_RUN_ID,
                "node_id": "draft",
                "snapshot_id": SNAPSHOT_ID,
                "task_run_id": activation_id,
            },
        },
        "session": {
            "id": session_id if session_status == "available" else None,
            "status": session_status,
        },
        "status": "failed",
        "started_at": "2026-11-02T22:00:41.740717Z",
        "ended_at": "2026-11-02T22:00:55.957805Z",
        **overrides,
    }


def executions_page(
    executions: list[dict[str, Any]], next_cursor: str | None = None
) -> dict[str, Any]:
    """Return a page of `GET /flow_runs/{id}/agent-executions` as Cloud returns it."""
    return {
        "flow_run_id": FLOW_RUN_ID,
        "executions": executions,
        "history": {"coverage": "complete", "issues": []},
        "next_cursor": next_cursor,
        "observed_at": "2026-11-02T23:20:28.645927Z",
    }


def activity_item(
    attributes: dict[str, Any],
    *,
    failed: bool = False,
    in_flight: bool = False,
    content: str = "available",
    record_id: str = RECORD_ID,
) -> dict[str, Any]:
    """Return one item of a session's activity as Cloud returns it.

    Its span is in OTLP JSON. `attributes` are the span's string and boolean
    attributes, and a value of None leaves the attribute out.
    """
    span: dict[str, Any] = {
        "name": "prefect.agent.activity",
        "attributes": [
            {
                "key": key,
                "value": {"boolValue": value}
                if isinstance(value, bool)
                else {"stringValue": value},
            }
            for key, value in attributes.items()
            if value is not None
        ],
        "startTimeUnixNano": "1793646051610300000",
    }
    if not in_flight:
        span["endTimeUnixNano"] = "1793646051854886000"
        span["status"] = {"code": 2 if failed else 0, "message": ""}
    return {
        "id": record_id,
        "kind": "operation_started" if in_flight else "operation_completed",
        "content": content,
        "operation": span,
    }


def failure_attributes(failure_type: str) -> dict[str, Any]:
    return {
        "prefect.agent.activity.failure_type": failure_type,
        "prefect.agent.activity.retryable": True,
    }


def tool_call_item(
    tool: str = "search",
    *,
    failed: bool = False,
    in_flight: bool = False,
    content: str = "available",
    mcp_server: str | None = None,
    arguments: str | None = '{"query": "refund policy"}',
    result: str | None = "No results.",
    record_id: str = RECORD_ID,
) -> dict[str, Any]:
    """Return one `execute_tool` item of a session's activity as Cloud returns it,
    with the attributes the collector writes."""
    return activity_item(
        {
            "prefect.agent.activity.operation": "execute_tool",
            "prefect.agent.activity.tool": tool,
            "prefect.agent.activity.mcp_server": mcp_server,
            **(failure_attributes("tool_error") if failed else {}),
            "gen_ai.tool.call.arguments": arguments,
            "gen_ai.tool.call.result": result,
        },
        failed=failed,
        in_flight=in_flight,
        content=content,
        record_id=record_id,
    )


def model_output(*parts: dict[str, Any]) -> str:
    """Return `gen_ai.output.messages` for one assistant message with `parts`."""
    return json.dumps([{"role": "assistant", "parts": list(parts)}])


def model_call_item(
    output: str | None = None,
    *,
    failed: bool = False,
    content: str = "available",
    record_id: str = RECORD_ID,
) -> dict[str, Any]:
    """Return one `chat` item of a session's activity as Cloud returns it, with
    the attributes the collector writes."""
    if output is None and not failed:
        output = model_output({"type": "text", "content": "Searching."})
    return activity_item(
        {
            "prefect.agent.activity.operation": "chat",
            "prefect.agent.activity.model": "claude-sonnet-4-5",
            **(failure_attributes("rate_limit") if failed else {}),
            "gen_ai.output.messages": output,
        },
        failed=failed,
        content=content,
        record_id=record_id,
    )


def activity_record(item: dict[str, Any], attributes: dict[str, str]) -> dict[str, Any]:
    """Return the records route's answer for the span of an activity `item`,
    with `attributes` added to the span."""
    span = item["operation"]
    span = {
        **span,
        "attributes": span["attributes"]
        + [
            {"key": key, "value": {"stringValue": value}}
            for key, value in attributes.items()
        ],
    }
    return {
        "session_id": SESSION_ID,
        "record_id": item["id"],
        "revision_id": item["id"],
        "source_id": SESSION_ID,
        "signal": "span",
        "content": item["content"],
        "otlp": {
            "resourceSpans": [
                {"resource": {}, "scopeSpans": [{"scope": {}, "spans": [span]}]}
            ]
        },
    }


def activity_page(
    items: list[dict[str, Any]], next_cursor: str | None = None
) -> dict[str, Any]:
    """Return a page of `GET /agent-sessions/{id}/activity` as Cloud returns it."""
    return {"session_id": SESSION_ID, "activity": items, "next_cursor": next_cursor}


def request_body(request: httpx.Request) -> Any:
    """Return the JSON body of a request, or None when it has no body."""
    return json.loads(request.content) if request.content else None


def error_text(result: CallToolResult) -> str:
    """Return the message of a tool call that failed."""
    assert result.is_error, f"expected an error, got {result.structured_content!r}"
    content = result.content[0]
    assert isinstance(content, TextContent)
    return content.text


def browser_user(
    action: Literal["accept", "decline", "cancel"], opened_urls: list[str]
) -> ElicitationHandler:
    """Return an elicitation handler that records each URL and answers `action`."""

    async def handle(
        message: str,
        response_type: type | None,
        params: ElicitRequestParams,
        context: RequestContext,
    ) -> ElicitResult[dict[str, Any]]:
        assert isinstance(params, ElicitRequestURLParams)
        opened_urls.append(params.url)
        return ElicitResult(action=action)

    return handle
