"""Constants, payloads, and helpers shared by the mocked tests."""

import json
from typing import Any

import httpx
from fastmcp.client.client import CallToolResult
from mcp.types import TextContent

ACCOUNT_ID = "11111111-1111-1111-1111-111111111111"
WORKSPACE_ID = "22222222-2222-2222-2222-222222222222"
WORKSPACE_API_URL = (
    f"https://api.prefect.cloud/api/accounts/{ACCOUNT_ID}/workspaces/{WORKSPACE_ID}"
)
WORKSPACE_API_PATH = httpx.URL(WORKSPACE_API_URL).path
API_KEY = "pnu_test_key"

FLOW_ID = "33333333-3333-3333-3333-333333333333"
VERSION_ID = "44444444-4444-4444-4444-444444444444"
OLDER_VERSION_ID = "55555555-5555-5555-5555-555555555555"
FLOW_RUN_ID = "66666666-6666-6666-6666-666666666666"
SNAPSHOT_ID = "77777777-7777-7777-7777-777777777777"
ACTIVATION_ID = "88888888-8888-8888-8888-888888888888"
SCHEDULE_ID = "99999999-9999-9999-9999-999999999999"

PLAN_PATH = f"/flows/{FLOW_ID}/execution-plan"
VERSIONS_PATH = f"{PLAN_PATH}/versions"
SCHEDULES_PATH = f"{PLAN_PATH}/schedules"
RUN_PATH = f"/flow_runs/{FLOW_RUN_ID}/execution-plan"

PLAN: dict[str, Any] = {
    "schema_version": "0.1",
    "kind": "execution_plan",
    "nodes": {"summarize": {"kind": "agent", "prompt": "Summarize the failures."}},
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


def request_body(request: httpx.Request) -> Any:
    """Return the JSON body of a request, or None when it has no body."""
    return json.loads(request.content) if request.content else None


def error_text(result: CallToolResult) -> str:
    """Return the message of a tool call that failed."""
    assert result.is_error, f"expected an error, got {result.structured_content!r}"
    content = result.content[0]
    assert isinstance(content, TextContent)
    return content.text
