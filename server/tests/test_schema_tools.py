"""Tests for the `get_schema` and `validate_plan` tools."""

import json
from typing import Any

import respx
from fastmcp import Client

from support import error_text, schema_response


async def test_get_schema_returns_the_current_schema_by_default(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    result = await mcp_client.call_tool("get_schema", {})

    assert result.structured_content == schema_response()
    last_request = cloud_api.routes["schema"].calls.last.request
    assert "version" not in last_request.url.params
    assert last_request.headers["Authorization"] == "Bearer pnu_test_key"


async def test_get_schema_reads_the_requested_version(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    older = schema_response("0.0")
    older["is_current"] = False
    older["is_deprecated"] = True
    cloud_api.get("/execution-plans/schema", params={"version": "0.0"}).respond(
        200, json=older
    )

    result = await mcp_client.call_tool("get_schema", {"version": "0.0"})

    assert result.structured_content == older


async def test_get_schema_reports_an_unknown_version_with_the_supported_versions(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get("/execution-plans/schema", params={"version": "9.9"}).respond(
        404,
        json={
            "detail": {
                "message": "Execution plan schema version not found.",
                "supported_schema_versions": ["0.1"],
            }
        },
    )

    result = await mcp_client.call_tool(
        "get_schema", {"version": "9.9"}, raise_on_error=False
    )

    assert result.is_error
    message = error_text(result)
    assert "Execution plan schema version not found." in message
    assert "'0.1'" in message


PLAN: dict[str, Any] = {
    "schema_version": "0.1",
    "kind": "execution_plan",
    "nodes": [{"id": "summarize", "type": "agent"}],
}


async def test_validate_plan_sends_the_plan_and_returns_a_valid_result(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post("/execution-plans/validate").respond(
        200, json={"valid": True, "errors": []}
    )

    result = await mcp_client.call_tool("validate_plan", {"plan": PLAN})

    assert result.structured_content == {"valid": True, "errors": []}
    assert json.loads(route.calls.last.request.content) == {"plan": PLAN}


async def test_validate_plan_returns_every_validation_error(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    errors = [
        {
            "code": "missing",
            "phase": "document_shape",
            "path": ["nodes", 0, "prompt"],
            "message": "Field required",
        },
        {
            "code": "unreachable_node",
            "phase": "semantic",
            "path": ["nodes", 0],
            "message": "Node 'summarize' is not reachable from a start node.",
        },
    ]
    cloud_api.post("/execution-plans/validate").respond(
        200, json={"valid": False, "errors": errors}
    )

    result = await mcp_client.call_tool("validate_plan", {"plan": PLAN})

    assert not result.is_error
    assert result.structured_content == {"valid": False, "errors": errors}


async def test_validate_plan_reports_a_rejected_request(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post("/execution-plans/validate").respond(
        422, json={"detail": "plan must be an object"}
    )

    result = await mcp_client.call_tool(
        "validate_plan", {"plan": PLAN}, raise_on_error=False
    )

    message = error_text(result)
    assert "422" in message
    assert "plan must be an object" in message


async def test_schema_tools_are_read_only(mcp_client: Client[Any]):
    tools = {tool.name: tool for tool in await mcp_client.list_tools()}

    for name in ("get_schema", "validate_plan"):
        annotations = tools[name].annotations
        assert annotations is not None
        assert annotations.readOnlyHint is True
        assert annotations.destructiveHint is False
        assert "alpha" in (tools[name].description or "")
