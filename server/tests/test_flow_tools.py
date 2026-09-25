"""Tests for the `get_or_create_flow` tool."""

import json
from typing import Any

import respx
from fastmcp import Client

from support import error_text

FLOW_ID = "33333333-3333-3333-3333-333333333333"


def flow_response(
    flow_id: str, name: str, tags: list[str] | None = None
) -> dict[str, Any]:
    """Return a flow in the shape Cloud returns from the flow routes."""
    return {
        "id": flow_id,
        "created": "2026-09-25T12:00:00Z",
        "updated": "2026-09-25T12:00:00Z",
        "name": name,
        "tags": tags or [],
        "labels": {},
    }


async def test_get_or_create_flow_returns_an_existing_flow_without_creating_one(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    flow = flow_response(FLOW_ID, "daily-summary")
    cloud_api.get("/flows/name/daily-summary").respond(200, json=flow)
    create = cloud_api.post("/flows/")

    result = await mcp_client.call_tool("get_or_create_flow", {"name": "daily-summary"})

    assert result.structured_content == {"created": False, "flow": flow}
    assert not create.called


async def test_get_or_create_flow_creates_a_missing_flow_with_its_tags(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get("/flows/name/daily-summary").respond(
        404, json={"detail": "Flow not found"}
    )
    flow = flow_response(FLOW_ID, "daily-summary", tags=["agentic"])
    create = cloud_api.post("/flows/").respond(201, json=flow)

    result = await mcp_client.call_tool(
        "get_or_create_flow", {"name": "daily-summary", "tags": ["agentic"]}
    )

    assert result.structured_content == {"created": True, "flow": flow}
    assert json.loads(create.calls.last.request.content) == {
        "name": "daily-summary",
        "tags": ["agentic"],
    }


async def test_get_or_create_flow_returns_the_same_flow_id_on_a_second_call(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    flow = flow_response(FLOW_ID, "daily-summary")
    cloud_api.get("/flows/name/daily-summary").mock(
        side_effect=[
            respx.MockResponse(404, json={"detail": "Flow not found"}),
            respx.MockResponse(200, json=flow),
        ]
    )
    cloud_api.post("/flows/").respond(201, json=flow)

    first = await mcp_client.call_tool("get_or_create_flow", {"name": "daily-summary"})
    second = await mcp_client.call_tool(
        "get_or_create_flow", {"name": "daily-summary"}
    )

    assert first.structured_content is not None
    assert second.structured_content is not None
    assert first.structured_content["created"] is True
    assert second.structured_content["created"] is False
    assert (
        first.structured_content["flow"]["id"]
        == second.structured_content["flow"]["id"]
        == FLOW_ID
    )


async def test_get_or_create_flow_escapes_the_name_in_the_lookup_path(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    flow = flow_response(FLOW_ID, "ops/daily summary")
    lookup = cloud_api.get(url__regex=r"/flows/name/.+").respond(200, json=flow)

    await mcp_client.call_tool("get_or_create_flow", {"name": "ops/daily summary"})

    assert lookup.calls.last.request.url.raw_path.endswith(
        b"/flows/name/ops%2Fdaily%20summary"
    )


async def test_get_or_create_flow_reports_a_failed_lookup(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get("/flows/name/daily-summary").respond(
        500, json={"detail": "database unavailable"}
    )

    result = await mcp_client.call_tool(
        "get_or_create_flow", {"name": "daily-summary"}, raise_on_error=False
    )

    message = error_text(result)
    assert "500" in message
    assert "database unavailable" in message


async def test_get_or_create_flow_writes_but_is_not_destructive(
    mcp_client: Client[Any],
):
    tools = {tool.name: tool for tool in await mcp_client.list_tools()}

    annotations = tools["get_or_create_flow"].annotations
    assert annotations is not None
    assert annotations.readOnlyHint is False
    assert annotations.destructiveHint is False
