"""Tests for the `get_or_create_flow` tool."""

from typing import Any

import respx
from fastmcp import Client
from support import flow_response, request_body


async def test_get_or_create_flow_returns_an_existing_flow_without_creating_one(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get("/flows/name/daily-summary").respond(200, json=flow_response())
    create = cloud_api.post("/flows/")

    result = await mcp_client.call_tool("get_or_create_flow", {"name": "daily-summary"})

    assert result.structured_content == {"created": False, "flow": flow_response()}
    assert not create.called


async def test_get_or_create_flow_creates_a_missing_flow_with_its_tags(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get("/flows/name/daily-summary").respond(404, json={"detail": "Nope"})
    flow = flow_response(tags=["agentic"])
    create = cloud_api.post("/flows/").respond(201, json=flow)

    result = await mcp_client.call_tool(
        "get_or_create_flow", {"name": "daily-summary", "tags": ["agentic"]}
    )

    assert result.structured_content == {"created": True, "flow": flow}
    assert request_body(create.calls.last.request) == {
        "name": "daily-summary",
        "tags": ["agentic"],
    }


async def test_get_or_create_flow_escapes_the_name_in_the_lookup_path(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    lookup = cloud_api.get(url__regex=r"/flows/name/.+").respond(
        200, json=flow_response("ops/daily summary")
    )

    await mcp_client.call_tool("get_or_create_flow", {"name": "ops/daily summary"})

    assert lookup.calls.last.request.url.raw_path.endswith(
        b"/flows/name/ops%2Fdaily%20summary"
    )
