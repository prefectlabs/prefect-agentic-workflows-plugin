"""Integration tests for the flow tools against a real Cloud workspace."""

from typing import Any

from fastmcp import Client

FLOW_NAME = "prefect-agentic-workflows-integration"


async def test_get_or_create_flow_returns_the_same_flow_twice(
    mcp_client: Client[Any],
):
    first = await mcp_client.call_tool("get_or_create_flow", {"name": FLOW_NAME})
    second = await mcp_client.call_tool("get_or_create_flow", {"name": FLOW_NAME})

    assert first.structured_content is not None
    assert second.structured_content is not None
    assert second.structured_content["created"] is False
    assert (
        first.structured_content["flow"]["id"]
        == second.structured_content["flow"]["id"]
    )
