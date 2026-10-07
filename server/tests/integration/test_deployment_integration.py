"""Integration test for `list_deployments` against a real Cloud workspace."""

from typing import Any

from fastmcp import Client


async def test_list_deployments_returns_ids_and_names(mcp_client: Client[Any]):
    result = await mcp_client.call_tool("list_deployments", {})

    body = result.structured_content
    assert body is not None
    for deployment in body["deployments"]:
        assert set(deployment) == {"id", "name", "flow_name", "description"}
