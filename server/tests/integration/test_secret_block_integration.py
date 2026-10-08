"""Integration test for `list_secret_blocks` against a real Cloud workspace."""

from typing import Any

from fastmcp import Client


async def test_list_secret_blocks_returns_only_names_and_ids(mcp_client: Client[Any]):
    result = await mcp_client.call_tool("list_secret_blocks", {})

    body = result.structured_content
    assert body is not None
    for block in body["secret_blocks"]:
        assert set(block) == {"name", "id"}
