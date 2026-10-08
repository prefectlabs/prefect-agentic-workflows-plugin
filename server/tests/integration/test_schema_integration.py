"""Integration tests for the schema tools against a real Cloud workspace."""

from typing import Any

from fastmcp import Client


async def test_get_schema_reads_the_current_schema(mcp_client: Client[Any]):
    result = await mcp_client.call_tool("get_schema", {})

    body = result.structured_content
    assert body is not None
    assert body["schema_version"] == body["current_schema_version"]
    assert body["schema_version"] in body["supported_schema_versions"]
    assert body["schema"]["type"] == "object"
