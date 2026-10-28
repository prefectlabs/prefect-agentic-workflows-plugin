"""Integration test for the MCP connection tools against a real Cloud workspace.

The account needs MCP connections enabled. The test saves a connection to a
public MCP server that needs no sign-in, checks it, and deletes it.
"""

import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from fastmcp import Client

from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi

# DeepWiki's MCP server lists its tools without credentials.
PUBLIC_SERVER_URL = "https://mcp.deepwiki.com/mcp"


@pytest.fixture
async def created_ids() -> AsyncIterator[list[str]]:
    """Collect the IDs of connections a test creates, and delete them after it."""
    ids: list[str] = []
    yield ids
    api = WorkspaceApi()
    for connection_id in ids:
        await api.call("DELETE", f"/agentic/mcp-connections/{connection_id}")


async def test_a_public_server_is_saved_and_checked_without_setup(
    mcp_client: Client[Any], created_ids: list[str]
):
    discovered = await mcp_client.call_tool(
        "discover_mcp_server", {"url": PUBLIC_SERVER_URL}
    )
    assert discovered.structured_content == {"requires_auth": False, "oauth": None}

    created = await mcp_client.call_tool(
        "create_mcp_connection",
        {"name": f"DeepWiki (test {uuid.uuid4().hex[:8]})", "url": PUBLIC_SERVER_URL},
    )
    body = created.structured_content
    assert body is not None
    connection_id = body["connection"]["id"]
    created_ids.append(connection_id)
    assert body["setup"] == "not_needed"
    assert body["connection"]["auth_method"] == "none"
    assert body["connection"]["status"]["state"] == "not_verified"

    checked = await mcp_client.call_tool(
        "check_mcp_connection", {"connection_id": connection_id}
    )
    assert checked.structured_content is not None
    assert checked.structured_content["last_check"]["outcome"] == "succeeded"
    assert checked.structured_content["status"]["state"] == "authorized"

    listed = await mcp_client.call_tool("list_mcp_connections", {})
    assert listed.structured_content is not None
    assert connection_id in {
        connection["id"] for connection in listed.structured_content["mcp_connections"]
    }
