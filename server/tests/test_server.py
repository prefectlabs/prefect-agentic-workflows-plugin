"""Tests for the rules that apply to the server and to every tool."""

from typing import Any

from fastmcp import Client


async def test_server_reports_its_name_and_alpha_version(mcp_client: Client[Any]):
    initialize_result = mcp_client.initialize_result
    assert initialize_result is not None

    assert initialize_result.serverInfo.name == "prefect-agentic-workflows"
    assert initialize_result.serverInfo.version == "0.1.0a1"


async def test_server_instructions_say_it_is_alpha(mcp_client: Client[Any]):
    initialize_result = mcp_client.initialize_result
    assert initialize_result is not None

    assert "alpha" in (initialize_result.instructions or "")


async def test_every_tool_is_annotated_and_marked_alpha(mcp_client: Client[Any]):
    tools = await mcp_client.list_tools()

    assert tools
    for tool in tools:
        annotations = tool.annotations
        assert annotations is not None, tool.name
        assert annotations.readOnlyHint is not None, tool.name
        assert annotations.destructiveHint is not None, tool.name
        assert not (annotations.readOnlyHint and annotations.destructiveHint), tool.name
        assert "alpha" in (tool.description or ""), tool.name
