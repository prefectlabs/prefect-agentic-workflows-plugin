"""Tests for the rules that apply to the server and to every tool."""

from typing import Any

from fastmcp import Client


async def test_server_reports_its_name_and_alpha_version(mcp_client: Client[Any]):
    server_info = mcp_client.server_info
    assert server_info is not None

    assert server_info.name == "prefect-agentic-workflows"
    assert server_info.version == "0.1.0a1"


async def test_server_instructions_say_it_is_alpha(mcp_client: Client[Any]):
    assert "alpha" in (mcp_client.instructions or "")


async def test_every_tool_is_annotated_and_marked_alpha(mcp_client: Client[Any]):
    tools = await mcp_client.list_tools()

    assert tools
    for tool in tools:
        annotations = tool.annotations
        assert annotations is not None, tool.name
        assert annotations.read_only_hint is not None, tool.name
        assert annotations.destructive_hint is not None, tool.name
        assert not (annotations.read_only_hint and annotations.destructive_hint), (
            tool.name
        )
        assert "alpha" in (tool.description or ""), tool.name
