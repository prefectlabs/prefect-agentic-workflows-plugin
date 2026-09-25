"""Integration tests for the schedule tools against a real Cloud workspace."""

from typing import Any
from uuid import uuid4

from fastmcp import Client

from support import error_text


async def test_list_schedules_reports_a_flow_that_does_not_exist(
    mcp_client: Client[Any],
):
    result = await mcp_client.call_tool(
        "list_schedules", {"flow_id": str(uuid4())}, raise_on_error=False
    )

    assert "HTTP 404" in error_text(result)
