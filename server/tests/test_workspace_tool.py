"""Tests for `get_workspace`, which reports the workspace the server uses."""

from typing import Any

import pytest
import respx
from fastmcp import Client
from support import ACCOUNT_ID, WORKSPACE_API_URL, WORKSPACE_ID

from prefect_agentic_workflows_mcp.tools.workspace import app_url


async def test_get_workspace_returns_the_api_and_ui_addresses(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    result = await mcp_client.call_tool("get_workspace", {})

    assert result.structured_content == {
        "api_url": WORKSPACE_API_URL,
        "app_url": (
            f"https://app.prefect.cloud/account/{ACCOUNT_ID}/workspace/{WORKSPACE_ID}"
        ),
    }
    assert not cloud_api.calls


@pytest.mark.parametrize(
    "api_url",
    [
        "https://api.eu.prefect.cloud/api/accounts/a/workspaces/w",
        "https://api.prefect.cloud/api/accounts/a/workspaces/w/extra",
    ],
)
def test_an_unfamiliar_api_url_has_no_ui_address(api_url: str):
    assert app_url(api_url) is None
