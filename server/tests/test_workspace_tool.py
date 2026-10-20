"""Tests for `get_workspace`, which reports the workspace the server uses."""

from typing import Any

import pytest
import respx
from fastmcp import Client
from support import ACCOUNT_ID, WORKSPACE_API_URL, WORKSPACE_ID

STAGING_API_URL = (
    f"https://api.stg.prefect.dev/api/accounts/{ACCOUNT_ID}/workspaces/{WORKSPACE_ID}"
)


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


async def test_get_workspace_follows_a_staging_profile(
    mcp_client: Client[Any], monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("PREFECT_API_URL", STAGING_API_URL)
    monkeypatch.setenv("PREFECT_CLOUD_API_URL", "https://api.stg.prefect.dev/api")

    result = await mcp_client.call_tool("get_workspace", {})

    assert result.structured_content is not None
    assert result.structured_content["app_url"] == (
        f"https://app.stg.prefect.dev/account/{ACCOUNT_ID}/workspace/{WORKSPACE_ID}"
    )


async def test_get_workspace_uses_an_explicit_ui_url(
    mcp_client: Client[Any], monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("PREFECT_UI_URL", "https://prefect.example.com/workspace")

    result = await mcp_client.call_tool("get_workspace", {})

    assert result.structured_content is not None
    assert (
        result.structured_content["app_url"] == "https://prefect.example.com/workspace"
    )
