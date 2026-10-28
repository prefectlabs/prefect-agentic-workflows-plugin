"""Tests for `get_workspace`, which reports the workspace the server uses."""

from typing import Any

import httpx
import pytest
import respx
from fastmcp import Client
from support import (
    ACCOUNT_ID,
    API_KEY,
    ME_WORKSPACES_URL,
    WORKSPACE_API_URL,
    WORKSPACE_ID,
    WORKSPACE_NAME,
    me_workspace,
)

STAGING_API_URL = (
    f"https://api.stg.prefect.dev/api/accounts/{ACCOUNT_ID}/workspaces/{WORKSPACE_ID}"
)


async def test_get_workspace_returns_the_name_and_addresses(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    result = await mcp_client.call_tool("get_workspace", {})

    assert result.structured_content == {
        "name": WORKSPACE_NAME,
        "api_url": WORKSPACE_API_URL,
        "app_url": (
            f"https://app.prefect.cloud/account/{ACCOUNT_ID}/workspace/{WORKSPACE_ID}"
        ),
    }
    request = cloud_api.routes["me_workspaces"].calls.last.request
    assert request.headers["Authorization"] == f"Bearer {API_KEY}"
    assert [call.request.url for call in cloud_api.calls] == [
        httpx.URL(ME_WORKSPACES_URL)
    ]


async def test_get_workspace_picks_its_workspace_from_the_list(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    other = {
        **me_workspace(workspace_id="cccccccc-cccc-cccc-cccc-cccccccccccc"),
        "workspace_handle": "other-workspace",
    }
    cloud_api.routes["me_workspaces"].respond(200, json=[other, me_workspace()])

    result = await mcp_client.call_tool("get_workspace", {})

    assert result.structured_content is not None
    assert result.structured_content["name"] == WORKSPACE_NAME


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, json=[]),
        httpx.Response(403, json={"detail": "Forbidden"}),
        httpx.Response(200, text="not json"),
    ],
    ids=["not-listed", "error", "invalid"],
)
async def test_get_workspace_omits_a_name_it_cannot_read(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, response: httpx.Response
):
    cloud_api.routes["me_workspaces"].mock(return_value=response)

    result = await mcp_client.call_tool("get_workspace", {})

    assert result.structured_content is not None
    assert result.structured_content["name"] is None
    assert result.structured_content["api_url"] == WORKSPACE_API_URL


async def test_get_workspace_follows_a_staging_profile(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("PREFECT_API_URL", STAGING_API_URL)
    monkeypatch.setenv("PREFECT_CLOUD_API_URL", "https://api.stg.prefect.dev/api")
    staging = cloud_api.get("https://api.stg.prefect.dev/api/me/workspaces").respond(
        200, json=[{**me_workspace(), "account_handle": "prefect-staging"}]
    )

    result = await mcp_client.call_tool("get_workspace", {})

    assert staging.called
    assert result.structured_content is not None
    assert result.structured_content["name"] == "prefect-staging/test-workspace"
    assert result.structured_content["app_url"] == (
        f"https://app.stg.prefect.dev/account/{ACCOUNT_ID}/workspace/{WORKSPACE_ID}"
    )


async def test_workspace_selection_requires_oauth(mcp_client: Client[Any]):
    result = await mcp_client.call_tool(
        "get_workspace", {"workspace_id": str(WORKSPACE_ID)}, raise_on_error=False
    )
    assert result.is_error
    assert "OAuth" in str(result.content)


async def test_get_workspace_uses_an_explicit_ui_url(
    mcp_client: Client[Any], monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("PREFECT_UI_URL", "https://prefect.example.com/workspace")

    result = await mcp_client.call_tool("get_workspace", {})

    assert result.structured_content is not None
    assert (
        result.structured_content["app_url"] == "https://prefect.example.com/workspace"
    )
