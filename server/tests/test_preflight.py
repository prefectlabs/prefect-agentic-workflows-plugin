"""Tests for the preflight check that runs before a tool's first request."""

from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from fastmcp import Client
from support import API_KEY, PLAN, WORKSPACE_API_URL, error_text

from prefect_agentic_workflows_mcp.server import build_server
from prefect_agentic_workflows_mcp.workspace_api import (
    ALLOW_LOOPBACK_VARIABLE,
    is_cloud_workspace_api_url,
)

VALID = {"valid": True, "errors": []}


def set_profile(monkeypatch: pytest.MonkeyPatch, values: dict[str, str | None]) -> None:
    """Override Prefect settings. An empty value hides the profile's setting."""
    for name, value in values.items():
        monkeypatch.setenv(name, value or "")


@pytest.mark.parametrize("api_url", [None, "http://127.0.0.1:4200/api"])
async def test_server_starts_and_lists_tools_without_a_cloud_profile(
    monkeypatch: pytest.MonkeyPatch, api_url: str | None
):
    set_profile(monkeypatch, {"PREFECT_API_URL": api_url, "PREFECT_API_KEY": None})
    async with Client(build_server()) as client:
        names = {tool.name for tool in await client.list_tools()}

    assert {"get_schema", "validate_plan"} <= names


@pytest.mark.parametrize(
    "api_url",
    [
        "http://api.prefect.cloud/api/accounts/a/workspaces/w",
        "https://api.prefect.cloud.example.com/api/accounts/a/workspaces/w",
        "https://example.com/api/accounts/a/workspaces/w",
        "https://api.prefect.cloud:bad/api/accounts/a/workspaces/w",
    ],
    ids=["plain-http", "lookalike-host", "other-host", "malformed"],
)
async def test_the_api_key_is_only_sent_to_prefect_cloud_over_https(
    monkeypatch: pytest.MonkeyPatch, cloud_api: respx.MockRouter, api_url: str
):
    set_profile(monkeypatch, {"PREFECT_API_URL": api_url})
    async with Client(build_server()) as client:
        result = await client.call_tool("get_schema", {}, raise_on_error=False)

    assert "not a Prefect Cloud workspace" in error_text(result)
    assert not cloud_api.calls


LOOPBACK_URL = "http://127.0.0.1:4200/api/accounts/a/workspaces/w"


def test_a_cloud_workspace_url_is_accepted():
    assert is_cloud_workspace_api_url(
        "https://api.prefect.cloud/api/accounts/a/workspaces/w"
    )


def test_a_loopback_url_is_accepted_only_when_a_test_harness_allows_it(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv(ALLOW_LOOPBACK_VARIABLE, raising=False)
    assert not is_cloud_workspace_api_url(LOOPBACK_URL)

    monkeypatch.setenv(ALLOW_LOOPBACK_VARIABLE, "1")
    assert is_cloud_workspace_api_url(LOOPBACK_URL)


@pytest.mark.parametrize(
    ("updates", "expected"),
    [
        ({"PREFECT_API_URL": None}, ["No Prefect API URL", "prefect cloud login"]),
        (
            {"PREFECT_API_URL": "http://127.0.0.1:4200/api"},
            ["http://127.0.0.1:4200/api", "not a Prefect Cloud workspace"],
        ),
        ({"PREFECT_API_KEY": None}, ["no API key", "prefect cloud login"]),
    ],
    ids=["no-url", "self-hosted", "no-key"],
)
async def test_a_profile_that_is_not_a_cloud_workspace_sends_no_request(
    monkeypatch: pytest.MonkeyPatch,
    cloud_api: respx.MockRouter,
    updates: dict[str, str | None],
    expected: list[str],
):
    set_profile(monkeypatch, updates)
    async with Client(build_server()) as client:
        result = await client.call_tool("get_schema", {}, raise_on_error=False)

    message = error_text(result)
    for text in expected:
        assert text in message
    assert not cloud_api.calls


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        (httpx.Response(404), ["not enabled", "`execution-plans`"]),
        (httpx.Response(401), ["rejected the API key", "prefect cloud login"]),
        (httpx.Response(403), ["HTTP 403", "permission", "prefect cloud login"]),
        (httpx.Response(500, json={"detail": "boom"}), ["HTTP 500", "boom"]),
        (
            httpx.ConnectError("connection refused"),
            ["Could not reach Prefect Cloud", "connection refused"],
        ),
    ],
    ids=["feature-off", "unauthorized", "forbidden", "server-error", "unreachable"],
)
async def test_a_failing_preflight_stops_the_tool_request(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    response: httpx.Response | Exception,
    expected: list[str],
):
    if isinstance(response, Exception):
        cloud_api.routes["schema"].side_effect = response
    else:
        cloud_api.routes["schema"].mock(return_value=response)
    validate = cloud_api.post("/execution-plans/validate").respond(200, json=VALID)

    result = await mcp_client.call_tool(
        "validate_plan", {"plan": PLAN}, raise_on_error=False
    )

    message = error_text(result)
    for text in expected:
        assert text in message
    assert not validate.called


async def test_a_passing_preflight_runs_once(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post("/execution-plans/validate").respond(200, json=VALID)

    await mcp_client.call_tool("validate_plan", {"plan": PLAN})
    await mcp_client.call_tool("validate_plan", {"plan": PLAN})

    assert cloud_api.routes["schema"].call_count == 1


async def test_a_failing_preflight_runs_again_on_the_next_call(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.routes["schema"].respond(404)
    cloud_api.post("/execution-plans/validate").respond(200, json=VALID)
    failed = await mcp_client.call_tool(
        "validate_plan", {"plan": PLAN}, raise_on_error=False
    )
    assert failed.is_error

    cloud_api.routes["schema"].respond(200, json={})
    result = await mcp_client.call_tool("validate_plan", {"plan": PLAN})

    assert result.structured_content == VALID


async def test_a_profile_switch_takes_effect_without_a_restart(
    monkeypatch: pytest.MonkeyPatch, mcp_client: Client[Any]
):
    await mcp_client.call_tool("get_schema", {})

    set_profile(monkeypatch, {"PREFECT_API_URL": "http://127.0.0.1:4200/api"})
    result = await mcp_client.call_tool("get_schema", {}, raise_on_error=False)

    assert "not a Prefect Cloud workspace" in error_text(result)


async def test_the_saved_active_profile_is_used(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, cloud_api: respx.MockRouter
):
    # `prefect cloud login` saves the workspace in profiles.toml and sets no
    # environment variables.
    (tmp_path / "profiles.toml").write_text(
        'active = "cloud"\n'
        "[profiles.cloud]\n"
        f'PREFECT_API_URL = "{WORKSPACE_API_URL}"\n'
        f'PREFECT_API_KEY = "{API_KEY}"\n'
    )
    for name in ("PREFECT_API_URL", "PREFECT_API_KEY", "PREFECT_PROFILE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PREFECT_PROFILES_PATH", str(tmp_path / "profiles.toml"))

    async with Client(build_server()) as client:
        await client.call_tool("get_schema", {})

    assert cloud_api.calls.last.request.headers["Authorization"] == f"Bearer {API_KEY}"
