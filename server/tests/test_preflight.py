"""Tests for the preflight check that runs before a tool's first request."""

from typing import Any

import httpx
import pytest
import respx
from fastmcp import Client
from prefect.settings import PREFECT_API_KEY, PREFECT_API_URL, temporary_settings
from support import PLAN, error_text

from prefect_agentic_workflows_mcp.server import build_server

VALID = {"valid": True, "errors": []}


@pytest.mark.parametrize("api_url", [None, "http://127.0.0.1:4200/api"])
async def test_server_starts_and_lists_tools_without_a_cloud_profile(
    api_url: str | None,
):
    with temporary_settings(updates={PREFECT_API_URL: api_url, PREFECT_API_KEY: None}):
        async with Client(build_server()) as client:
            names = {tool.name for tool in await client.list_tools()}

    assert {"get_schema", "validate_plan"} <= names


@pytest.mark.parametrize(
    ("updates", "expected"),
    [
        ({PREFECT_API_URL: None}, ["No Prefect API URL", "prefect cloud login"]),
        (
            {PREFECT_API_URL: "http://127.0.0.1:4200/api"},
            ["http://127.0.0.1:4200/api", "not a Prefect Cloud workspace"],
        ),
        ({PREFECT_API_KEY: None}, ["no API key", "prefect cloud login"]),
    ],
    ids=["no-url", "self-hosted", "no-key"],
)
async def test_a_profile_that_is_not_a_cloud_workspace_sends_no_request(
    cloud_api: respx.MockRouter, updates: dict[Any, Any], expected: list[str]
):
    with temporary_settings(updates=updates):
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
