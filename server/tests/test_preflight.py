"""Tests for the preflight check that runs before a tool's first request."""

from typing import Any

import httpx
import pytest
import respx
from fastmcp import Client
from prefect.settings import PREFECT_API_KEY, PREFECT_API_URL, temporary_settings

from prefect_agentic_workflows_mcp.server import build_server
from support import error_text

VALID = {"valid": True, "errors": []}
PLAN = {"schema_version": "0.1"}


@pytest.mark.parametrize("api_url", [None, "http://127.0.0.1:4200/api"])
async def test_server_starts_and_lists_tools_without_a_cloud_profile(
    api_url: str | None,
):
    with temporary_settings(updates={PREFECT_API_URL: api_url, PREFECT_API_KEY: None}):
        async with Client(build_server()) as client:
            names = {tool.name for tool in await client.list_tools()}

    assert {"get_schema", "validate_plan"} <= names


async def test_a_profile_without_an_api_url_asks_the_user_to_log_in(
    cloud_api: respx.MockRouter,
):
    with temporary_settings(updates={PREFECT_API_URL: None}):
        async with Client(build_server()) as client:
            result = await client.call_tool("get_schema", {}, raise_on_error=False)

    message = error_text(result)
    assert "No Prefect API URL" in message
    assert "prefect cloud login" in message
    assert not cloud_api.calls


async def test_a_self_hosted_server_is_reported_as_not_cloud(
    cloud_api: respx.MockRouter,
):
    with temporary_settings(updates={PREFECT_API_URL: "http://127.0.0.1:4200/api"}):
        async with Client(build_server()) as client:
            result = await client.call_tool(
                "validate_plan", {"plan": PLAN}, raise_on_error=False
            )

    message = error_text(result)
    assert "http://127.0.0.1:4200/api" in message
    assert "not a Prefect Cloud workspace" in message
    assert "only available in Prefect Cloud" in message
    assert not cloud_api.calls


async def test_a_cloud_profile_without_an_api_key_asks_the_user_to_log_in(
    cloud_api: respx.MockRouter,
):
    with temporary_settings(updates={PREFECT_API_KEY: None}):
        async with Client(build_server()) as client:
            result = await client.call_tool("get_schema", {}, raise_on_error=False)

    message = error_text(result)
    assert "no API key" in message
    assert "prefect cloud login" in message
    assert not cloud_api.calls


async def test_a_disabled_feature_flag_is_reported_by_name(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.routes["schema"].respond(404, json={"detail": "Not found"})
    validate = cloud_api.post("/execution-plans/validate").respond(200, json=VALID)

    result = await mcp_client.call_tool(
        "validate_plan", {"plan": PLAN}, raise_on_error=False
    )

    message = error_text(result)
    assert "not enabled" in message
    assert "`execution-plans`" in message
    assert not validate.called


async def test_a_rejected_api_key_asks_the_user_to_log_in_again(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.routes["schema"].respond(401, json={"detail": "Unauthorized"})

    result = await mcp_client.call_tool("get_schema", {}, raise_on_error=False)

    message = error_text(result)
    assert "rejected the API key" in message
    assert "prefect cloud login" in message


async def test_an_unreachable_api_is_reported_with_its_url(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.routes["schema"].side_effect = httpx.ConnectError("connection refused")

    result = await mcp_client.call_tool("get_schema", {}, raise_on_error=False)

    message = error_text(result)
    assert "Could not reach Prefect Cloud" in message
    assert "connection refused" in message


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
    cloud_api.routes["schema"].respond(404, json={"detail": "Not found"})
    cloud_api.post("/execution-plans/validate").respond(200, json=VALID)
    failed = await mcp_client.call_tool(
        "validate_plan", {"plan": PLAN}, raise_on_error=False
    )
    assert failed.is_error

    cloud_api.routes["schema"].respond(200, json={})
    result = await mcp_client.call_tool("validate_plan", {"plan": PLAN})

    assert result.structured_content == VALID


async def test_a_forbidden_request_says_the_key_may_lack_permission(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.routes["schema"].respond(403, json={"detail": "Forbidden"})

    result = await mcp_client.call_tool("get_schema", {}, raise_on_error=False)

    message = error_text(result)
    assert "HTTP 403" in message
    assert "permission" in message
    assert "prefect cloud login" in message
