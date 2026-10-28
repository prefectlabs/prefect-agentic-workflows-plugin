"""Tests for credentials sent in the `X-Prefect-Api-Url` and `X-Prefect-Api-Key`
headers when the server runs over HTTP.

These tests run the server over real HTTP on a loopback port, so the headers
pass through the same transport a hosted server uses. The profile points at a
different workspace with a different key, so each test can tell which
credentials a request used.
"""

from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
import respx
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.utilities.tests import run_server_async
from support import (
    ACCOUNT_ID,
    API_KEY,
    ME_WORKSPACES_URL,
    PLAN,
    WORKSPACE_API_URL,
    WORKSPACE_ID,
    WORKSPACE_NAME,
    error_text,
    me_workspace,
    schema_response,
)

from prefect_agentic_workflows_mcp.server import build_server

PROFILE_API_URL = (
    "https://api.prefect.cloud/api/accounts/"
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/workspaces/"
    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
)
PROFILE_API_KEY = "pnu_profile_key"
HEADERS = {"X-Prefect-Api-Url": WORKSPACE_API_URL, "X-Prefect-Api-Key": API_KEY}


@pytest.fixture(autouse=True)
def cloud_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the profile at a workspace the headers don't name."""
    monkeypatch.setenv("PREFECT_API_URL", PROFILE_API_URL)
    monkeypatch.setenv("PREFECT_API_KEY", PROFILE_API_KEY)
    monkeypatch.delenv("PREFECT_HEADER_API_HOSTS", raising=False)


@pytest.fixture
def cloud_api() -> Iterator[respx.MockRouter]:
    """Mock the schema route in both workspaces and let loopback requests
    through to the server."""
    with respx.mock(assert_all_called=False) as router:
        router.route(host="127.0.0.1").pass_through()
        router.get(
            f"{WORKSPACE_API_URL}/execution-plans/schema", name="header_schema"
        ).respond(200, json=schema_response())
        router.get(
            f"{PROFILE_API_URL}/execution-plans/schema", name="profile_schema"
        ).respond(200, json=schema_response())
        router.get(ME_WORKSPACES_URL, name="me_workspaces").respond(
            200, json=[me_workspace()]
        )
        yield router


@pytest.fixture
async def server_url(cloud_api: respx.MockRouter) -> AsyncIterator[str]:
    async with run_server_async(build_server()) as url:
        yield url


def connect(server_url: str, headers: dict[str, str] | None = None) -> Client[Any]:
    return Client(StreamableHttpTransport(server_url, headers=headers))


def cloud_calls(cloud_api: respx.MockRouter) -> list[Any]:
    return [call for call in cloud_api.calls if call.request.url.host != "127.0.0.1"]


async def test_the_headers_are_used_instead_of_the_profile(
    server_url: str, cloud_api: respx.MockRouter
):
    async with connect(server_url, HEADERS) as client:
        await client.call_tool("get_schema", {})
        workspace = await client.call_tool("get_workspace", {})

    request = cloud_api.routes["header_schema"].calls.last.request
    assert request.headers["Authorization"] == f"Bearer {API_KEY}"
    assert not cloud_api.routes["profile_schema"].called
    assert workspace.structured_content is not None
    assert workspace.structured_content["name"] == WORKSPACE_NAME
    lookup = cloud_api.routes["me_workspaces"].calls.last.request
    assert lookup.headers["Authorization"] == f"Bearer {API_KEY}"
    assert workspace.structured_content["api_url"] == WORKSPACE_API_URL
    assert workspace.structured_content["app_url"] == (
        f"https://app.prefect.cloud/account/{ACCOUNT_ID}/workspace/{WORKSPACE_ID}"
    )


async def test_the_profile_is_used_without_headers(
    server_url: str, cloud_api: respx.MockRouter
):
    async with connect(server_url) as client:
        await client.call_tool("get_schema", {})

    request = cloud_api.routes["profile_schema"].calls.last.request
    assert request.headers["Authorization"] == f"Bearer {PROFILE_API_KEY}"
    assert not cloud_api.routes["header_schema"].called


@pytest.mark.parametrize(
    ("headers", "missing"),
    [
        ({"X-Prefect-Api-Url": WORKSPACE_API_URL}, "X-Prefect-Api-Key"),
        ({"X-Prefect-Api-Key": API_KEY}, "X-Prefect-Api-Url"),
    ],
    ids=["url-only", "key-only"],
)
async def test_one_header_without_the_other_sends_no_request(
    server_url: str,
    cloud_api: respx.MockRouter,
    headers: dict[str, str],
    missing: str,
):
    # A URL header alone must never send the profile's key to that URL.
    async with connect(server_url, headers) as client:
        result = await client.call_tool("get_schema", {}, raise_on_error=False)

    assert f"not the {missing} header" in error_text(result)
    assert not cloud_calls(cloud_api)


async def test_a_header_url_that_is_not_a_cloud_workspace_sends_no_request(
    server_url: str, cloud_api: respx.MockRouter
):
    headers = {**HEADERS, "X-Prefect-Api-Url": "http://prefect.example.com/api"}
    async with connect(server_url, headers) as client:
        result = await client.call_tool("get_schema", {}, raise_on_error=False)

    message = error_text(result)
    assert "X-Prefect-Api-Url header" in message
    assert "not a Prefect Cloud workspace" in message
    assert not cloud_calls(cloud_api)


WORKSPACE_PATH = "/api/accounts/a/workspaces/w"


@pytest.mark.parametrize(
    "api_url",
    [
        f"http://127.0.0.1:4200{WORKSPACE_PATH}",
        f"https://localhost{WORKSPACE_PATH}",
        f"https://10.0.0.5{WORKSPACE_PATH}",
        f"https://169.254.169.254{WORKSPACE_PATH}",
        f"https://prefect.internal.example.com{WORKSPACE_PATH}",
        f"https://api.stg.prefect.dev{WORKSPACE_PATH}",
        f"http://api.prefect.cloud{WORKSPACE_PATH}",
    ],
    ids=[
        "loopback-over-http",
        "localhost",
        "private-address",
        "metadata-address",
        "internal-host",
        "unlisted-cloud-host",
        "trusted-host-over-http",
    ],
)
async def test_a_header_url_off_the_trusted_hosts_sends_no_request(
    server_url: str, cloud_api: respx.MockRouter, api_url: str
):
    headers = {**HEADERS, "X-Prefect-Api-Url": api_url}
    async with connect(server_url, headers) as client:
        result = await client.call_tool("get_schema", {}, raise_on_error=False)

    message = error_text(result)
    assert "X-Prefect-Api-Url header" in message
    assert not cloud_calls(cloud_api)


async def test_operators_can_trust_other_hosts(
    monkeypatch: pytest.MonkeyPatch, server_url: str, cloud_api: respx.MockRouter
):
    staging_url = f"https://api.stg.prefect.dev{WORKSPACE_PATH}"
    monkeypatch.setenv(
        "PREFECT_HEADER_API_HOSTS",
        "api.prefect.cloud, api.stg.prefect.dev",
    )
    staging = cloud_api.get(f"{staging_url}/execution-plans/schema").respond(
        200, json=schema_response()
    )

    async with connect(
        server_url, {**HEADERS, "X-Prefect-Api-Url": staging_url}
    ) as client:
        await client.call_tool("get_schema", {})

    assert staging.called


async def test_a_rejected_header_key_says_to_fix_the_header(
    server_url: str, cloud_api: respx.MockRouter
):
    cloud_api.routes["header_schema"].respond(401)
    async with connect(server_url, HEADERS) as client:
        result = await client.call_tool("get_schema", {}, raise_on_error=False)

    message = error_text(result)
    assert "X-Prefect-Api-Key header" in message
    assert "prefect cloud login" not in message
    assert "restart the server" not in message


async def test_urls_that_differ_only_in_the_fragment_share_one_preflight(
    server_url: str, cloud_api: respx.MockRouter
):
    cloud_api.post(f"{WORKSPACE_API_URL}/execution-plans/validate").respond(
        200, json={"valid": True, "errors": []}
    )
    for fragment in ["", "#1", "#2"]:
        headers = {**HEADERS, "X-Prefect-Api-Url": WORKSPACE_API_URL + fragment}
        async with connect(server_url, headers) as client:
            await client.call_tool("validate_plan", {"plan": PLAN})

    assert cloud_api.routes["header_schema"].call_count == 1


async def test_the_preflight_runs_once_for_each_set_of_headers(
    server_url: str, cloud_api: respx.MockRouter
):
    other_headers = {**HEADERS, "X-Prefect-Api-Key": "pnu_other_key"}
    async with (
        connect(server_url, HEADERS) as first,
        connect(server_url, other_headers) as second,
    ):
        for _ in range(2):
            await first.call_tool("get_schema", {})
            await second.call_tool("get_schema", {})

    # Each `get_schema` call reads the schema too, so count only the preflight
    # requests: one per key, then one schema read per call.
    assert cloud_api.routes["header_schema"].call_count == 2 + 4
