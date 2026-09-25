"""Shared fixtures for the mocked test suite.

Every test calls tools through an in-memory FastMCP client. The Prefect Cloud
API is mocked with respx, and any request to an unmocked route fails the test.
The active Prefect profile is replaced with a fake Cloud workspace for each
test, so the suite never reads or calls the developer's real workspace.

Constants and payload builders are in `support.py`.
"""

from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
import respx
from fastmcp import Client
from prefect.settings import PREFECT_API_KEY, PREFECT_API_URL, temporary_settings

from prefect_agentic_workflows_mcp.server import build_server
from support import API_KEY, WORKSPACE_API_URL, schema_response


@pytest.fixture(autouse=True)
def cloud_profile() -> Iterator[None]:
    """Point the active Prefect profile at a fake Cloud workspace."""
    with temporary_settings(
        updates={PREFECT_API_URL: WORKSPACE_API_URL, PREFECT_API_KEY: API_KEY}
    ):
        yield


@pytest.fixture
def cloud_api() -> Iterator[respx.MockRouter]:
    """Mock the workspace API.

    The `schema` route reads the current schema, with no query parameters.
    It answers the preflight check, so tools in every group can run without
    extra setup. A test that needs a different preflight response replaces
    it with `cloud_api.routes["schema"].respond(...)`.
    """
    with respx.mock(base_url=WORKSPACE_API_URL, assert_all_called=False) as router:
        router.get("/execution-plans/schema", params__eq={}, name="schema").respond(
            200, json=schema_response()
        )
        yield router


@pytest.fixture
async def mcp_client(cloud_api: respx.MockRouter) -> AsyncIterator[Client[Any]]:
    """Return an in-memory client connected to a fresh server."""
    async with Client(build_server()) as client:
        yield client
