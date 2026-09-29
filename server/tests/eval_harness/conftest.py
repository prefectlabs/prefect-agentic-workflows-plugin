"""Fixtures for the tests of the behavioral evaluation harness in `evals/`.

These tests run in CI. They check the fake Cloud API through the MCP tools,
the same way an agent in an evaluation calls it, and they never start an agent.
"""

from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
import respx
from fastmcp import Client
from support import WORKSPACE_API_URL

from evals.fake_cloud import FakeCloud
from prefect_agentic_workflows_mcp.server import build_server


@pytest.fixture
def fake_cloud() -> FakeCloud:
    return FakeCloud(WORKSPACE_API_URL)


@pytest.fixture
def fake_api(fake_cloud: FakeCloud) -> Iterator[respx.MockRouter]:
    """Send every workspace API request to `fake_cloud`."""
    with respx.mock(assert_all_called=False) as router:
        router.route(url__startswith=WORKSPACE_API_URL).mock(
            side_effect=fake_cloud.handle
        )
        yield router


@pytest.fixture
async def client(fake_api: respx.MockRouter) -> AsyncIterator[Client[Any]]:
    """Return an in-memory client of a server that uses the fake Cloud API."""
    async with Client(build_server()) as client:
        yield client
