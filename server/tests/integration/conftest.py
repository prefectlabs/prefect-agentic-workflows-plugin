"""Fixtures for the integration suite, which calls a real Cloud workspace.

The suite is opt-in. It runs only when `PREFECT_AGENTIC_WORKFLOWS_INTEGRATION=1`
is set and the active Prefect profile has a Cloud workspace URL and an API key.
The account needs the `execution-plans` feature. Otherwise every test skips.

Run it with:

    PREFECT_AGENTIC_WORKFLOWS_INTEGRATION=1 uv run pytest tests/integration
"""

import os
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from fastmcp import Client
from prefect.settings import get_current_settings

from prefect_agentic_workflows_mcp.server import build_server
from prefect_agentic_workflows_mcp.workspace_api import is_cloud_workspace_api_url

OPT_IN_VARIABLE = "PREFECT_AGENTIC_WORKFLOWS_INTEGRATION"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "integration" in item.path.parts:
            item.add_marker(pytest.mark.integration)


@pytest.fixture(autouse=True)
def cloud_profile() -> Iterator[None]:
    """Use the real active profile, and skip unless the suite is opted in."""
    if os.environ.get(OPT_IN_VARIABLE) != "1":
        pytest.skip(f"set {OPT_IN_VARIABLE}=1 to run integration tests")

    settings = get_current_settings()
    api_url = settings.api.url or ""
    if not is_cloud_workspace_api_url(api_url) or settings.api.key is None:
        pytest.skip("the active Prefect profile has no Cloud workspace credentials")
    yield


@pytest.fixture
async def mcp_client() -> AsyncIterator[Client[Any]]:
    """Return an in-memory client connected to a server that uses the real API."""
    async with Client(build_server()) as client:
        yield client
