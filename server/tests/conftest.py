"""Shared fixtures for the mocked test suite.

Every test calls tools through an in-memory FastMCP client. The Prefect Cloud
API is mocked with respx, and any request to an unmocked route fails the test.
The active Prefect profile is replaced with a fake Cloud workspace for each
test, so the suite never reads or calls the developer's real workspace.

Constants and payload builders are in `support.py`.
"""

from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
import respx
from fastmcp import Client
from support import (
    API_KEY,
    ME_WORKSPACES_URL,
    WORKSPACE_API_URL,
    me_workspace,
    schema_response,
)

from prefect_agentic_workflows_mcp.server import build_server


@pytest.fixture(autouse=True)
def cloud_profile(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Point the Prefect settings at a fake Cloud workspace.

    The server loads settings fresh on each call, so tests set environment
    variables and an empty profiles file. A developer's profile can't leak in,
    such as a staging `PREFECT_CLOUD_API_URL` that changes the UI address.
    """
    for name in ["ENABLED", "API_BASE_URL", "AUTH_BASE_URL", "PUBLIC_BASE_URL"]:
        monkeypatch.delenv(f"PREFECT_MCP_CLOUD_{name}", raising=False)
    monkeypatch.setenv("PREFECT_PROFILES_PATH", str(tmp_path / "profiles.toml"))
    monkeypatch.delenv("PREFECT_CLOUD_API_URL", raising=False)
    monkeypatch.delenv("PREFECT_UI_URL", raising=False)
    monkeypatch.setenv("PREFECT_API_URL", WORKSPACE_API_URL)
    monkeypatch.setenv("PREFECT_API_KEY", API_KEY)


@pytest.fixture
def cloud_api() -> Iterator[respx.MockRouter]:
    """Mock the workspace API.

    The `schema` route reads the current schema, with no query parameters.
    It answers the preflight check, so tools in every group can run without
    extra setup. A test that needs a different preflight response replaces
    it with `cloud_api.routes["schema"].respond(...)`. The `me_workspaces`
    route lists the key's workspaces, which `get_workspace` reads for the
    workspace's name.
    """
    with respx.mock(base_url=WORKSPACE_API_URL, assert_all_called=False) as router:
        router.get("/execution-plans/schema", params__eq={}, name="schema").respond(
            200, json=schema_response()
        )
        router.get(ME_WORKSPACES_URL, name="me_workspaces").respond(
            200, json=[me_workspace()]
        )
        yield router


@pytest.fixture
async def mcp_client(cloud_api: respx.MockRouter) -> AsyncIterator[Client[Any]]:
    """Return an in-memory client connected to a fresh server."""
    async with Client(build_server()) as client:
        yield client
