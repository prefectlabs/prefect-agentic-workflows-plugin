"""A tool that reports which Prefect Cloud workspace the server uses."""

from typing import Any

import httpx
from fastmcp import FastMCP
from pydantic import BaseModel

from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi

CLOUD_API_HOST = "api.prefect.cloud"
CLOUD_APP_ORIGIN = "https://app.prefect.cloud"


class Workspace(BaseModel):
    api_url: str
    app_url: str | None


def app_url(api_url: str) -> str | None:
    """Return the Prefect Cloud UI URL for a workspace API URL, when known."""
    url = httpx.URL(api_url)
    parts = url.path.strip("/").split("/")
    if url.host != CLOUD_API_HOST or parts[:1] != ["api"] or len(parts) != 5:
        return None
    _, accounts, account_id, workspaces, workspace_id = parts
    if (accounts, workspaces) != ("accounts", "workspaces"):
        return None
    return f"{CLOUD_APP_ORIGIN}/account/{account_id}/workspace/{workspace_id}"


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the workspace tool to `mcp`."""

    @tool(mcp, read_only=True)
    async def get_workspace() -> Workspace:
        """Return the Prefect Cloud workspace this server sends requests to.

        Returns `api_url` and `app_url`, the workspace's page in the Prefect
        Cloud UI. Build links from `app_url`: add `/flows/flow/<flow_id>` for
        a flow, or `/runs/flow-run/<flow_run_id>` for a run. `app_url` is null
        when the server can't work out the UI address from the API URL.
        """
        api_url = api.current_api_url()
        return Workspace(api_url=api_url, app_url=app_url(api_url))
