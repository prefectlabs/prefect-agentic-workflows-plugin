"""A tool that reports which Prefect Cloud workspace the server uses."""

from typing import Any

from fastmcp import FastMCP
from pydantic import BaseModel

from prefect_agentic_workflows_mcp.parameters import WorkspaceId
from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi


class Workspace(BaseModel):
    api_url: str
    app_url: str


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the workspace tool to `mcp`."""

    @tool(mcp, read_only=True)
    async def get_workspace(workspace_id: WorkspaceId = None) -> Workspace:
        """Return the Prefect Cloud workspace this server sends requests to.

        Returns `api_url` and `app_url`, the workspace's page in the Prefect
        Cloud UI. Build links from `app_url`: add `/flows/flow/<flow_id>` for
        a flow, or `/runs/flow-run/<flow_run_id>` for a run.
        """
        api_url, app_url = await api.current_workspace_urls(workspace_id=workspace_id)
        return Workspace(api_url=api_url, app_url=app_url)
