"""Builds the `prefect-agentic-workflows` MCP server."""

import importlib
import pkgutil
from typing import Any

from fastmcp import FastMCP

from prefect_agentic_workflows_mcp import __version__, tools
from prefect_agentic_workflows_mcp.cloud_oauth import (
    CloudOAuthSettings,
    build_auth_provider,
)
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi

SERVER_NAME = "prefect-agentic-workflows"

INSTRUCTIONS = """\
Tools for authoring, publishing, and running Prefect Cloud execution plans.
This server is alpha, so tool names, arguments, and results can change in any
release.

With OAuth, the tools use a workspace in the user's consent grant. Call
get_workspace first. If several workspaces are available, ask which to use
and pass its workspace_id to each tool. With OAuth disabled, the tools use
paired HTTP credentials or the user's active Prefect profile.
Execution plans are only available in Prefect Cloud, and the account needs the
`execution-plans` feature. When a tool returns instructions for fixing
authorization or the workspace, relay them to the user.

Set `model` on every agent node. Every workspace has a `prefect` provider
connection, which runs Prefect's models with no API key. When the user hasn't
picked a model, ask which to use; Prefect's models are the easiest start.
"""


def build_server() -> FastMCP[Any]:
    """Return a server with every tool group in the `tools` package."""
    oauth_settings = CloudOAuthSettings()
    mcp: FastMCP[Any] = FastMCP(
        SERVER_NAME,
        instructions=INSTRUCTIONS,
        version=__version__,
        auth=build_auth_provider(oauth_settings),
    )
    api = WorkspaceApi(oauth_settings)
    for module_info in sorted(
        pkgutil.iter_modules(tools.__path__), key=lambda info: info.name
    ):
        if module_info.name.startswith("_"):
            continue
        module = importlib.import_module(f"{tools.__name__}.{module_info.name}")
        module.register(mcp, api)
    return mcp
