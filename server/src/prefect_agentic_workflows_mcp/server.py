"""Builds the `prefect-agentic-workflows` MCP server."""

import importlib
import pkgutil
from typing import Any

from fastmcp import FastMCP

from prefect_agentic_workflows_mcp import __version__, tools
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi

SERVER_NAME = "prefect-agentic-workflows"

INSTRUCTIONS = """\
Tools for authoring, publishing, and running Prefect Cloud execution plans.
This server is alpha, so tool names, arguments, and results can change in any
release.

The tools use the Prefect Cloud workspace in the user's active Prefect profile.
Execution plans are only available in Prefect Cloud, and the account needs the
`execution-plans` feature. When a tool returns an error that explains how to fix
the profile or the workspace, relay that error to the user.
"""


def build_server() -> FastMCP[Any]:
    """Return a server with every tool group in the `tools` package."""
    mcp: FastMCP[Any] = FastMCP(
        SERVER_NAME, instructions=INSTRUCTIONS, version=__version__
    )
    api = WorkspaceApi()
    for module_info in sorted(
        pkgutil.iter_modules(tools.__path__), key=lambda info: info.name
    ):
        if module_info.name.startswith("_"):
            continue
        module = importlib.import_module(f"{tools.__name__}.{module_info.name}")
        module.register(mcp, api)
    return mcp
