"""Tools that read the execution-plan schema and validate plan drafts."""

from typing import Annotated, Any

from fastmcp import FastMCP
from pydantic import Field

from prefect_agentic_workflows_mcp.parameters import PlanDocument
from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi

SchemaVersion = Annotated[
    str | None,
    Field(
        description=(
            "Schema version to read, for example '0.1'. Omit it to read the "
            "current version."
        ),
    ),
]


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the schema tools to `mcp`."""

    @tool(mcp, read_only=True)
    async def get_schema(version: SchemaVersion = None) -> dict[str, Any]:
        """Read the JSON Schema for execution-plan documents from Prefect Cloud.

        Call this before drafting a plan. Omitting `version` returns the
        platform's current version, which can be older than the newest one in
        `supported_schema_versions`. The schema covers document shape only.
        Call `validate_plan` to check a draft against the graph rules too.
        """
        params = {"version": version} if version is not None else None
        return await api.call("GET", "/execution-plans/schema", params=params)

    @tool(mcp, read_only=True)
    async def validate_plan(plan: PlanDocument) -> dict[str, Any]:
        """Check an execution-plan draft without saving it.

        Returns `valid` and a list of `errors`. Each error has a `code`, a
        `phase` (`document_shape` or `semantic`), a `path` into the plan, and
        a `message`. Fix every error and validate again before publishing.
        A plan that passes can still fail on publish. Publishing also checks
        that you can see every referenced Secret block, and it checks MCP
        server hostnames again, so a DNS change between the two calls can fail.
        """
        return await api.call("POST", "/execution-plans/validate", json={"plan": plan})
