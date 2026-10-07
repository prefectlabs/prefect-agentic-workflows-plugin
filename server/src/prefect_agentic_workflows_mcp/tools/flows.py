"""Tools that find or create the flow an execution plan belongs to."""

from typing import Annotated, Any
from urllib.parse import quote

from fastmcp import FastMCP
from pydantic import Field

from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi, read_json

FlowName = Annotated[
    str,
    Field(
        min_length=1,
        description="Flow name. Flow names are unique in a workspace.",
    ),
]
FlowTags = Annotated[
    list[str] | None,
    Field(
        description=(
            "Tags for the flow. They are only set when the tool creates the "
            "flow. An existing flow keeps its tags."
        ),
    ),
]


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the flow tools to `mcp`."""

    @tool(mcp, read_only=False)
    async def get_or_create_flow(
        name: FlowName, tags: FlowTags = None
    ) -> dict[str, Any]:
        """Return the flow with this name, and create it if it doesn't exist.

        An execution plan is attached to a flow. Call this before
        `publish_plan` and pass the returned `flow.id` to it. Publishing to a
        flow that already has a plan adds a new version to that flow, so
        reusing a name never creates a duplicate flow.

        Returns `created`, which is true when this call created the flow, and
        `flow`, the flow as Prefect Cloud returns it.
        """
        lookup_path = f"/flows/name/{quote(name, safe='')}"
        response = await api.request("GET", lookup_path)
        if response.status_code != 404:
            return {"created": False, "flow": read_json(response, "GET", lookup_path)}

        response = await api.request(
            "POST", "/flows/", json={"name": name, "tags": tags or []}
        )
        flow = read_json(response, "POST", "/flows/")
        # Cloud answers 200 instead of 201 when another caller created the flow
        # between the lookup and this request.
        return {"created": response.status_code == 201, "flow": flow}
