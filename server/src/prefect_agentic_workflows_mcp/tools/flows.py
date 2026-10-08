"""Tools that find or create the flow an execution plan belongs to."""

from typing import Annotated, Any
from urllib.parse import quote

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
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
# Names that a URL path would read as "this directory" or "the parent".
DOT_SEGMENTS = frozenset({".", ".."})
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

    async def find_flow(name: str) -> dict[str, Any] | None:
        if name in DOT_SEGMENTS:
            raise ToolError(f"{name!r} isn't a valid flow name.")
        lookup_path = f"/flows/name/{quote(name, safe='')}"
        response = await api.request("GET", lookup_path)
        if response.status_code == 404:
            return None
        return read_json(response, "GET", lookup_path)

    @tool(mcp, read_only=True)
    async def get_flow(name: FlowName) -> dict[str, Any]:
        """Return the existing flow with this name, without creating one.

        Use this to find a published workflow to run, roll back, or schedule.
        Returns `found` and, when it is true, the `flow`.
        """
        flow = await find_flow(name)
        return {"found": flow is not None, "flow": flow}

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
        flow = await find_flow(name)
        if flow is not None:
            return {"created": False, "flow": flow}

        response = await api.request(
            "POST", "/flows/", json={"name": name, "tags": tags or []}
        )
        flow = read_json(response, "POST", "/flows/")
        # Cloud answers 200 instead of 201 when another caller created the flow
        # between the lookup and this request.
        return {"created": response.status_code == 201, "flow": flow}
