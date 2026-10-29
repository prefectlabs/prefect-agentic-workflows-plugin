"""A tool that lists the deployments a Deployment node can run."""

from typing import Annotated, Any
from uuid import UUID

from fastmcp import FastMCP
from pydantic import BaseModel, Field

from prefect_agentic_workflows_mcp.parameters import WorkspaceId
from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi

PAGE_SIZE = 200
# `list_deployments` returns at most this many deployments, so a workspace
# with hundreds of them doesn't overflow the agent's context.
LIST_LIMIT = 50
DESCRIPTION_LIMIT = 200


class DeploymentSummary(BaseModel):
    id: str
    name: str
    flow_name: str | None
    description: str | None


class DeploymentList(BaseModel):
    deployments: list[DeploymentSummary]
    has_more: bool


def summarize_description(description: str | None) -> str | None:
    """Return the first line of a description, cut to `DESCRIPTION_LIMIT`.

    Ends with "…" when text was left out.
    """
    text = (description or "").strip()
    if not text:
        return None
    summary = text.splitlines()[0].strip()[:DESCRIPTION_LIMIT]
    return summary if summary == text else f"{summary.rstrip()}…"


class Deployment(DeploymentSummary):
    parameters: dict[str, Any]
    parameter_openapi_schema: dict[str, Any] | None


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the deployment tools to `mcp`."""

    async def read_all(
        path: str, body: dict[str, Any], workspace_id: WorkspaceId = None
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = await api.call(
                "POST",
                path,
                json={**body, "offset": offset, "limit": PAGE_SIZE},
                workspace_id=workspace_id,
            )
            documents: list[dict[str, Any]] = page or []
            items.extend(documents)
            if len(documents) < PAGE_SIZE:
                return items
            offset += PAGE_SIZE

    @tool(mcp, read_only=True)
    async def list_deployments(
        name: Annotated[
            str | None,
            Field(
                description=(
                    "Part of a deployment or flow name to find. The match ignores case."
                )
            ),
        ] = None,
        workspace_id: WorkspaceId = None,
    ) -> DeploymentList:
        """List up to 50 of the workspace's deployments, sorted by name.

        Returns each deployment's `id`, `name`, `flow_name`, and the first
        line of its `description`. When `has_more` is true, the workspace has
        more deployments than the list shows: pass `name` to find one. A
        Deployment node runs a deployment by its `id`. A step that must run
        code, such as a script or a data load, needs a deployment that runs
        that code. Call `get_deployment` for the full description and the
        parameters of a deployment you plan to use.
        """
        body: dict[str, Any] = {"sort": "NAME_ASC", "limit": LIST_LIMIT + 1}
        if name:
            body["deployments"] = {"flow_or_deployment_name": {"like_": name}}
        page: list[dict[str, Any]] = await api.call(
            "POST", "/deployments/filter", json=body, workspace_id=workspace_id
        )
        deployments = page[:LIST_LIMIT]
        flow_ids = sorted({str(d["flow_id"]) for d in deployments if d.get("flow_id")})
        flow_names: dict[str, str] = {}
        if flow_ids:
            flows = await read_all(
                "/flows/filter",
                {"flows": {"id": {"any_": flow_ids}}},
                workspace_id=workspace_id,
            )
            flow_names = {str(flow["id"]): str(flow["name"]) for flow in flows}
        return DeploymentList(
            deployments=[
                DeploymentSummary(
                    id=str(deployment["id"]),
                    name=str(deployment["name"]),
                    flow_name=flow_names.get(str(deployment.get("flow_id"))),
                    description=summarize_description(deployment.get("description")),
                )
                for deployment in deployments
            ],
            has_more=len(page) > LIST_LIMIT,
        )

    @tool(mcp, read_only=True)
    async def get_deployment(
        deployment_id: UUID, workspace_id: WorkspaceId = None
    ) -> Deployment:
        """Read one deployment, with its parameters.

        Returns the same fields as `list_deployments`, with the full
        `description`, plus the default `parameters` and the
        `parameter_openapi_schema`. Use them to build a Deployment node's
        `parameters` input, and ask the user for any required parameter that
        has no default. Never ask the user to type a
        credential, such as a token or password: have them set it up inside
        the deployment instead.
        """
        deployment = await api.call(
            "GET", f"/deployments/{deployment_id}", workspace_id=workspace_id
        )
        flow_name = None
        if deployment.get("flow_id"):
            flow = await api.call(
                "GET", f"/flows/{deployment['flow_id']}", workspace_id=workspace_id
            )
            flow_name = str(flow["name"])
        return Deployment(
            id=str(deployment["id"]),
            name=str(deployment["name"]),
            flow_name=flow_name,
            description=deployment.get("description") or None,
            parameters=deployment.get("parameters") or {},
            parameter_openapi_schema=deployment.get("parameter_openapi_schema"),
        )
