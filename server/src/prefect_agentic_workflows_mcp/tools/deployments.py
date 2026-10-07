"""A tool that lists the deployments a Deployment node can run."""

from typing import Any
from uuid import UUID

from fastmcp import FastMCP
from pydantic import BaseModel

from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi

PAGE_SIZE = 200


class DeploymentSummary(BaseModel):
    id: str
    name: str
    flow_name: str | None
    description: str | None


class DeploymentList(BaseModel):
    deployments: list[DeploymentSummary]


class Deployment(DeploymentSummary):
    parameters: dict[str, Any]
    parameter_openapi_schema: dict[str, Any] | None


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the deployment tools to `mcp`."""

    async def read_all(path: str, body: dict[str, Any]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = await api.call(
                "POST", path, json={**body, "offset": offset, "limit": PAGE_SIZE}
            )
            documents: list[dict[str, Any]] = page or []
            items.extend(documents)
            if len(documents) < PAGE_SIZE:
                return items
            offset += PAGE_SIZE

    @tool(mcp, read_only=True)
    async def list_deployments() -> DeploymentList:
        """List the deployments in the workspace.

        Returns each deployment's `id`, `name`, `flow_name`, and
        `description`, sorted by deployment name. A Deployment node runs a
        deployment by its `id`. A step that must run code, such as a script
        or a data load, needs a deployment that runs that code. Call
        `get_deployment` for the parameters of a deployment you plan to use.
        """
        deployments = await read_all("/deployments/filter", {"sort": "NAME_ASC"})
        flow_ids = sorted({str(d["flow_id"]) for d in deployments if d.get("flow_id")})
        flow_names: dict[str, str] = {}
        if flow_ids:
            flows = await read_all(
                "/flows/filter", {"flows": {"id": {"any_": flow_ids}}}
            )
            flow_names = {str(flow["id"]): str(flow["name"]) for flow in flows}
        return DeploymentList(
            deployments=[
                DeploymentSummary(
                    id=str(deployment["id"]),
                    name=str(deployment["name"]),
                    flow_name=flow_names.get(str(deployment.get("flow_id"))),
                    description=deployment.get("description") or None,
                )
                for deployment in deployments
            ]
        )

    @tool(mcp, read_only=True)
    async def get_deployment(deployment_id: UUID) -> Deployment:
        """Read one deployment, with its parameters.

        Returns the same fields as `list_deployments`, plus the default
        `parameters` and the `parameter_openapi_schema`. Use them to build a
        Deployment node's `parameters` input, and ask the user for any
        required parameter that has no default. Never ask the user to type a
        credential, such as a token or password: have them set it up inside
        the deployment instead.
        """
        deployment = await api.call("GET", f"/deployments/{deployment_id}")
        flow_name = None
        if deployment.get("flow_id"):
            flow = await api.call("GET", f"/flows/{deployment['flow_id']}")
            flow_name = str(flow["name"])
        return Deployment(
            id=str(deployment["id"]),
            name=str(deployment["name"]),
            flow_name=flow_name,
            description=deployment.get("description") or None,
            parameters=deployment.get("parameters") or {},
            parameter_openapi_schema=deployment.get("parameter_openapi_schema"),
        )
