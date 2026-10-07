"""A tool that lists the deployments a Deployment node can run."""

from typing import Any

from fastmcp import FastMCP
from pydantic import BaseModel

from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi

PAGE_SIZE = 200

# Schema keywords whose values are maps of names to schemas. A key inside one is
# a parameter or definition name, not a keyword, so it is never removed.
SCHEMA_MAPS = frozenset({"properties", "patternProperties", "definitions", "$defs"})
# Keywords that carry example or default values, which can hold credentials.
VALUE_KEYWORDS = frozenset({"default", "examples", "example"})


def without_values(schema: Any, *, in_map: bool = False) -> Any:
    """Return a JSON Schema without its default and example values."""
    if isinstance(schema, list):
        return [without_values(item) for item in schema]
    if not isinstance(schema, dict):
        return schema
    return {
        key: without_values(value, in_map=not in_map and key in SCHEMA_MAPS)
        for key, value in schema.items()
        if in_map or key not in VALUE_KEYWORDS
    }


class Deployment(BaseModel):
    id: str
    name: str
    flow_name: str | None
    description: str | None
    parameters_with_defaults: list[str]
    parameter_openapi_schema: dict[str, Any] | None


class DeploymentList(BaseModel):
    deployments: list[Deployment]


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the deployment tool to `mcp`."""

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

        Returns each deployment's `id`, `name`, `flow_name`, `description`,
        `parameter_openapi_schema`, and `parameters_with_defaults`, sorted by
        deployment name. `parameters_with_defaults` names the parameters that
        have a stored default. Default and example values are left out of both
        fields, because they can hold credentials. A Deployment node runs a
        deployment by its `id`. Use the schema to build the node's `parameters`
        input, and ask the user for any required parameter that has no default.
        A step that must run code, such as a script or a data load, needs a
        deployment that runs that code.
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
                Deployment(
                    id=str(deployment["id"]),
                    name=str(deployment["name"]),
                    flow_name=flow_names.get(str(deployment.get("flow_id"))),
                    description=deployment.get("description") or None,
                    parameters_with_defaults=sorted(deployment.get("parameters") or {}),
                    parameter_openapi_schema=without_values(
                        deployment.get("parameter_openapi_schema")
                    ),
                )
                for deployment in deployments
            ]
        )
