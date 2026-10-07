"""A tool that lists the deployments a Deployment node can run."""

import re
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
# Keywords that list allowed values. They stay, except under a parameter whose
# name looks like a credential, where an allowed value can be the secret itself.
LITERAL_KEYWORDS = frozenset({"const", "enum"})
CREDENTIAL_NAME = re.compile(
    r"token|secret|password|passwd|credential|api_?key|access_?key|private_?key|auth",
    re.IGNORECASE,
)


def ref_names(schema: Any) -> set[str]:
    """Return the definition names that `$ref` values in a schema point at."""
    if isinstance(schema, list):
        return set().union(*(ref_names(item) for item in schema))
    if not isinstance(schema, dict):
        return set()
    names = {schema["$ref"].rsplit("/", 1)[-1]} if "$ref" in schema else set()
    return names.union(*(ref_names(value) for value in schema.values()))


def credential_definitions(schema: dict[str, Any]) -> set[str]:
    """Return the definitions a credential-like parameter uses, directly or not."""
    definitions = {
        name: value
        for key in ("definitions", "$defs")
        for name, value in (schema.get(key) or {}).items()
    }
    found = set().union(
        *(
            ref_names(value)
            for name, value in (schema.get("properties") or {}).items()
            if CREDENTIAL_NAME.search(name)
        )
    )
    pending = list(found)
    while pending:
        for name in ref_names(definitions.get(pending.pop())) - found:
            found.add(name)
            pending.append(name)
    return found


def without_values(schema: Any) -> Any:
    """Return a JSON Schema without values that can hold credentials.

    Default and example values are always removed. Allowed values (`const`
    and `enum`) are removed only under a parameter with a credential-like
    name, and in every definition such a parameter uses through `$ref`, so
    ordinary choices such as a list of regions stay.
    """
    sensitive = credential_definitions(schema) if isinstance(schema, dict) else set()

    def strip(node: Any, *, in_map: bool, credential: bool) -> Any:
        if isinstance(node, list):
            return [strip(item, in_map=False, credential=credential) for item in node]
        if not isinstance(node, dict):
            return node
        if in_map:
            return {
                key: strip(
                    value,
                    in_map=False,
                    credential=credential
                    or key in sensitive
                    or bool(CREDENTIAL_NAME.search(key)),
                )
                for key, value in node.items()
            }
        removed = VALUE_KEYWORDS | (LITERAL_KEYWORDS if credential else frozenset())
        return {
            key: strip(value, in_map=key in SCHEMA_MAPS, credential=credential)
            for key, value in node.items()
            if key not in removed
        }

    return strip(schema, in_map=False, credential=False)


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
        input, and ask the user for any required parameter that has no default,
        except a credential such as a token or password. Never ask for a
        credential's value: tell the user to set it up inside the deployment
        instead, for example as a Secret block the flow reads.
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
