"""Tools that publish, read, list, and activate a flow's plan versions.

Plan versions are immutable. Publishing always adds a new version, and a flow
runs whichever version is active.

`publish_plan` sends no `layout` with a new version. The Cloud UI computes the
graph layout from the plan's nodes and routes each time it draws a plan, and
it doesn't read a stored layout (checked on 2026-09-25). A plan published
without a layout displays the same as one published with a layout, so the
server doesn't generate one.
"""

from typing import Annotated, Any
from uuid import UUID

import httpx
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from prefect_agentic_workflows_mcp.parameters import PlanDocument
from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import (
    WorkspaceApi,
    read_json,
)

FlowId = Annotated[
    UUID,
    Field(description="ID of the flow that owns the plan, from `get_or_create_flow`."),
]
VersionId = Annotated[
    UUID,
    Field(description="ID of a plan version, from `list_plan_versions`."),
]


def publish_result(
    *,
    errors: list[dict[str, Any]] | None = None,
    version: dict[str, Any] | None = None,
    active_state: dict[str, Any] | None = None,
    activation_error: str | None = None,
) -> dict[str, Any]:
    """Return a `publish_plan` result. Every result has the same keys."""
    return {
        "published": version is not None,
        "activated": active_state is not None,
        "version_id": version["id"] if version is not None else None,
        "version": version,
        "active_state": active_state,
        "errors": errors or [],
        "activation_error": activation_error,
    }


def create_errors(response: httpx.Response) -> list[dict[str, Any]]:
    """Return the plan errors from a 422 response to a create-version request.

    Cloud sends two shapes. Graph-rule failures come as `detail`, a list of
    errors shaped like `validate_plan` errors. Document-shape failures, such
    as an MCP server URL whose hostname doesn't resolve, come as
    `exception_detail`, a list of request-validation errors whose `loc`
    starts with `body` and `plan`. Both become `validate_plan` errors, with
    paths into the plan. Returns an empty list for any other body.
    """
    if response.status_code != 422:
        return []
    try:
        body = response.json()
    except ValueError:
        return []
    if not isinstance(body, dict):
        return []

    errors: list[dict[str, Any]] = []
    detail = body.get("detail")
    if isinstance(detail, list):
        for error in detail:
            if isinstance(error, dict) and {"code", "message"} <= error.keys():
                errors.append(
                    {
                        "code": error["code"],
                        "phase": error.get("phase", "semantic"),
                        "path": error.get("path", []),
                        "message": error["message"],
                    }
                )
    exception_detail = body.get("exception_detail")
    if isinstance(exception_detail, list):
        for error in exception_detail:
            if isinstance(error, dict) and {"type", "msg"} <= error.keys():
                path = list(error.get("loc", []))
                if path[:2] == ["body", "plan"]:
                    path = path[2:]
                errors.append(
                    {
                        "code": error["type"],
                        "phase": "document_shape",
                        "path": path,
                        "message": error["msg"],
                    }
                )
    return errors


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the plan version tools to `mcp`."""

    @tool(mcp, read_only=False, destructive=True)
    async def publish_plan(
        flow_id: FlowId,
        plan: PlanDocument,
        activate: Annotated[
            bool,
            Field(
                description=(
                    "Make the new version the flow's active version. Set it to "
                    "false to save the version for review and leave the active "
                    "version unchanged."
                ),
            ),
        ] = True,
    ) -> dict[str, Any]:
        """Validate a plan, save it as a new version of the flow, and activate it.

        Returns `published`, `activated`, the new `version_id` and `version`,
        and `active_state`, the flow's active version after activation.

        When the plan doesn't pass, nothing is saved: `published` is false
        and `errors` lists what to fix, in the same shape as `validate_plan`
        errors. Saving checks more than `validate_plan` does. It checks that
        every MCP server hostname resolves and that you can see every Secret
        block the plan references. So a plan that passed `validate_plan` can
        still come back with `errors`, or fail with the API's message.

        When the version is saved but activation fails, `published` is true,
        `activated` is false, and `activation_error` says why. Fix the cause,
        then call `activate_plan_version` with the returned `version_id`.
        """
        validation = await api.call(
            "POST", "/execution-plans/validate", json={"plan": plan}
        )
        if not validation.get("valid"):
            return publish_result(errors=validation.get("errors", []))

        versions_path = f"/flows/{flow_id}/execution-plan/versions"
        response = await api.request("POST", versions_path, json={"plan": plan})
        errors = create_errors(response)
        if errors:
            return publish_result(errors=errors)
        version = read_json(response, "POST", versions_path)

        if not activate:
            return publish_result(version=version)
        try:
            active_state = await api.call(
                "POST", f"{versions_path}/{version['id']}/activate"
            )
        except ToolError as exc:
            # Cloud may have activated the version before the connection
            # failed, so read the active version before reporting a failure.
            active_state = await api.call("GET", f"/flows/{flow_id}/execution-plan")
            active = (active_state or {}).get("active_version") or {}
            if active.get("id") != version["id"]:
                return publish_result(version=version, activation_error=str(exc))
        return publish_result(version=version, active_state=active_state)

    @tool(mcp, read_only=True)
    async def get_plan(
        flow_id: FlowId,
        version_id: Annotated[
            UUID | None,
            Field(
                description=(
                    "ID of the plan version to read. Omit it to read the "
                    "flow's active version."
                ),
            ),
        ] = None,
    ) -> dict[str, Any]:
        """Read a flow's active plan, or one of its plan versions.

        Without `version_id`, returns `active_version`, which is null when
        the flow has no active plan. The active version includes when it was
        activated and by whom. With `version_id`, returns that version.

        Each version includes the `plan` document and `output_schemas`, the
        JSON Schema of each named plan output. To change a plan, edit the
        `plan` document and publish it as a new version.
        """
        if version_id is None:
            return await api.call("GET", f"/flows/{flow_id}/execution-plan")
        return await api.call(
            "GET", f"/flows/{flow_id}/execution-plan/versions/{version_id}"
        )

    @tool(mcp, read_only=True)
    async def list_plan_versions(
        flow_id: FlowId,
        page: Annotated[
            int,
            Field(ge=1, description="Page of results to read, starting at 1."),
        ] = 1,
    ) -> dict[str, Any]:
        """List a flow's plan versions, without their plan documents.

        Returns `results`, one summary per version with its `id`,
        `schema_version`, `semantic_hash`, `created`, and `created_by`, plus
        `count`, `page`, and `pages` for paging. `active_version_id` is the
        ID of the flow's active version, or null when no version is active.
        Two versions with the same `semantic_hash` describe the same graph.

        Call `get_plan` with a version's ID to read its plan, and
        `activate_plan_version` to roll back to it.
        """
        versions = await api.call(
            "GET",
            f"/flows/{flow_id}/execution-plan/versions",
            params={"page": page},
        )
        active_state = await api.call("GET", f"/flows/{flow_id}/execution-plan")
        active_version = active_state.get("active_version")
        return {
            **versions,
            "active_version_id": active_version["id"] if active_version else None,
        }

    @tool(mcp, read_only=False, destructive=True)
    async def activate_plan_version(
        flow_id: FlowId, version_id: VersionId
    ) -> dict[str, Any]:
        """Make a saved plan version the flow's active version.

        The flow's next runs, including scheduled runs, use this version.
        Runs that already started keep the version they started with. Use it
        to activate a version published with `activate` set to false, or to
        roll back to an older version.

        Returns `active_version`, the flow's active version after the change.
        """
        return await api.call(
            "POST",
            f"/flows/{flow_id}/execution-plan/versions/{version_id}/activate",
        )
