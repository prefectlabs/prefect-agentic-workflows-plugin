"""Tests for the tools that publish, read, list, and activate plan versions."""

import json
from typing import Any

import respx
from fastmcp import Client

from support import error_text

FLOW_ID = "33333333-3333-3333-3333-333333333333"
VERSION_ID = "44444444-4444-4444-4444-444444444444"
OLDER_VERSION_ID = "55555555-5555-5555-5555-555555555555"
PLAN_PATH = f"/flows/{FLOW_ID}/execution-plan"
VERSIONS_PATH = f"{PLAN_PATH}/versions"

PLAN: dict[str, Any] = {
    "schema_version": "0.1",
    "kind": "execution_plan",
    "nodes": {"summarize": {"kind": "agent", "prompt": "Summarize the failures."}},
}


def version_response(version_id: str = VERSION_ID) -> dict[str, Any]:
    """Return a plan version in the shape Cloud returns from the version routes."""
    return {
        "id": version_id,
        "flow_id": FLOW_ID,
        "schema_version": "0.1",
        "semantic_hash": f"hash-{version_id[:4]}",
        "created": "2026-09-25T12:00:00Z",
        "created_by": {"type": "USER", "display_value": "marvin"},
        "plan": PLAN,
        "output_schemas": {},
    }


def active_state_response(version_id: str | None = VERSION_ID) -> dict[str, Any]:
    """Return the active-plan state Cloud returns for a flow."""
    if version_id is None:
        return {"active_version": None}
    version = version_response(version_id)
    del version["created_by"]
    return {
        "active_version": {
            **version,
            "activated": "2026-09-25T12:01:00Z",
            "activated_by": {"type": "USER", "display_value": "marvin"},
        }
    }


def mock_valid_plan(cloud_api: respx.MockRouter) -> respx.Route:
    return cloud_api.post("/execution-plans/validate").respond(
        200, json={"valid": True, "errors": []}
    )


async def test_publish_plan_validates_creates_and_activates_a_version(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    validate = mock_valid_plan(cloud_api)
    create = cloud_api.post(VERSIONS_PATH).respond(201, json=version_response())
    activate = cloud_api.post(f"{VERSIONS_PATH}/{VERSION_ID}/activate").respond(
        200, json=active_state_response()
    )

    result = await mcp_client.call_tool(
        "publish_plan", {"flow_id": FLOW_ID, "plan": PLAN}
    )

    assert result.structured_content == {
        "published": True,
        "activated": True,
        "version_id": VERSION_ID,
        "version": version_response(),
        "active_state": active_state_response(),
        "errors": [],
        "activation_error": None,
    }
    assert json.loads(validate.calls.last.request.content) == {"plan": PLAN}
    assert json.loads(create.calls.last.request.content) == {"plan": PLAN}
    assert activate.called


async def test_publish_plan_without_activation_leaves_the_active_version_unchanged(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    mock_valid_plan(cloud_api)
    cloud_api.post(VERSIONS_PATH).respond(201, json=version_response())
    activate = cloud_api.post(url__regex=r".*/activate$")
    cloud_api.get(PLAN_PATH).respond(200, json=active_state_response(OLDER_VERSION_ID))

    result = await mcp_client.call_tool(
        "publish_plan", {"flow_id": FLOW_ID, "plan": PLAN, "activate": False}
    )
    active = await mcp_client.call_tool("get_plan", {"flow_id": FLOW_ID})

    assert result.structured_content is not None
    assert result.structured_content["published"] is True
    assert result.structured_content["activated"] is False
    assert result.structured_content["version_id"] == VERSION_ID
    assert result.structured_content["active_state"] is None
    assert not activate.called
    assert active.structured_content == active_state_response(OLDER_VERSION_ID)


async def test_publish_plan_saves_nothing_when_validation_fails(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    errors = [
        {
            "code": "unreachable_node",
            "phase": "semantic",
            "path": ["nodes", "summarize"],
            "message": "Node 'summarize' is not reachable from a start node.",
        }
    ]
    cloud_api.post("/execution-plans/validate").respond(
        200, json={"valid": False, "errors": errors}
    )
    create = cloud_api.post(VERSIONS_PATH)

    result = await mcp_client.call_tool(
        "publish_plan", {"flow_id": FLOW_ID, "plan": PLAN}
    )

    assert not result.is_error
    assert result.structured_content == {
        "published": False,
        "activated": False,
        "version_id": None,
        "version": None,
        "active_state": None,
        "errors": errors,
        "activation_error": None,
    }
    assert not create.called


async def test_publish_plan_returns_the_errors_from_a_rejected_create(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    mock_valid_plan(cloud_api)
    error = {
        "code": "unsupported_orchestration_node",
        "phase": "semantic",
        "path": ["nodes", "wait"],
        "message": "Timer nodes can't be activated yet.",
    }
    cloud_api.post(VERSIONS_PATH).respond(422, json={"detail": [error]})

    result = await mcp_client.call_tool(
        "publish_plan", {"flow_id": FLOW_ID, "plan": PLAN}
    )

    assert result.structured_content is not None
    assert result.structured_content["published"] is False
    assert result.structured_content["errors"] == [error]


async def test_publish_plan_reports_an_mcp_hostname_that_does_not_resolve(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    mock_valid_plan(cloud_api)
    url_path = ["nodes", "summarize", "mcp", "mcpServers", "slack", "url"]
    message = "Value error, MCP HTTP server URL is invalid or restricted."
    cloud_api.post(VERSIONS_PATH).respond(
        422,
        json={
            "exception_message": "Invalid request received.",
            "exception_detail": [
                {
                    "type": "value_error",
                    "loc": ["body", "plan", *url_path],
                    "msg": message,
                }
            ],
            "request_body": {"plan": PLAN},
        },
    )

    result = await mcp_client.call_tool(
        "publish_plan", {"flow_id": FLOW_ID, "plan": PLAN}
    )

    assert result.structured_content is not None
    assert result.structured_content["published"] is False
    assert result.structured_content["errors"] == [
        {
            "code": "value_error",
            "phase": "document_shape",
            "path": url_path,
            "message": message,
        }
    ]


async def test_publish_plan_reports_a_missing_secret_scope_with_the_api_message(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    mock_valid_plan(cloud_api)
    detail = (
        "Actor does not have permission to reference secret blocks in execution "
        "plan MCP credentials. Requires the see_secret_blocks scope."
    )
    cloud_api.post(VERSIONS_PATH).respond(403, json={"detail": detail})

    result = await mcp_client.call_tool(
        "publish_plan", {"flow_id": FLOW_ID, "plan": PLAN}, raise_on_error=False
    )

    message = error_text(result)
    assert "403" in message
    assert detail in message


async def test_publish_plan_names_the_missing_storage_bucket(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    mock_valid_plan(cloud_api)
    cloud_api.post(VERSIONS_PATH).respond(
        409,
        json={"detail": "Workspace object storage bucket has not been provisioned."},
    )

    result = await mcp_client.call_tool(
        "publish_plan", {"flow_id": FLOW_ID, "plan": PLAN}, raise_on_error=False
    )

    message = error_text(result)
    assert "no object storage bucket" in message
    assert "configure" in message


async def test_publish_plan_returns_the_saved_version_when_activation_fails(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    mock_valid_plan(cloud_api)
    cloud_api.post(VERSIONS_PATH).respond(201, json=version_response())
    cloud_api.post(f"{VERSIONS_PATH}/{VERSION_ID}/activate").respond(
        422,
        json={
            "detail": [
                {
                    "code": "unsupported_orchestration_mode",
                    "phase": "semantic",
                    "path": ["orchestration"],
                    "message": "Manual evaluation can't be activated yet.",
                }
            ]
        },
    )

    result = await mcp_client.call_tool(
        "publish_plan", {"flow_id": FLOW_ID, "plan": PLAN}
    )

    content = result.structured_content
    assert content is not None
    assert content["published"] is True
    assert content["activated"] is False
    assert content["version_id"] == VERSION_ID
    assert "Manual evaluation can't be activated yet." in content["activation_error"]


async def test_get_plan_reads_a_flow_with_no_active_plan(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(PLAN_PATH).respond(200, json=active_state_response(None))

    result = await mcp_client.call_tool("get_plan", {"flow_id": FLOW_ID})

    assert result.structured_content == {"active_version": None}


async def test_get_plan_reads_a_specific_version(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    older = version_response(OLDER_VERSION_ID)
    cloud_api.get(f"{VERSIONS_PATH}/{OLDER_VERSION_ID}").respond(200, json=older)

    result = await mcp_client.call_tool(
        "get_plan", {"flow_id": FLOW_ID, "version_id": OLDER_VERSION_ID}
    )

    assert result.structured_content == older


async def test_get_plan_reports_a_missing_version(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(f"{VERSIONS_PATH}/{OLDER_VERSION_ID}").respond(
        404, json={"detail": "Execution plan version not found"}
    )

    result = await mcp_client.call_tool(
        "get_plan",
        {"flow_id": FLOW_ID, "version_id": OLDER_VERSION_ID},
        raise_on_error=False,
    )

    assert "Execution plan version not found" in error_text(result)


async def test_get_plan_rejects_a_flow_id_that_is_not_a_uuid(
    mcp_client: Client[Any],
):
    result = await mcp_client.call_tool(
        "get_plan", {"flow_id": "daily-summary"}, raise_on_error=False
    )

    assert result.is_error


async def test_list_plan_versions_returns_summaries_and_the_active_version_id(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    summaries = []
    for version_id in (VERSION_ID, OLDER_VERSION_ID):
        summary = version_response(version_id)
        del summary["plan"], summary["output_schemas"]
        summaries.append(summary)
    page = {"results": summaries, "count": 2, "limit": 200, "pages": 1, "page": 1}
    listing = cloud_api.get(VERSIONS_PATH).respond(200, json=page)
    cloud_api.get(PLAN_PATH).respond(200, json=active_state_response(OLDER_VERSION_ID))

    result = await mcp_client.call_tool("list_plan_versions", {"flow_id": FLOW_ID})

    assert result.structured_content == {**page, "active_version_id": OLDER_VERSION_ID}
    assert listing.calls.last.request.url.params["page"] == "1"


async def test_list_plan_versions_reads_the_requested_page_with_no_active_version(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    page = {"results": [], "count": 0, "limit": 200, "pages": 0, "page": 2}
    listing = cloud_api.get(VERSIONS_PATH).respond(200, json=page)
    cloud_api.get(PLAN_PATH).respond(200, json=active_state_response(None))

    result = await mcp_client.call_tool(
        "list_plan_versions", {"flow_id": FLOW_ID, "page": 2}
    )

    assert result.structured_content == {**page, "active_version_id": None}
    assert listing.calls.last.request.url.params["page"] == "2"


async def test_activate_plan_version_rolls_back_to_an_older_version(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    activate = cloud_api.post(f"{VERSIONS_PATH}/{OLDER_VERSION_ID}/activate").respond(
        200, json=active_state_response(OLDER_VERSION_ID)
    )

    result = await mcp_client.call_tool(
        "activate_plan_version",
        {"flow_id": FLOW_ID, "version_id": OLDER_VERSION_ID},
    )

    assert result.structured_content == active_state_response(OLDER_VERSION_ID)
    assert activate.called


async def test_activate_plan_version_names_the_missing_storage_bucket(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(f"{VERSIONS_PATH}/{OLDER_VERSION_ID}/activate").respond(
        409,
        json={"detail": "Workspace object storage bucket has not been provisioned."},
    )

    result = await mcp_client.call_tool(
        "activate_plan_version",
        {"flow_id": FLOW_ID, "version_id": OLDER_VERSION_ID},
        raise_on_error=False,
    )

    assert "no object storage bucket" in error_text(result)


async def test_a_conflict_other_than_a_missing_bucket_keeps_the_api_message(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(f"{VERSIONS_PATH}/{OLDER_VERSION_ID}/activate").respond(
        409, json={"detail": "The flow is being updated concurrently."}
    )

    result = await mcp_client.call_tool(
        "activate_plan_version",
        {"flow_id": FLOW_ID, "version_id": OLDER_VERSION_ID},
        raise_on_error=False,
    )

    message = error_text(result)
    assert "409" in message
    assert "The flow is being updated concurrently." in message
    assert "bucket" not in message


async def test_plan_version_tools_are_annotated(mcp_client: Client[Any]):
    tools = {tool.name: tool for tool in await mcp_client.list_tools()}
    expected = {
        "publish_plan": (False, True),
        "activate_plan_version": (False, True),
        "get_plan": (True, False),
        "list_plan_versions": (True, False),
    }

    for name, (read_only, destructive) in expected.items():
        annotations = tools[name].annotations
        assert annotations is not None, name
        assert annotations.readOnlyHint is read_only, name
        assert annotations.destructiveHint is destructive, name
