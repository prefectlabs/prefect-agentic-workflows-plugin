"""Tests for publishing plans and listing plan versions."""

from typing import Any

import httpx
import pytest
import respx
from fastmcp import Client
from support import (
    FLOW_ID,
    OLDER_VERSION_ID,
    PLAN,
    PLAN_PATH,
    VERSION_ID,
    VERSIONS_PATH,
    active_state_response,
    error_text,
    request_body,
    version_response,
)

ACTIVATE_PATH = f"{VERSIONS_PATH}/{VERSION_ID}/activate"


def publish_result(**overrides: Any) -> dict[str, Any]:
    return {
        "published": True,
        "activated": False,
        "version_id": VERSION_ID,
        "version": version_response(),
        "active_state": None,
        "errors": [],
        "activation_error": None,
        **overrides,
    }


@pytest.fixture
def validate(cloud_api: respx.MockRouter) -> respx.Route:
    return cloud_api.post("/execution-plans/validate").respond(
        200, json={"valid": True, "errors": []}
    )


async def publish(client: Client[Any], **arguments: Any) -> Any:
    return await client.call_tool(
        "publish_plan",
        {"flow_id": FLOW_ID, "plan": PLAN, **arguments},
        raise_on_error=False,
    )


async def test_publish_plan_validates_creates_and_activates_a_version(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, validate: respx.Route
):
    create = cloud_api.post(VERSIONS_PATH).respond(201, json=version_response())
    activate = cloud_api.post(ACTIVATE_PATH).respond(200, json=active_state_response())

    result = await publish(mcp_client)

    assert result.structured_content == publish_result(
        activated=True, active_state=active_state_response()
    )
    assert request_body(validate.calls.last.request) == {"plan": PLAN}
    assert request_body(create.calls.last.request) == {"plan": PLAN}
    assert activate.called


async def test_publish_plan_without_activation_saves_but_does_not_activate(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, validate: respx.Route
):
    cloud_api.post(VERSIONS_PATH).respond(201, json=version_response())
    activate = cloud_api.post(ACTIVATE_PATH)

    result = await publish(mcp_client, activate=False)

    assert result.structured_content == publish_result()
    assert not activate.called


async def test_publish_plan_returns_the_saved_version_when_activation_fails(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, validate: respx.Route
):
    cloud_api.post(VERSIONS_PATH).respond(201, json=version_response())
    cloud_api.post(ACTIVATE_PATH).respond(
        422, json={"detail": "Manual evaluation can't be activated yet."}
    )

    result = await publish(mcp_client)

    content = result.structured_content
    assert content is not None
    assert content == publish_result(activation_error=content["activation_error"])
    assert "Manual evaluation can't be activated yet." in content["activation_error"]


async def test_publish_plan_saves_nothing_when_validation_fails(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    errors = [
        {"code": "unreachable_node", "phase": "semantic", "path": [], "message": "x"}
    ]
    cloud_api.post("/execution-plans/validate").respond(
        200, json={"valid": False, "errors": errors}
    )
    create = cloud_api.post(VERSIONS_PATH)

    result = await publish(mcp_client)

    assert result.structured_content == publish_result(
        published=False, version_id=None, version=None, errors=errors
    )
    assert not create.called


URL_PATH = ["nodes", "summarize", "mcp", "mcpServers", "slack", "url"]
CYCLE = {
    "code": "cycle_detected",
    "phase": "semantic",
    "path": ["edges", 1],
    "message": "cycle",
}


@pytest.mark.parametrize(
    ("body", "errors"),
    [
        ({"detail": [CYCLE]}, [CYCLE]),
        (
            {
                "exception_message": "Invalid request received.",
                "exception_detail": [
                    {
                        "type": "value_error",
                        "loc": ["body", "plan", *URL_PATH],
                        "msg": "bad",
                    }
                ],
            },
            [
                {
                    "code": "value_error",
                    "phase": "document_shape",
                    "path": URL_PATH,
                    "message": "bad",
                }
            ],
        ),
    ],
    ids=["graph-rule", "document-shape"],
)
async def test_publish_plan_returns_the_errors_from_a_rejected_create(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    validate: respx.Route,
    body: dict[str, Any],
    errors: list[dict[str, Any]],
):
    cloud_api.post(VERSIONS_PATH).respond(422, json=body)

    result = await publish(mcp_client)

    assert result.structured_content == publish_result(
        published=False, version_id=None, version=None, errors=errors
    )


async def test_publish_plan_names_the_missing_storage_bucket(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, validate: respx.Route
):
    cloud_api.post(VERSIONS_PATH).respond(
        409,
        json={"detail": "Workspace object storage bucket has not been provisioned."},
    )

    result = await publish(mcp_client)

    assert "no object storage bucket" in error_text(result)


@pytest.mark.parametrize(
    ("page", "active_version_id"), [(1, OLDER_VERSION_ID), (2, None)]
)
async def test_list_plan_versions_marks_the_active_version(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    page: int,
    active_version_id: str | None,
):
    listing = {"results": [{"id": VERSION_ID}], "count": 1, "pages": 2, "page": page}
    versions = cloud_api.get(VERSIONS_PATH).respond(200, json=listing)
    cloud_api.get(PLAN_PATH).respond(200, json=active_state_response(active_version_id))

    result = await mcp_client.call_tool(
        "list_plan_versions", {"flow_id": FLOW_ID, "page": page}
    )

    assert result.structured_content == {
        **listing,
        "active_version_id": active_version_id,
    }
    assert versions.calls.last.request.url.params["page"] == str(page)


@pytest.mark.usefixtures("validate")
async def test_publish_plan_reports_an_activation_whose_response_was_lost(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(VERSIONS_PATH).respond(201, json=version_response())
    cloud_api.post(ACTIVATE_PATH).mock(side_effect=httpx.ReadTimeout("lost"))
    cloud_api.get(PLAN_PATH).respond(200, json=active_state_response())

    result = await mcp_client.call_tool(
        "publish_plan", {"flow_id": FLOW_ID, "plan": PLAN}
    )

    assert result.structured_content is not None
    assert result.structured_content["activated"] is True
    assert result.structured_content["activation_error"] is None
