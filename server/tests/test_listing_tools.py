"""Tests for `list_secret_blocks` and `list_deployments`, which read every page."""

import json
from typing import Any

import httpx
import respx
from fastmcp import Client
from support import request_body

SLACK_TOKEN_ID = "aaaaaaaa-0000-0000-0000-000000000001"
GITHUB_ID = "aaaaaaaa-0000-0000-0000-000000000002"
LOAD_FLOW_ID = "cccccccc-0000-0000-0000-000000000001"
REPORT_FLOW_ID = "cccccccc-0000-0000-0000-000000000002"


def block_document(
    block_id: str, name: str, slug: str = "secret", value: str = "********"
) -> dict[str, Any]:
    """Return a block document in the shape `/block_documents/filter` returns."""
    return {
        "id": block_id,
        "name": name,
        "data": {"value": value},
        "block_type_name": slug.title(),
        "block_type": {"name": slug.title(), "slug": slug},
    }


# The schema Cloud stores for a flow with a credential default, and a parameter
# that happens to be named `default`.
STORED_SCHEMA = {
    "type": "object",
    "properties": {
        "region": {"type": "string"},
        "api_token": {"type": "string", "default": "sk-live-not-for-the-agent"},
        "default": {"type": "boolean", "default": True, "examples": [False]},
    },
    "required": ["region"],
}
RETURNED_SCHEMA = {
    "type": "object",
    "properties": {
        "region": {"type": "string"},
        "api_token": {"type": "string"},
        "default": {"type": "boolean"},
    },
    "required": ["region"],
}


def deployment(
    deployment_id: str,
    name: str,
    flow_id: str,
    description: str | None = None,
    parameters: dict[str, Any] | None = None,
    parameter_openapi_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a deployment in the shape `/deployments/filter` returns."""
    return {
        "id": deployment_id,
        "name": name,
        "flow_id": flow_id,
        "description": description,
        "parameters": parameters or {},
        "parameter_openapi_schema": parameter_openapi_schema,
    }


def pages(*bodies: list[dict[str, Any]]) -> list[httpx.Response]:
    return [httpx.Response(200, json=body) for body in bodies]


def offsets(route: respx.Route) -> list[int]:
    return [request_body(call.request)["offset"] for call in route.calls]


async def test_list_secret_blocks_returns_only_secret_names_and_ids(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post("/block_documents/filter").respond(
        200,
        json=[
            block_document(SLACK_TOKEN_ID, "slack-token", value="xoxb-real-value"),
            block_document(GITHUB_ID, "github", slug="github-credentials"),
        ],
    )

    result = await mcp_client.call_tool("list_secret_blocks", {})

    assert result.structured_content == {
        "secret_blocks": [{"name": "slack-token", "id": SLACK_TOKEN_ID}]
    }
    assert "xoxb-real-value" not in str(result.content)
    body = request_body(route.calls.last.request)
    assert body["block_types"] == {"slug": {"any_": ["secret"]}}
    assert body["include_secrets"] is False


async def test_list_secret_blocks_reads_every_page(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    first_page = [
        block_document(f"bbbbbbbb-0000-0000-0000-{i:012d}", f"s-{i:03d}")
        for i in range(200)
    ]
    route = cloud_api.post("/block_documents/filter").mock(
        side_effect=pages(first_page, [block_document(SLACK_TOKEN_ID, "slack-token")])
    )

    result = await mcp_client.call_tool("list_secret_blocks", {})

    assert result.structured_content is not None
    blocks = result.structured_content["secret_blocks"]
    assert len(blocks) == 201
    assert blocks[-1] == {"name": "slack-token", "id": SLACK_TOKEN_ID}
    assert offsets(route) == [0, 200]


async def test_list_deployments_returns_names_flow_names_and_parameters(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    first_page = [
        deployment(f"eeeeeeee-0000-0000-0000-{i:012d}", f"d-{i:03d}", LOAD_FLOW_ID)
        for i in range(199)
    ] + [
        deployment(
            GITHUB_ID,
            "nightly",
            LOAD_FLOW_ID,
            "Loads orders",
            parameters={"region": "us", "api_token": "sk-live-not-for-the-agent"},
            parameter_openapi_schema=STORED_SCHEMA,
        )
    ]
    last_page = [deployment(SLACK_TOKEN_ID, "weekly", REPORT_FLOW_ID)]
    deployments_route = cloud_api.post("/deployments/filter").mock(
        side_effect=pages(first_page, last_page)
    )
    flows_route = cloud_api.post("/flows/filter").respond(
        200,
        json=[
            {"id": LOAD_FLOW_ID, "name": "load-orders"},
            {"id": REPORT_FLOW_ID, "name": "report"},
        ],
    )

    result = await mcp_client.call_tool("list_deployments", {})

    assert result.structured_content is not None
    assert result.structured_content["deployments"][-2:] == [
        {
            "id": GITHUB_ID,
            "name": "nightly",
            "flow_name": "load-orders",
            "description": "Loads orders",
            "parameters_with_defaults": ["api_token", "region"],
            "parameter_openapi_schema": RETURNED_SCHEMA,
        },
        {
            "id": SLACK_TOKEN_ID,
            "name": "weekly",
            "flow_name": "report",
            "description": None,
            "parameters_with_defaults": [],
            "parameter_openapi_schema": None,
        },
    ]
    assert "sk-live-not-for-the-agent" not in json.dumps(result.structured_content)
    assert len(result.structured_content["deployments"]) == 201
    assert offsets(deployments_route) == [0, 200]
    assert request_body(deployments_route.calls.last.request)["sort"] == "NAME_ASC"
    flow_filter = request_body(flows_route.calls.last.request)["flows"]
    assert flow_filter == {"id": {"any_": [LOAD_FLOW_ID, REPORT_FLOW_ID]}}


async def test_list_deployments_skips_the_flow_lookup_when_there_are_none(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post("/deployments/filter").respond(200, json=[])
    flows_route = cloud_api.post("/flows/filter").respond(200, json=[])

    result = await mcp_client.call_tool("list_deployments", {})

    assert result.structured_content == {"deployments": []}
    assert not flows_route.called
