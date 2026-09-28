"""Tests for the `list_deployments` tool."""

import json
from typing import Any

import respx
from fastmcp import Client
from support import error_text

LOAD_FLOW_ID = "cccccccc-0000-0000-0000-000000000001"
REPORT_FLOW_ID = "cccccccc-0000-0000-0000-000000000002"
NIGHTLY_LOAD_ID = "dddddddd-0000-0000-0000-000000000001"
WEEKLY_REPORT_ID = "dddddddd-0000-0000-0000-000000000002"


def deployment(
    deployment_id: str, name: str, flow_id: str, description: str | None = None
) -> dict[str, Any]:
    """Return a deployment in the shape `/deployments/filter` returns."""
    return {
        "id": deployment_id,
        "created": "2026-09-01T00:00:00Z",
        "updated": "2026-09-01T00:00:00Z",
        "name": name,
        "flow_id": flow_id,
        "description": description,
        "parameters": {},
        "tags": [],
        "paused": False,
    }


def flow(flow_id: str, name: str) -> dict[str, Any]:
    """Return a flow in the shape `/flows/filter` returns."""
    return {"id": flow_id, "name": name, "tags": []}


async def test_list_deployments_returns_ids_names_and_flow_names(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    deployments_route = cloud_api.post("/deployments/filter").respond(
        200,
        json=[
            deployment(NIGHTLY_LOAD_ID, "nightly", LOAD_FLOW_ID, "Loads orders"),
            deployment(WEEKLY_REPORT_ID, "weekly", REPORT_FLOW_ID),
        ],
    )
    flows_route = cloud_api.post("/flows/filter").respond(
        200,
        json=[flow(LOAD_FLOW_ID, "load-orders"), flow(REPORT_FLOW_ID, "report")],
    )

    result = await mcp_client.call_tool("list_deployments", {})

    assert result.structured_content == {
        "deployments": [
            {
                "id": NIGHTLY_LOAD_ID,
                "name": "nightly",
                "flow_name": "load-orders",
                "description": "Loads orders",
            },
            {
                "id": WEEKLY_REPORT_ID,
                "name": "weekly",
                "flow_name": "report",
                "description": None,
            },
        ]
    }
    assert json.loads(deployments_route.calls.last.request.content)["sort"] == (
        "NAME_ASC"
    )
    flow_filter = json.loads(flows_route.calls.last.request.content)["flows"]
    assert sorted(flow_filter["id"]["any_"]) == [LOAD_FLOW_ID, REPORT_FLOW_ID]


async def test_list_deployments_returns_an_empty_list_when_there_are_none(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post("/deployments/filter").respond(200, json=[])
    flows_route = cloud_api.post("/flows/filter").respond(200, json=[])

    result = await mcp_client.call_tool("list_deployments", {})

    assert result.structured_content == {"deployments": []}
    assert not flows_route.called


async def test_list_deployments_reads_every_page(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    first_page = [
        deployment(
            f"eeeeeeee-0000-0000-0000-{index:012d}", f"d-{index:03d}", LOAD_FLOW_ID
        )
        for index in range(200)
    ]
    route = cloud_api.post("/deployments/filter").mock(
        side_effect=[
            respx.MockResponse(200, json=first_page),
            respx.MockResponse(
                200, json=[deployment(WEEKLY_REPORT_ID, "weekly", REPORT_FLOW_ID)]
            ),
        ]
    )
    cloud_api.post("/flows/filter").respond(
        200,
        json=[flow(LOAD_FLOW_ID, "load-orders"), flow(REPORT_FLOW_ID, "report")],
    )

    result = await mcp_client.call_tool("list_deployments", {})

    assert result.structured_content is not None
    deployments = result.structured_content["deployments"]
    assert len(deployments) == 201
    assert deployments[-1]["flow_name"] == "report"
    offsets = [json.loads(call.request.content)["offset"] for call in route.calls]
    assert offsets == [0, 200]


async def test_list_deployments_reports_a_failed_request(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post("/deployments/filter").respond(
        403, json={"detail": "Missing scope see_deployments"}
    )

    result = await mcp_client.call_tool("list_deployments", {}, raise_on_error=False)

    message = error_text(result)
    assert "403" in message
    assert "Missing scope see_deployments" in message


async def test_list_deployments_is_read_only(mcp_client: Client[Any]):
    tools = {tool.name: tool for tool in await mcp_client.list_tools()}

    annotations = tools["list_deployments"].annotations
    assert annotations is not None
    assert annotations.readOnlyHint is True
    assert annotations.destructiveHint is False
