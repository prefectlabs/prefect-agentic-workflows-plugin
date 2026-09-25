"""Tests for the schedule tools."""

import json
from typing import Any

import pytest
import respx
from fastmcp import Client

from support import error_text

FLOW_ID = "33333333-3333-3333-3333-333333333333"
SCHEDULE_ID = "44444444-4444-4444-4444-444444444444"
SCHEDULES_PATH = f"/flows/{FLOW_ID}/execution-plan/schedules"
SCHEDULE_PATH = f"{SCHEDULES_PATH}/{SCHEDULE_ID}"


def schedule_response(**overrides: Any) -> dict[str, Any]:
    """Return a schedule body in the shape Cloud returns."""
    body: dict[str, Any] = {
        "id": SCHEDULE_ID,
        "flow_id": FLOW_ID,
        "name": "weekday-mornings",
        "active": True,
        "schedule": {"type": "cron", "cron": "0 9 * * 1-5", "timezone": "UTC"},
        "parameters": {"channel": "#ops"},
        "created": "2026-09-25T12:00:00Z",
        "updated": "2026-09-25T12:00:00Z",
        "next_scheduled_time": "2026-09-28T09:00:00Z",
        "last_created_flow_run_id": "55555555-5555-5555-5555-555555555555",
        "last_error": "Parameter 'channel' is required.",
        "execution_plan_version_selection": "active_at_scheduled_time",
        "parameters_revalidated_at_scheduled_time": True,
    }
    body.update(overrides)
    return body


async def test_create_schedule_sends_a_cron_schedule(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post(SCHEDULES_PATH).respond(201, json=schedule_response())

    result = await mcp_client.call_tool(
        "create_schedule",
        {
            "flow_id": FLOW_ID,
            "name": "weekday-mornings",
            "schedule": {"type": "cron", "cron": "0 9 * * 1-5", "timezone": "UTC"},
            "parameters": {"channel": "#ops"},
        },
    )

    assert json.loads(route.calls.last.request.content) == {
        "name": "weekday-mornings",
        "active": True,
        "schedule": {"type": "cron", "cron": "0 9 * * 1-5", "timezone": "UTC"},
        "parameters": {"channel": "#ops"},
    }
    body = result.structured_content
    assert body is not None
    assert body["next_scheduled_time"] == "2026-09-28T09:00:00Z"
    assert body["last_error"] == "Parameter 'channel' is required."
    assert (
        body["last_created_flow_run_id"] == "55555555-5555-5555-5555-555555555555"
    )


@pytest.mark.parametrize(
    "schedule",
    [
        {"type": "interval", "interval": 3600},
        {
            "type": "interval",
            "interval": 900.5,
            "anchor_date": "2026-09-25T00:00:00Z",
            "timezone": "Europe/Berlin",
        },
        {"type": "rrule", "rrule": "FREQ=WEEKLY;BYDAY=MO", "timezone": "UTC"},
        {"type": "cron", "cron": "0 0 1 * 1", "day_or": False},
    ],
)
async def test_create_schedule_sends_each_schedule_type(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, schedule: dict[str, Any]
):
    route = cloud_api.post(SCHEDULES_PATH).respond(
        201, json=schedule_response(schedule=schedule, parameters={})
    )

    result = await mcp_client.call_tool(
        "create_schedule",
        {"flow_id": FLOW_ID, "name": "nightly", "schedule": schedule, "active": False},
    )

    assert json.loads(route.calls.last.request.content) == {
        "name": "nightly",
        "active": False,
        "schedule": schedule,
        "parameters": {},
    }
    assert result.structured_content is not None
    assert result.structured_content["schedule"] == schedule


async def test_create_schedule_rejects_a_schedule_without_its_type_field(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post(SCHEDULES_PATH).respond(201, json=schedule_response())

    result = await mcp_client.call_tool(
        "create_schedule",
        {
            "flow_id": FLOW_ID,
            "name": "hourly",
            "schedule": {"type": "rrule", "cron": "0 * * * *"},
        },
        raise_on_error=False,
    )

    assert "rrule" in error_text(result)
    assert not route.called


async def test_create_schedule_reports_a_flow_without_an_active_plan(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(SCHEDULES_PATH).respond(
        409, json={"detail": "Flow has no active execution plan version."}
    )

    result = await mcp_client.call_tool(
        "create_schedule",
        {
            "flow_id": FLOW_ID,
            "name": "hourly",
            "schedule": {"type": "interval", "interval": 3600},
        },
        raise_on_error=False,
    )

    message = error_text(result)
    assert "409" in message
    assert "no active execution plan version" in message


async def test_list_schedules_returns_the_flow_schedules(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    schedules = [
        schedule_response(),
        schedule_response(
            id="66666666-6666-6666-6666-666666666666",
            name="hourly",
            active=False,
            schedule={"type": "interval", "interval": 3600.0},
        ),
    ]
    cloud_api.get(SCHEDULES_PATH).respond(200, json={"schedules": schedules})

    result = await mcp_client.call_tool("list_schedules", {"flow_id": FLOW_ID})

    assert result.structured_content == {"schedules": schedules}


async def test_get_schedule_returns_one_schedule(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(SCHEDULE_PATH).respond(200, json=schedule_response())

    result = await mcp_client.call_tool(
        "get_schedule", {"flow_id": FLOW_ID, "schedule_id": SCHEDULE_ID}
    )

    assert result.structured_content == schedule_response()


async def test_get_schedule_reports_a_missing_schedule(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(SCHEDULE_PATH).respond(
        404, json={"detail": "Execution plan schedule not found."}
    )

    result = await mcp_client.call_tool(
        "get_schedule",
        {"flow_id": FLOW_ID, "schedule_id": SCHEDULE_ID},
        raise_on_error=False,
    )

    message = error_text(result)
    assert "404" in message
    assert "Execution plan schedule not found." in message


async def test_delete_schedule_deletes_the_schedule(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.delete(SCHEDULE_PATH).respond(204)

    result = await mcp_client.call_tool(
        "delete_schedule", {"flow_id": FLOW_ID, "schedule_id": SCHEDULE_ID}
    )

    assert route.called
    assert result.structured_content == {
        "deleted": True,
        "flow_id": FLOW_ID,
        "schedule_id": SCHEDULE_ID,
    }


@pytest.mark.parametrize(
    ("arguments", "expected_body"),
    [
        ({"active": False}, {"active": False}),
        ({"name": "renamed"}, {"name": "renamed"}),
        ({"parameters": {}}, {"parameters": {}}),
        (
            {"schedule": {"type": "rrule", "rrule": "FREQ=DAILY"}},
            {"schedule": {"type": "rrule", "rrule": "FREQ=DAILY"}},
        ),
        (
            {
                "name": "renamed",
                "active": True,
                "schedule": {"type": "interval", "interval": 60},
                "parameters": {"channel": "#alerts"},
            },
            {
                "name": "renamed",
                "active": True,
                "schedule": {"type": "interval", "interval": 60},
                "parameters": {"channel": "#alerts"},
            },
        ),
    ],
)
async def test_update_schedule_sends_only_the_given_fields(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    arguments: dict[str, Any],
    expected_body: dict[str, Any],
):
    route = cloud_api.patch(SCHEDULE_PATH).respond(200, json=schedule_response())

    result = await mcp_client.call_tool(
        "update_schedule",
        {"flow_id": FLOW_ID, "schedule_id": SCHEDULE_ID, **arguments},
    )

    assert json.loads(route.calls.last.request.content) == expected_body
    assert result.structured_content == schedule_response()


async def test_update_schedule_needs_at_least_one_field(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.patch(SCHEDULE_PATH).respond(200, json=schedule_response())

    result = await mcp_client.call_tool(
        "update_schedule",
        {"flow_id": FLOW_ID, "schedule_id": SCHEDULE_ID},
        raise_on_error=False,
    )

    assert "at least one" in error_text(result)
    assert not route.called


async def test_schedule_tools_have_the_right_hints(mcp_client: Client[Any]):
    tools = {tool.name: tool for tool in await mcp_client.list_tools()}
    expected = {
        "list_schedules": (True, False),
        "get_schedule": (True, False),
        "create_schedule": (False, False),
        "update_schedule": (False, False),
        "delete_schedule": (False, True),
    }

    for name, (read_only, destructive) in expected.items():
        annotations = tools[name].annotations
        assert annotations is not None, name
        assert annotations.readOnlyHint is read_only, name
        assert annotations.destructiveHint is destructive, name
        assert "alpha" in (tools[name].description or ""), name
