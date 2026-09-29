"""Tests for the schedule shapes `create_schedule` and `update_schedule` send."""

from typing import Any

import pytest
import respx
from fastmcp import Client
from support import (
    FLOW_ID,
    SCHEDULE_ID,
    SCHEDULES_PATH,
    request_body,
    schedule_response,
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
    route = cloud_api.post(SCHEDULES_PATH).respond(201, json=schedule_response())

    await mcp_client.call_tool(
        "create_schedule",
        {"flow_id": FLOW_ID, "name": "nightly", "schedule": schedule, "active": False},
    )

    assert request_body(route.calls.last.request) == {
        "name": "nightly",
        "active": False,
        "schedule": schedule,
        "parameters": {},
    }


ALL_FIELDS = {
    "name": "renamed",
    "active": True,
    "schedule": {"type": "interval", "interval": 60},
    "parameters": {"channel": "#alerts"},
}


@pytest.mark.parametrize(
    "changes",
    [
        {"active": False},
        {"name": "renamed"},
        {"parameters": {}},
        {"schedule": {"type": "rrule", "rrule": "FREQ=DAILY"}},
        ALL_FIELDS,
    ],
)
async def test_update_schedule_sends_only_the_given_fields(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, changes: dict[str, Any]
):
    route = cloud_api.patch(f"{SCHEDULES_PATH}/{SCHEDULE_ID}").respond(
        200, json=schedule_response()
    )

    result = await mcp_client.call_tool(
        "update_schedule", {"flow_id": FLOW_ID, "schedule_id": SCHEDULE_ID, **changes}
    )

    assert request_body(route.calls.last.request) == changes
    assert result.structured_content == schedule_response()
