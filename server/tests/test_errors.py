"""Tests for how tools report Cloud errors, through one representative tool.

Every tool reads responses through the same helper, so `get_run` stands in for
all of them. The run tools also pass on `Retry-After`.
"""

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from typing import Any

import httpx
import pytest
import respx
from fastmcp import Client
from support import FLOW_RUN_ID, RUN_PATH, error_text

ARGUMENTS = {"flow_run_id": FLOW_RUN_ID}
NO_BUCKET = "Workspace object storage bucket has not been provisioned."


@pytest.mark.parametrize(
    ("response", "expected", "unexpected"),
    [
        (
            httpx.Response(404, json={"detail": "Flow run not found."}),
            ["HTTP 404", f"GET {RUN_PATH}", "Flow run not found."],
            ["Retry in"],
        ),
        (
            httpx.Response(422, json={"detail": [{"loc": ["path"], "msg": "bad id"}]}),
            ["HTTP 422", "bad id"],
            [],
        ),
        (
            httpx.Response(409, json={"detail": "The flow is being updated."}),
            ["HTTP 409", "The flow is being updated."],
            ["bucket", "Retry in"],
        ),
        (
            httpx.Response(500, text="database unavailable"),
            ["HTTP 500", "database unavailable"],
            [],
        ),
        (
            httpx.Response(409, headers={"Retry-After": "5"}, json={"detail": "Busy."}),
            ["HTTP 409", "Busy.", "Retry in 5 seconds."],
            [],
        ),
        (
            httpx.Response(
                503, headers={"Retry-After": "2"}, json={"detail": "Unavailable."}
            ),
            ["HTTP 503", "Unavailable.", "Retry in 2 seconds."],
            [],
        ),
        (
            httpx.Response(409, json={"detail": NO_BUCKET}),
            ["no object storage bucket", "configure"],
            [],
        ),
        (
            httpx.Response(200, text="<html>proxy login</html>"),
            ["not JSON", f"GET {RUN_PATH}"],
            [],
        ),
    ],
    ids=[
        "not-found",
        "validation",
        "conflict",
        "text-body",
        "retry-409",
        "retry-503",
        "no-bucket",
        "not-json",
    ],
)
async def test_an_error_response_is_reported_with_its_detail(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    response: httpx.Response,
    expected: list[str],
    unexpected: list[str],
):
    cloud_api.get(RUN_PATH).mock(return_value=response)

    result = await mcp_client.call_tool("get_run", ARGUMENTS, raise_on_error=False)

    message = error_text(result)
    for text in expected:
        assert text in message
    for text in unexpected:
        assert text not in message


async def test_a_retry_after_date_is_turned_into_seconds(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    retry_at = datetime.now(timezone.utc) + timedelta(seconds=60)
    cloud_api.get(RUN_PATH).respond(
        503,
        headers={"Retry-After": format_datetime(retry_at, usegmt=True)},
        json={"detail": "Busy."},
    )

    result = await mcp_client.call_tool("get_run", ARGUMENTS, raise_on_error=False)

    message = error_text(result)
    seconds = float(message.split("Retry in ")[1].split(" seconds")[0])
    assert 50 < seconds <= 60


async def test_an_unreachable_cloud_is_reported_with_its_url(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(RUN_PATH).mock(side_effect=httpx.ConnectError("connection refused"))

    result = await mcp_client.call_tool("get_run", ARGUMENTS, raise_on_error=False)

    message = error_text(result)
    assert "Could not reach Prefect Cloud at https://api.prefect.cloud/" in message
    assert "connection refused" in message
