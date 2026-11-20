"""Tests for `get_run_checks`, which reads how Prefect Cloud checked the
expectations on the outputs of a run's agent nodes."""

from datetime import UTC, datetime
from typing import Any

import respx
from fastmcp import Client
from support import (
    ACTIVATION_ID,
    FLOW_ID,
    FLOW_RUN_ID,
    VERSION_ID,
    error_text,
    flow_run_response,
    request_body,
)

from prefect_agentic_workflows_mcp.tools.run_checks import (
    CHECKS_PATH,
    MAX_CHECKS_READ,
    PAGE_LIMIT,
)

FLOW_RUN_PATH = f"/flow_runs/{FLOW_RUN_ID}"
ATTEMPT_ID = "01a19e47-660c-7358-a65c-883f5130bbde"
OTHER_ACTIVATION_ID = "01a19e46-fe91-7233-9df3-17a122456abf"


def check(**overrides: Any) -> dict[str, Any]:
    """Return one check of `POST /agent-sessions/checks/filter` as Cloud returns it."""
    return {
        "workflow_id": FLOW_ID,
        "workflow_version_id": VERSION_ID,
        "workflow_run_id": FLOW_RUN_ID,
        "authored_node_id": "draft_reply",
        "activation_id": ACTIVATION_ID,
        "attempt_id": ATTEMPT_ID,
        "attempt_number": 1,
        "output": "reply_ready",
        "expectation_key": "cites_fetched_docs",
        "expectation_text": "The reply links only to docs pages fetched here.",
        "attempt_started_at": "2026-11-02T22:00:05Z",
        "ended_at": "2026-11-02T22:01:10Z",
        "verdict": "satisfied",
        "check_error": None,
        "reason": None,
        "content": "none",
        **overrides,
    }


def checks_page(
    checks: list[dict[str, Any]], next_cursor: str | None = None
) -> dict[str, Any]:
    return {"context": {}, "checks": checks, "next_cursor": next_cursor}


async def get_run_checks(client: Client[Any], **arguments: Any) -> dict[str, Any]:
    result = await client.call_tool(
        "get_run_checks", {"flow_run_id": FLOW_RUN_ID, **arguments}
    )
    assert result.structured_content is not None
    return result.structured_content


async def test_get_run_checks_returns_each_check_with_its_verdict_and_reason(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(FLOW_RUN_PATH).respond(200, json=flow_run_response())
    cloud_api.post(CHECKS_PATH).respond(
        200,
        json=checks_page(
            [
                check(
                    verdict="violated",
                    reason="The reply links to a page that no tool call fetched.",
                    content="available",
                ),
                check(expectation_key="answers_the_question"),
            ]
        ),
    )

    found = await get_run_checks(mcp_client)

    assert found == {
        "checks": [
            {
                "node": "draft_reply",
                "activation_id": ACTIVATION_ID,
                "attempt_id": ATTEMPT_ID,
                "attempt_number": 1,
                "output": "reply_ready",
                "expectation_key": "cites_fetched_docs",
                "expectation_text": "The reply links only to docs pages fetched here.",
                "verdict": "violated",
                "reason": "The reply links to a page that no tool call fetched.",
                "check_error": None,
                "content": "available",
                "ended_at": "2026-11-02T22:01:10Z",
            },
            {
                "node": "draft_reply",
                "activation_id": ACTIVATION_ID,
                "attempt_id": ATTEMPT_ID,
                "attempt_number": 1,
                "output": "reply_ready",
                "expectation_key": "answers_the_question",
                "expectation_text": "The reply links only to docs pages fetched here.",
                "verdict": "satisfied",
                "reason": None,
                "check_error": None,
                "content": "none",
                "ended_at": "2026-11-02T22:01:10Z",
            },
        ],
        "checks_unread": False,
    }


async def test_get_run_checks_asks_for_the_run_from_its_creation_until_now(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(FLOW_RUN_PATH).respond(
        200, json=flow_run_response(created="2026-11-02T22:00:00Z")
    )
    route = cloud_api.post(CHECKS_PATH).respond(200, json=checks_page([]))

    await get_run_checks(mcp_client)

    body = request_body(route.calls.last.request)
    assert body["filters"] == {"workflow_run_ids": [FLOW_RUN_ID]}
    assert body["limit"] == PAGE_LIMIT
    assert body["range"]["start"] == "2026-11-02T22:00:00Z"
    assert datetime.fromisoformat(body["range"]["end"]) > datetime.now(UTC)


async def test_get_run_checks_returns_a_check_error(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    error = {"cause": "judge_failed", "sub_cause": None, "detail": "No verdict."}
    cloud_api.get(FLOW_RUN_PATH).respond(200, json=flow_run_response())
    cloud_api.post(CHECKS_PATH).respond(
        200, json=checks_page([check(verdict=None, check_error=error)])
    )

    found = await get_run_checks(mcp_client)

    assert found["checks"][0]["verdict"] is None
    assert found["checks"][0]["check_error"] == error


async def test_get_run_checks_keeps_only_the_checks_of_one_activation(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(FLOW_RUN_PATH).respond(200, json=flow_run_response())
    cloud_api.post(CHECKS_PATH).respond(
        200,
        json=checks_page(
            [
                check(),
                check(authored_node_id="review", activation_id=OTHER_ACTIVATION_ID),
            ]
        ),
    )

    found = await get_run_checks(mcp_client, activation_id=OTHER_ACTIVATION_ID)

    assert [entry["node"] for entry in found["checks"]] == ["review"]


async def test_get_run_checks_reads_every_page(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(FLOW_RUN_PATH).respond(200, json=flow_run_response())
    route = cloud_api.post(CHECKS_PATH)
    route.side_effect = [
        respx.MockResponse(
            200, json=checks_page([check(attempt_number=1)], next_cursor="next")
        ),
        respx.MockResponse(200, json=checks_page([check(attempt_number=2)])),
    ]

    found = await get_run_checks(mcp_client)

    assert [entry["attempt_number"] for entry in found["checks"]] == [1, 2]
    assert found["checks_unread"] is False
    assert request_body(route.calls.last.request)["cursor"] == "next"


async def test_get_run_checks_stops_reading_a_run_with_too_many_checks(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(FLOW_RUN_PATH).respond(200, json=flow_run_response())
    route = cloud_api.post(CHECKS_PATH).respond(
        200, json=checks_page([check()] * PAGE_LIMIT, next_cursor="next")
    )

    found = await get_run_checks(mcp_client)

    assert len(found["checks"]) == MAX_CHECKS_READ
    assert found["checks_unread"] is True
    assert route.call_count == MAX_CHECKS_READ // PAGE_LIMIT


async def test_get_run_checks_finds_an_activation_after_many_other_checks(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(FLOW_RUN_PATH).respond(200, json=flow_run_response())
    other_pages = MAX_CHECKS_READ // PAGE_LIMIT
    route = cloud_api.post(CHECKS_PATH)
    route.side_effect = [
        *(
            respx.MockResponse(
                200, json=checks_page([check()] * PAGE_LIMIT, next_cursor="next")
            )
            for _ in range(other_pages)
        ),
        respx.MockResponse(
            200,
            json=checks_page(
                [check(authored_node_id="review", activation_id=OTHER_ACTIVATION_ID)]
            ),
        ),
    ]

    found = await get_run_checks(mcp_client, activation_id=OTHER_ACTIVATION_ID)

    assert [entry["node"] for entry in found["checks"]] == ["review"]
    assert found["checks_unread"] is False


async def test_get_run_checks_reports_a_run_that_does_not_exist(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(FLOW_RUN_PATH).respond(404, json={"detail": "Flow run not found"})

    result = await mcp_client.call_tool(
        "get_run_checks", {"flow_run_id": FLOW_RUN_ID}, raise_on_error=False
    )

    assert "Flow run not found" in error_text(result)
