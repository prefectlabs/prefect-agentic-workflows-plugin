"""Tests for `get_run_activity`, which reads a run's agent executions and the
tool calls of each execution's session."""

from typing import Any

import respx
from fastmcp import Client
from support import (
    ACTIVATION_ID,
    EXECUTIONS_PATH,
    FLOW_RUN_ID,
    SESSION_ID,
    activity_page,
    agent_execution,
    error_text,
    executions_page,
    tool_call_item,
)

from prefect_agentic_workflows_mcp.tools.run_activity import (
    MAX_CONTENT_CHARACTERS,
    MAX_TOOL_CALLS_SEARCHED,
    TRUNCATED_MARKER,
)

ACTIVITY_PATH = f"/agent-sessions/{SESSION_ID}/activity"
OTHER_SESSION_ID = "01a19e46-ff81-71df-9a5d-fce8daf19c9d"
OTHER_ACTIVATION_ID = "01a19e46-fe91-7233-9df3-17a122456abf"
OTHER_ACTIVITY_PATH = f"/agent-sessions/{OTHER_SESSION_ID}/activity"


async def get_run_activity(client: Client[Any], **arguments: Any) -> dict[str, Any]:
    result = await client.call_tool(
        "get_run_activity", {"flow_run_id": FLOW_RUN_ID, **arguments}
    )
    assert result.structured_content is not None
    return result.structured_content


async def test_get_run_activity_returns_each_attempt_with_its_tool_calls(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    activity = cloud_api.get(ACTIVITY_PATH).respond(
        200,
        json=activity_page(
            [
                tool_call_item("search"),
                tool_call_item(
                    "web_search_exa",
                    failed=True,
                    result="You've hit Exa's free MCP rate limit.",
                ),
                tool_call_item("lookup", in_flight=True, result=None),
            ]
        ),
    )

    result = await get_run_activity(mcp_client)

    assert dict(activity.calls.last.request.url.params) == {
        "operation": "execute_tool",
        "limit": "100",
    }
    assert result == {
        "history": {"coverage": "complete", "issues": []},
        "attempts": [
            {
                "attempt_id": SESSION_ID,
                "node_id": "draft",
                "activation_id": ACTIVATION_ID,
                "task_run_id": ACTIVATION_ID,
                "attempt_ordinal": 1,
                "status": "failed",
                "started_at": "2026-11-02T22:00:41.740717Z",
                "ended_at": "2026-11-02T22:00:55.957805Z",
                "tool_calls": [
                    {
                        "tool": "search",
                        "mcp_server": None,
                        "started_at": "2026-11-02T19:00:51.610300+00:00",
                        "ended_at": "2026-11-02T19:00:51.854886+00:00",
                        "status": "ok",
                        "failure_type": None,
                        "retryable": None,
                        "content": "available",
                        "arguments": '{"query": "refund policy"}',
                        "result": "No results.",
                    },
                    {
                        "tool": "web_search_exa",
                        "mcp_server": None,
                        "started_at": "2026-11-02T19:00:51.610300+00:00",
                        "ended_at": "2026-11-02T19:00:51.854886+00:00",
                        "status": "error",
                        "failure_type": "tool_error",
                        "retryable": True,
                        "content": "available",
                        "arguments": '{"query": "refund policy"}',
                        "result": "You've hit Exa's free MCP rate limit.",
                    },
                    {
                        "tool": "lookup",
                        "mcp_server": None,
                        "started_at": "2026-11-02T19:00:51.610300+00:00",
                        "ended_at": None,
                        "status": "in_flight",
                        "failure_type": None,
                        "retryable": None,
                        "content": "available",
                        "arguments": '{"query": "refund policy"}',
                        "result": None,
                    },
                ],
                "tool_calls_cut": False,
            }
        ],
    }


async def test_get_run_activity_reads_every_page_of_executions(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    executions = cloud_api.get(EXECUTIONS_PATH).mock(
        side_effect=[
            respx.MockResponse(
                200, json=executions_page([agent_execution()], next_cursor="page-2")
            ),
            respx.MockResponse(
                200,
                json=executions_page(
                    [agent_execution(OTHER_SESSION_ID, OTHER_ACTIVATION_ID)]
                ),
            ),
        ]
    )
    cloud_api.get(ACTIVITY_PATH).respond(200, json=activity_page([]))
    cloud_api.get(OTHER_ACTIVITY_PATH).respond(200, json=activity_page([]))

    result = await get_run_activity(mcp_client)

    assert [dict(call.request.url.params) for call in executions.calls] == [
        {"limit": "100"},
        {"limit": "100", "cursor": "page-2"},
    ]
    assert [attempt["attempt_id"] for attempt in result["attempts"]] == [
        SESSION_ID,
        OTHER_SESSION_ID,
    ]


async def test_get_run_activity_reads_only_the_attempts_of_one_activation(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200,
        json=executions_page(
            [
                agent_execution(),
                agent_execution(OTHER_SESSION_ID, OTHER_ACTIVATION_ID),
            ]
        ),
    )
    cloud_api.get(ACTIVITY_PATH).respond(200, json=activity_page([]))
    other_activity = cloud_api.get(OTHER_ACTIVITY_PATH)

    result = await get_run_activity(mcp_client, activation_id=ACTIVATION_ID)

    assert not other_activity.called
    assert [attempt["activation_id"] for attempt in result["attempts"]] == [
        ACTIVATION_ID
    ]


async def test_get_run_activity_returns_only_failed_calls_when_asked(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    cloud_api.get(ACTIVITY_PATH).respond(
        200,
        json=activity_page(
            [
                tool_call_item("search"),
                tool_call_item("send_reply", failed=True),
                tool_call_item("lookup", in_flight=True),
            ]
        ),
    )

    result = await get_run_activity(mcp_client, failed_only=True)

    [attempt] = result["attempts"]
    assert [call["tool"] for call in attempt["tool_calls"]] == ["send_reply"]


async def test_get_run_activity_stops_after_the_tool_call_cap(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    page = [tool_call_item("search")] * 100
    activity = cloud_api.get(ACTIVITY_PATH).mock(
        side_effect=[
            respx.MockResponse(200, json=activity_page(page, next_cursor="page-2")),
            respx.MockResponse(200, json=activity_page(page, next_cursor="page-3")),
            respx.MockResponse(200, json=activity_page(page)),
        ]
    )

    result = await get_run_activity(mcp_client)

    assert activity.call_count == 2
    [attempt] = result["attempts"]
    assert len(attempt["tool_calls"]) == 200
    assert attempt["tool_calls_cut"] is True


async def test_get_run_activity_cuts_long_arguments_and_results(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    long_result = "x" * (MAX_CONTENT_CHARACTERS + 1)
    fits = "y" * MAX_CONTENT_CHARACTERS
    cloud_api.get(ACTIVITY_PATH).respond(
        200,
        json=activity_page([tool_call_item(arguments=fits, result=long_result)]),
    )

    result = await get_run_activity(mcp_client)

    [call] = result["attempts"][0]["tool_calls"]
    assert call["arguments"] == fits
    assert call["result"] == "x" * MAX_CONTENT_CHARACTERS + TRUNCATED_MARKER


async def test_get_run_activity_passes_through_withheld_content(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    cloud_api.get(ACTIVITY_PATH).respond(
        200,
        json=activity_page(
            [
                tool_call_item(
                    failed=True, content="withheld", arguments=None, result=None
                )
            ]
        ),
    )

    result = await get_run_activity(mcp_client)

    [call] = result["attempts"][0]["tool_calls"]
    assert (call["status"], call["content"]) == ("error", "withheld")
    assert (call["arguments"], call["result"]) == (None, None)


async def test_get_run_activity_reads_no_tool_calls_for_an_attempt_without_a_session(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200,
        json=executions_page(
            [agent_execution(session_status="unknown", status="running", ended_at=None)]
        ),
    )
    activity = cloud_api.get(ACTIVITY_PATH)

    result = await get_run_activity(mcp_client)

    assert not activity.called
    [attempt] = result["attempts"]
    assert attempt["tool_calls"] is None


async def test_get_run_activity_passes_through_a_run_with_no_recorded_attempts(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    history = {
        "coverage": "unknown",
        "issues": [
            {
                "code": "no_attempts",
                "message": "No attempts are recorded for this flow run.",
            }
        ],
    }
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json={**executions_page([]), "history": history}
    )

    result = await get_run_activity(mcp_client)

    assert result == {"history": history, "attempts": []}


async def test_get_run_activity_says_when_to_retry_a_busy_read(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        503, headers={"Retry-After": "7"}, json={"detail": "Try again later."}
    )

    result = await mcp_client.call_tool(
        "get_run_activity", {"flow_run_id": FLOW_RUN_ID}, raise_on_error=False
    )

    assert error_text(result).endswith("Retry in 7 seconds.")


async def test_get_run_activity_returns_the_mcp_server_of_a_call(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    cloud_api.get(ACTIVITY_PATH).respond(
        200, json=activity_page([tool_call_item(mcp_server="pylon")])
    )

    result = await get_run_activity(mcp_client)

    [call] = result["attempts"][0]["tool_calls"]
    assert call["mcp_server"] == "pylon"


async def test_get_run_activity_is_partial_when_any_page_of_executions_is(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    not_final = {"code": "run_not_final", "message": "Some attempts are not final."}
    cloud_api.get(EXECUTIONS_PATH).mock(
        side_effect=[
            respx.MockResponse(
                200,
                json={
                    **executions_page([agent_execution()], next_cursor="page-2"),
                    "history": {"coverage": "partial", "issues": [not_final]},
                },
            ),
            respx.MockResponse(
                200,
                json=executions_page(
                    [agent_execution(OTHER_SESSION_ID, OTHER_ACTIVATION_ID)]
                ),
            ),
        ]
    )
    cloud_api.get(ACTIVITY_PATH).respond(200, json=activity_page([]))
    cloud_api.get(OTHER_ACTIVITY_PATH).respond(200, json=activity_page([]))

    result = await get_run_activity(mcp_client)

    assert result["history"] == {"coverage": "partial", "issues": [not_final]}


async def test_get_run_activity_finds_failed_calls_past_the_first_200(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    ok_page = [tool_call_item("search")] * 100
    cloud_api.get(ACTIVITY_PATH).mock(
        side_effect=[
            respx.MockResponse(200, json=activity_page(ok_page, next_cursor="2")),
            respx.MockResponse(200, json=activity_page(ok_page, next_cursor="3")),
            respx.MockResponse(
                200,
                json=activity_page(
                    [tool_call_item("search")] * 50
                    + [tool_call_item("send_reply", failed=True)]
                ),
            ),
        ]
    )

    result = await get_run_activity(mcp_client, failed_only=True)

    [attempt] = result["attempts"]
    assert [call["tool"] for call in attempt["tool_calls"]] == ["send_reply"]
    assert attempt["tool_calls_cut"] is False


async def test_get_run_activity_searches_at_most_2000_calls_for_failures(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    page = [tool_call_item("search")] * 99 + [tool_call_item("lookup", failed=True)]
    activity = cloud_api.get(ACTIVITY_PATH).respond(
        200, json=activity_page(page, next_cursor="more")
    )

    result = await get_run_activity(mcp_client, failed_only=True)

    assert activity.call_count == MAX_TOOL_CALLS_SEARCHED // 100
    [attempt] = result["attempts"]
    assert len(attempt["tool_calls"]) == MAX_TOOL_CALLS_SEARCHED // 100
    assert attempt["tool_calls_cut"] is True
