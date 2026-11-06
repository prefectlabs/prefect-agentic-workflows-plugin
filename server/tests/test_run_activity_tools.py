"""Tests for `get_run_activity`, which reads a run's agent executions and the
model calls and tool calls of each execution's session, and for
`get_activity_record`, which reads one of those calls in full."""

from typing import Any

import pytest
import respx
from fastmcp import Client
from mcp.types import TextContent
from support import (
    ACTIVATION_ID,
    EXECUTIONS_PATH,
    FLOW_RUN_ID,
    RECORD_ID,
    SESSION_ID,
    activity_page,
    activity_record,
    agent_execution,
    error_text,
    executions_page,
    model_call_item,
    model_output,
    tool_call_item,
)

from prefect_agentic_workflows_mcp.tools.run_activity import (
    FAILED_PREVIEW_CHARACTERS,
    MAX_ITEMS_READ,
    MAX_RESULT_CHARACTERS,
    PREVIEW_CHARACTERS,
    TRUNCATED_MARKER,
)

ACTIVITY_PATH = f"/agent-sessions/{SESSION_ID}/activity"
RECORD_PATH = f"/agent-sessions/{SESSION_ID}/records/{RECORD_ID}/revisions/{RECORD_ID}"
OTHER_SESSION_ID = "01a19e46-ff81-71df-9a5d-fce8daf19c9d"
OTHER_ACTIVATION_ID = "01a19e46-fe91-7233-9df3-17a122456abf"
OTHER_ACTIVITY_PATH = f"/agent-sessions/{OTHER_SESSION_ID}/activity"
STARTED_AT = "2026-11-02T19:00:51.610300+00:00"
ENDED_AT = "2026-11-02T19:00:51.854886+00:00"


async def get_run_activity(client: Client[Any], **arguments: Any) -> dict[str, Any]:
    result = await client.call_tool(
        "get_run_activity", {"flow_run_id": FLOW_RUN_ID, **arguments}
    )
    assert result.structured_content is not None
    return result.structured_content


def record_id(number: int) -> str:
    return f"01a19e47-d1a4-7c06-9a98-{number:012d}"


async def test_get_run_activity_returns_each_attempt_with_its_calls(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    activity = cloud_api.get(ACTIVITY_PATH).respond(
        200,
        json=activity_page(
            [
                model_call_item(
                    model_output(
                        {"type": "thinking", "content": ""},
                        {"type": "text", "content": "I'll search first."},
                        {
                            "type": "tool_call",
                            "id": "toolu_1",
                            "name": "web_search_exa",
                            "arguments": '{"query": "refund policy"}',
                        },
                    )
                ),
                tool_call_item(
                    "web_search_exa",
                    failed=True,
                    mcp_server="exa",
                    result="You've hit Exa's free MCP rate limit.",
                ),
                tool_call_item("lookup", in_flight=True, result=None),
            ]
        ),
    )

    result = await get_run_activity(mcp_client)

    assert dict(activity.calls.last.request.url.params) == {
        "operation": "chat,execute_tool",
        "limit": "100",
    }
    call = {
        "record_id": RECORD_ID,
        "started_at": STARTED_AT,
        "ended_at": ENDED_AT,
        "failure_type": None,
        "retryable": None,
        "content": "available",
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
                "activity": [
                    {
                        **call,
                        "operation": "chat",
                        "status": "ok",
                        "model": "claude-sonnet-4-5",
                        "output": (
                            "I'll search first.\n"
                            'web_search_exa({"query": "refund policy"})'
                        ),
                    },
                    {
                        **call,
                        "operation": "execute_tool",
                        "status": "error",
                        "failure_type": "tool_error",
                        "retryable": True,
                        "tool": "web_search_exa",
                        "mcp_server": "exa",
                        "arguments": '{"query": "refund policy"}',
                        "result": "You've hit Exa's free MCP rate limit.",
                    },
                    {
                        **call,
                        "operation": "execute_tool",
                        "status": "in_flight",
                        "ended_at": None,
                        "tool": "lookup",
                        "mcp_server": None,
                        "arguments": '{"query": "refund policy"}',
                        "result": None,
                    },
                ],
                "activity_left_out": 0,
                "activity_unread": False,
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


async def test_get_run_activity_cuts_content_and_keeps_more_of_a_failed_call(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    long_text = "x" * (FAILED_PREVIEW_CHARACTERS + 1)
    fits = "y" * PREVIEW_CHARACTERS
    cloud_api.get(ACTIVITY_PATH).respond(
        200,
        json=activity_page(
            [
                tool_call_item(arguments=fits, result=long_text),
                tool_call_item(failed=True, result=long_text),
                model_call_item(model_output({"type": "text", "content": long_text})),
            ]
        ),
    )

    result = await get_run_activity(mcp_client)

    ok_call, failed_call, model_call = result["attempts"][0]["activity"]
    assert ok_call["arguments"] == fits
    assert ok_call["result"] == "x" * PREVIEW_CHARACTERS + TRUNCATED_MARKER
    assert failed_call["result"] == "x" * FAILED_PREVIEW_CHARACTERS + TRUNCATED_MARKER
    assert model_call["output"] == "x" * PREVIEW_CHARACTERS + TRUNCATED_MARKER


async def test_get_run_activity_returns_output_that_the_collector_cut_as_text(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    output = model_output({"type": "text", "content": "Long"})
    cut_output = output[:-10] + TRUNCATED_MARKER
    cloud_api.get(ACTIVITY_PATH).respond(
        200, json=activity_page([model_call_item(cut_output)])
    )

    result = await get_run_activity(mcp_client)

    [model_call] = result["attempts"][0]["activity"]
    assert model_call["output"] == cut_output


async def test_get_run_activity_returns_tool_call_arguments_of_a_model_call_as_json(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    output = model_output(
        {
            "type": "tool_call",
            "id": "toolu_1",
            "name": "search",
            "arguments": {"query": "refund policy"},
        }
    )
    cloud_api.get(ACTIVITY_PATH).respond(
        200, json=activity_page([model_call_item(output)])
    )

    result = await get_run_activity(mcp_client)

    [model_call] = result["attempts"][0]["activity"]
    assert model_call["output"] == 'search({"query": "refund policy"})'


async def test_get_run_activity_leaves_out_older_successful_calls_to_fit(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    long_result = "x" * PREVIEW_CHARACTERS
    items = [
        tool_call_item("send_reply", failed=True, record_id=record_id(0)),
        *(
            tool_call_item(result=long_result, record_id=record_id(number))
            for number in range(1, 100)
        ),
    ]
    cloud_api.get(ACTIVITY_PATH).mock(
        side_effect=[
            respx.MockResponse(200, json=activity_page(items, next_cursor="2")),
            respx.MockResponse(
                200,
                json=activity_page(
                    [
                        tool_call_item(result=long_result, record_id=record_id(n))
                        for n in range(100, 200)
                    ]
                ),
            ),
        ]
    )

    result = await mcp_client.call_tool(
        "get_run_activity", {"flow_run_id": FLOW_RUN_ID}
    )

    [text] = result.content
    assert isinstance(text, TextContent)
    assert len(text.text) <= MAX_RESULT_CHARACTERS
    assert result.structured_content is not None
    [attempt] = result.structured_content["attempts"]
    listed = [call["record_id"] for call in attempt["activity"]]
    assert listed[0] == record_id(0)
    assert listed[-1] == record_id(199)
    assert listed[1:] == [record_id(n) for n in range(200 - len(listed) + 1, 200)]
    assert attempt["activity_left_out"] == 200 - len(listed) > 0


async def test_get_run_activity_reads_at_most_2000_calls_of_an_attempt(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(EXECUTIONS_PATH).respond(
        200, json=executions_page([agent_execution()])
    )
    activity = cloud_api.get(ACTIVITY_PATH).respond(
        200, json=activity_page([tool_call_item()] * 100, next_cursor="more")
    )

    result = await get_run_activity(mcp_client)

    assert activity.call_count == MAX_ITEMS_READ // 100
    [attempt] = result["attempts"]
    assert attempt["activity_unread"] is True
    assert len(attempt["activity"]) + attempt["activity_left_out"] == MAX_ITEMS_READ


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
                model_call_item(content="withheld", failed=True),
                tool_call_item(
                    failed=True, content="withheld", arguments=None, result=None
                ),
            ]
        ),
    )

    result = await get_run_activity(mcp_client)

    model_call, tool_call = result["attempts"][0]["activity"]
    assert (model_call["status"], model_call["content"]) == ("error", "withheld")
    assert model_call["output"] is None
    assert (tool_call["status"], tool_call["content"]) == ("error", "withheld")
    assert (tool_call["arguments"], tool_call["result"]) == (None, None)


async def test_get_run_activity_reads_no_calls_for_an_attempt_without_a_session(
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
    assert attempt["activity"] is None
    assert attempt["activity_left_out"] == 0


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


@pytest.mark.parametrize(
    ("item", "attributes", "expected"),
    [
        pytest.param(
            model_call_item(),
            {
                "gen_ai.system_instructions": "You execute one AgentNode.",
                "gen_ai.input.messages": "x" * 10_000,
            },
            {
                "operation": "chat",
                "system_instructions": "You execute one AgentNode.",
                "input": "x" * 10_000,
                "output": model_output({"type": "text", "content": "Searching."}),
            },
            id="model-call",
        ),
        pytest.param(
            tool_call_item(),
            {},
            {
                "operation": "execute_tool",
                "arguments": '{"query": "refund policy"}',
                "result": "No results.",
            },
            id="tool-call",
        ),
    ],
)
async def test_get_activity_record_returns_the_full_content_of_one_call(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    item: dict[str, Any],
    attributes: dict[str, str],
    expected: dict[str, Any],
):
    cloud_api.get(RECORD_PATH).respond(200, json=activity_record(item, attributes))

    result = await mcp_client.call_tool(
        "get_activity_record", {"attempt_id": SESSION_ID, "record_id": RECORD_ID}
    )

    assert result.structured_content == {
        "record_id": RECORD_ID,
        "content": "available",
        **expected,
    }
