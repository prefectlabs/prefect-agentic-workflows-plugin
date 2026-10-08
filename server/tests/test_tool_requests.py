"""Checks the request each tool sends to Cloud and the result it returns.

Each row calls one tool, answers its request with a Cloud response, and checks
the method, path, query, and JSON body of the request and the tool's result.
Tools that send more than one request, or that reshape what Cloud returns,
have their own tests in the per-group files.
"""

from typing import Any

import pytest
import respx
from fastmcp import Client
from support import (
    ACTIVATION_ID,
    API_KEY,
    FLOW_ID,
    FLOW_RUN_ID,
    OLDER_VERSION_ID,
    PLAN,
    PLAN_PATH,
    RUN_PATH,
    SCHEDULE_ID,
    SCHEDULES_PATH,
    VERSIONS_PATH,
    WORKSPACE_API_PATH,
    active_state_response,
    flow_run_response,
    request_body,
    run_observation,
    schedule_response,
    schema_response,
    version_response,
)

SCHEDULE_PATH = f"{SCHEDULES_PATH}/{SCHEDULE_ID}"
CRON = {"type": "cron", "cron": "0 9 * * 1-5", "timezone": "UTC"}
INVALID = {
    "valid": False,
    "errors": [
        {
            "code": "unreachable_node",
            "phase": "semantic",
            "path": ["nodes", "summarize"],
            "message": "Node 'summarize' is not reachable from a start node.",
        }
    ],
}
OLDER_SCHEMA = {**schema_response("0.0"), "is_current": False}


def available_output(value: Any) -> dict[str, Any]:
    return {
        "available": True,
        "value": value,
        "output_status": "available",
        "reason": None,
        "detail": None,
        "retry_after_seconds": None,
        "final": True,
    }


ROWS = [
    pytest.param(
        "get_schema",
        {},
        ("GET", "/execution-plans/schema", {}, None),
        (200, schema_response()),
        schema_response(),
        id="get_schema-current",
    ),
    pytest.param(
        "get_schema",
        {"version": "0.0"},
        ("GET", "/execution-plans/schema", {"version": "0.0"}, None),
        (200, OLDER_SCHEMA),
        OLDER_SCHEMA,
        id="get_schema-version",
    ),
    pytest.param(
        "validate_plan",
        {"plan": PLAN},
        ("POST", "/execution-plans/validate", {}, {"plan": PLAN}),
        (200, INVALID),
        INVALID,
        id="validate_plan",
    ),
    pytest.param(
        "get_plan",
        {"flow_id": FLOW_ID},
        ("GET", PLAN_PATH, {}, None),
        (200, active_state_response(None)),
        active_state_response(None),
        id="get_plan-active",
    ),
    pytest.param(
        "get_plan",
        {"flow_id": FLOW_ID, "version_id": OLDER_VERSION_ID},
        ("GET", f"{VERSIONS_PATH}/{OLDER_VERSION_ID}", {}, None),
        (200, version_response(OLDER_VERSION_ID)),
        version_response(OLDER_VERSION_ID),
        id="get_plan-version",
    ),
    pytest.param(
        "activate_plan_version",
        {"flow_id": FLOW_ID, "version_id": OLDER_VERSION_ID},
        ("POST", f"{VERSIONS_PATH}/{OLDER_VERSION_ID}/activate", {}, None),
        (200, active_state_response(OLDER_VERSION_ID)),
        active_state_response(OLDER_VERSION_ID),
        id="activate_plan_version",
    ),
    pytest.param(
        "create_schedule",
        {
            "flow_id": FLOW_ID,
            "name": "weekday-mornings",
            "schedule": CRON,
            "parameters": {"channel": "#ops"},
        },
        (
            "POST",
            SCHEDULES_PATH,
            {},
            {
                "name": "weekday-mornings",
                "active": True,
                "schedule": CRON,
                "parameters": {"channel": "#ops"},
            },
        ),
        (201, schedule_response()),
        schedule_response(),
        id="create_schedule",
    ),
    pytest.param(
        "list_schedules",
        {"flow_id": FLOW_ID},
        ("GET", SCHEDULES_PATH, {}, None),
        (200, {"schedules": [schedule_response()]}),
        {"schedules": [schedule_response()]},
        id="list_schedules",
    ),
    pytest.param(
        "get_schedule",
        {"flow_id": FLOW_ID, "schedule_id": SCHEDULE_ID},
        ("GET", SCHEDULE_PATH, {}, None),
        (200, schedule_response()),
        schedule_response(),
        id="get_schedule",
    ),
    pytest.param(
        "delete_schedule",
        {"flow_id": FLOW_ID, "schedule_id": SCHEDULE_ID},
        ("DELETE", SCHEDULE_PATH, {}, None),
        (204, None),
        {"deleted": True, "flow_id": FLOW_ID, "schedule_id": SCHEDULE_ID},
        id="delete_schedule",
    ),
    pytest.param(
        "start_run",
        {"flow_id": FLOW_ID},
        ("POST", f"{PLAN_PATH}/runs", {}, {"parameters": {}}),
        (201, flow_run_response()),
        {"created": True, "flow_run_id": FLOW_RUN_ID, "flow_run": flow_run_response()},
        id="start_run-new",
    ),
    pytest.param(
        "start_run",
        {
            "flow_id": FLOW_ID,
            "parameters": {"channel": "#ops"},
            "name": "nightly",
            "idempotency_key": "key-1",
        },
        (
            "POST",
            f"{PLAN_PATH}/runs",
            {},
            {
                "parameters": {"channel": "#ops"},
                "name": "nightly",
                "idempotency_key": "key-1",
            },
        ),
        (200, flow_run_response(idempotency_key="key-1")),
        {
            "created": False,
            "flow_run_id": FLOW_RUN_ID,
            "flow_run": flow_run_response(idempotency_key="key-1"),
        },
        id="start_run-existing-key",
    ),
    pytest.param(
        "get_run",
        {"flow_run_id": FLOW_RUN_ID},
        ("GET", RUN_PATH, {}, None),
        (200, run_observation("blocked")),
        {**run_observation("blocked"), "wait": None},
        id="get_run-no-wait",
    ),
    pytest.param(
        "get_run_output",
        {"flow_run_id": FLOW_RUN_ID, "output_name": "summary"},
        ("GET", f"{RUN_PATH}/outputs/summary", {}, None),
        (200, {"text": "3 runs failed."}),
        available_output({"text": "3 runs failed."}),
        id="get_run_output-plan",
    ),
    pytest.param(
        "get_run_output",
        {
            "flow_run_id": FLOW_RUN_ID,
            "output_name": "done",
            "activation_id": ACTIVATION_ID,
        },
        ("GET", f"{RUN_PATH}/activations/{ACTIVATION_ID}/outputs/done", {}, None),
        (200, None),
        available_output(None),
        id="get_run_output-activation-null",
    ),
    pytest.param(
        "submit_human_input",
        {
            "flow_run_id": FLOW_RUN_ID,
            "activation_id": ACTIVATION_ID,
            "response": {"approved": True},
        },
        (
            "POST",
            f"{RUN_PATH}/activations/{ACTIVATION_ID}/human-input/responses",
            {},
            {"response": {"approved": True}},
        ),
        (204, None),
        {"submitted": True, "flow_run_id": FLOW_RUN_ID, "activation_id": ACTIVATION_ID},
        id="submit_human_input",
    ),
]


@pytest.mark.parametrize(
    ("tool", "arguments", "request_", "response", "expected"), ROWS
)
async def test_tool_sends_its_request_and_returns_the_result(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    tool: str,
    arguments: dict[str, Any],
    request_: tuple[str, str, dict[str, str], Any],
    response: tuple[int, Any],
    expected: Any,
):
    method, path, query, body = request_
    status_code, content = response
    cloud_api.request(method, path).respond(status_code, json=content)

    result = await mcp_client.call_tool(tool, arguments)

    sent = cloud_api.calls.last.request
    assert (sent.method, sent.url.path) == (method, WORKSPACE_API_PATH + path)
    assert dict(sent.url.params) == query
    assert request_body(sent) == body
    assert sent.headers["Authorization"] == f"Bearer {API_KEY}"
    assert result.structured_content == expected


@pytest.mark.parametrize(
    ("tool", "arguments", "message"),
    [
        ("get_run", {"flow_run_id": FLOW_RUN_ID, "wait_seconds": -1}, None),
        ("get_plan", {"flow_id": "daily-summary"}, None),
        (
            "submit_human_input",
            {
                "flow_run_id": FLOW_RUN_ID,
                "activation_id": ACTIVATION_ID,
                "response": "yes",
            },
            None,
        ),
        (
            "create_schedule",
            {
                "flow_id": FLOW_ID,
                "name": "x",
                "schedule": {"type": "rrule", "cron": "0 * * * *"},
            },
            "rrule",
        ),
        (
            "update_schedule",
            {"flow_id": FLOW_ID, "schedule_id": SCHEDULE_ID},
            "at least one",
        ),
        (
            "get_run_output",
            {
                "flow_run_id": FLOW_RUN_ID,
                "output_name": "../../../../block_documents/x?include_secrets=true",
            },
            None,
        ),
        ("get_run_output", {"flow_run_id": FLOW_RUN_ID, "output_name": ".."}, None),
        ("get_run_output", {"flow_run_id": FLOW_RUN_ID, "output_name": "a/b"}, None),
        ("get_flow", {"name": ".."}, "valid flow name"),
    ],
)
async def test_invalid_arguments_are_rejected_before_any_request(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    tool: str,
    arguments: dict[str, Any],
    message: str | None,
):
    result = await mcp_client.call_tool(tool, arguments, raise_on_error=False)

    assert result.is_error
    if message is not None:
        assert message in str(result.content)
    assert not cloud_api.calls
