"""Tests for the tools that start runs, watch them, read outputs, and answer forms."""

import json
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
import respx
from fastmcp import Client

from prefect_agentic_workflows_mcp.tools import runs
from support import error_text

FLOW_ID = "33333333-3333-3333-3333-333333333333"
FLOW_RUN_ID = "66666666-6666-6666-6666-666666666666"
SNAPSHOT_ID = "77777777-7777-7777-7777-777777777777"
ACTIVATION_ID = "88888888-8888-8888-8888-888888888888"
RUNS_PATH = f"/flows/{FLOW_ID}/execution-plan/runs"
RUN_PATH = f"/flow_runs/{FLOW_RUN_ID}/execution-plan"


class FakeClock:
    """Replaces the clock and `sleep` that `get_run` uses to wait.

    Sleeping moves the clock forward at once, so a test can wait 30 seconds
    of clock time without taking 30 seconds.
    """

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeClock]:
    fake = FakeClock()
    monkeypatch.setattr(runs, "monotonic", fake.monotonic)
    monkeypatch.setattr(runs, "sleep", fake.sleep)
    yield fake


def flow_run_response(**overrides: Any) -> dict[str, Any]:
    """Return a `POST .../execution-plan/runs` body in the shape Cloud returns."""
    body: dict[str, Any] = {
        "id": FLOW_RUN_ID,
        "flow_id": FLOW_ID,
        "name": "crimson-otter",
        "parameters": {},
        "idempotency_key": None,
        "state_type": "RUNNING",
        "state_name": "Running",
        "execution_plan_snapshot_id": SNAPSHOT_ID,
    }
    body.update(overrides)
    return body


async def test_start_run_starts_a_run_of_the_active_plan(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post(RUNS_PATH).respond(201, json=flow_run_response())

    result = await mcp_client.call_tool("start_run", {"flow_id": FLOW_ID})

    assert json.loads(route.calls.last.request.content) == {"parameters": {}}
    assert result.structured_content == {
        "created": True,
        "flow_run_id": FLOW_RUN_ID,
        "flow_run": flow_run_response(),
    }


async def test_start_run_sends_parameters_name_and_idempotency_key(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post(RUNS_PATH).respond(
        201,
        json=flow_run_response(
            name="nightly-test",
            parameters={"channel": "#ops"},
            idempotency_key="test-1",
        ),
    )

    await mcp_client.call_tool(
        "start_run",
        {
            "flow_id": FLOW_ID,
            "parameters": {"channel": "#ops"},
            "name": "nightly-test",
            "idempotency_key": "test-1",
        },
    )

    assert json.loads(route.calls.last.request.content) == {
        "parameters": {"channel": "#ops"},
        "name": "nightly-test",
        "idempotency_key": "test-1",
    }


async def test_start_run_reports_a_run_that_already_exists_for_the_key(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(RUNS_PATH).respond(
        200, json=flow_run_response(idempotency_key="test-1")
    )

    result = await mcp_client.call_tool(
        "start_run", {"flow_id": FLOW_ID, "idempotency_key": "test-1"}
    )

    assert result.structured_content is not None
    assert result.structured_content["created"] is False
    assert result.structured_content["flow_run_id"] == FLOW_RUN_ID


async def test_start_run_reports_a_flow_without_an_active_plan(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(RUNS_PATH).respond(
        404, json={"detail": "Active execution plan version not found."}
    )

    result = await mcp_client.call_tool(
        "start_run", {"flow_id": FLOW_ID}, raise_on_error=False
    )

    message = error_text(result)
    assert "404" in message
    assert "Active execution plan version not found." in message


def node_observation(
    node: str = "summarize", status: str = "running", **overrides: Any
) -> dict[str, Any]:
    """Return one node of a run observation in the shape Cloud returns."""
    body: dict[str, Any] = {
        "node": node,
        "kind": "AgentNode",
        "status": status,
        "activation_id": ACTIVATION_ID,
        "outputs": [{"output": "done", "status": "pending"}],
    }
    body.update(overrides)
    return body


def run_observation(
    status: str = "running", nodes: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    """Return a `GET /flow_runs/{id}/execution-plan` body in the shape Cloud returns."""
    return {
        "snapshot": {
            "id": SNAPSHOT_ID,
            "flow_run_id": FLOW_RUN_ID,
            "flow_id": FLOW_ID,
            "schema_version": "0.1",
            "graph_revision": 3,
        },
        "status": status,
        "nodes": nodes if nodes is not None else [node_observation()],
        "edges": [],
        "outputs": [{"output": "summary", "status": "waiting"}],
        "diagnostics": [
            {
                "kind": "evaluation_pending",
                "category": "evaluation",
                "severity": "info",
                "message": "The plan is waiting for its next evaluation.",
            }
        ],
    }


async def test_get_run_returns_the_run_without_waiting(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, clock: FakeClock
):
    waiting_node = node_observation(
        "approve",
        "suspended",
        kind="HumanInputNode",
        wait={
            "kind": "human_input",
            "activation_id": ACTIVATION_ID,
            "form_schema": {
                "type": "object",
                "properties": {"approved": {"type": "boolean"}},
            },
            "deadline_at": "2026-09-25T13:00:00Z",
        },
    )
    failed_node = node_observation(
        "publish",
        "failed",
        failure={"code": "agent_timeout", "message": "The agent ran too long."},
    )
    observation = run_observation(
        "awaiting_external_progress", [waiting_node, failed_node]
    )
    route = cloud_api.get(RUN_PATH).respond(200, json=observation)

    result = await mcp_client.call_tool("get_run", {"flow_run_id": FLOW_RUN_ID})

    assert route.call_count == 1
    assert clock.sleeps == []
    assert result.structured_content == {**observation, "wait": None}


async def test_get_run_returns_as_soon_as_the_run_status_changes(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, clock: FakeClock
):
    route = cloud_api.get(RUN_PATH).mock(
        side_effect=[
            httpx.Response(200, json=run_observation("running")),
            httpx.Response(200, json=run_observation("running")),
            httpx.Response(200, json=run_observation("completed")),
            httpx.Response(200, json=run_observation("completed")),
        ]
    )

    result = await mcp_client.call_tool(
        "get_run", {"flow_run_id": FLOW_RUN_ID, "wait_seconds": 30}
    )

    assert route.call_count == 3
    assert sum(clock.sleeps) == 4
    assert result.structured_content == {
        **run_observation("completed"),
        "wait": {"seconds_waited": 4.0, "status_changed": True},
    }


async def test_get_run_returns_as_soon_as_a_node_status_changes(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, clock: FakeClock
):
    before = run_observation("running", [node_observation(status="running")])
    after = run_observation("running", [node_observation(status="completed")])
    route = cloud_api.get(RUN_PATH).mock(
        side_effect=[httpx.Response(200, json=before), httpx.Response(200, json=after)]
    )

    result = await mcp_client.call_tool(
        "get_run", {"flow_run_id": FLOW_RUN_ID, "wait_seconds": 10}
    )

    assert route.call_count == 2
    assert result.structured_content is not None
    assert result.structured_content["nodes"] == after["nodes"]
    assert result.structured_content["wait"]["status_changed"] is True


@pytest.mark.parametrize("wait_seconds", [30, 45, 3600])
async def test_get_run_never_waits_past_the_cap(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    clock: FakeClock,
    wait_seconds: int,
):
    cloud_api.get(RUN_PATH).respond(200, json=run_observation("running"))

    result = await mcp_client.call_tool(
        "get_run", {"flow_run_id": FLOW_RUN_ID, "wait_seconds": wait_seconds}
    )

    assert sum(clock.sleeps) == 30
    assert result.structured_content == {
        **run_observation("running"),
        "wait": {"seconds_waited": 30.0, "status_changed": False},
    }


async def test_get_run_waits_no_longer_than_asked(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, clock: FakeClock
):
    cloud_api.get(RUN_PATH).respond(200, json=run_observation("running"))

    result = await mcp_client.call_tool(
        "get_run", {"flow_run_id": FLOW_RUN_ID, "wait_seconds": 5}
    )

    assert sum(clock.sleeps) == 5
    assert result.structured_content is not None
    assert result.structured_content["wait"] == {
        "seconds_waited": 5.0,
        "status_changed": False,
    }


@pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
async def test_get_run_does_not_wait_on_a_finished_run(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    clock: FakeClock,
    status: str,
):
    route = cloud_api.get(RUN_PATH).respond(200, json=run_observation(status))

    result = await mcp_client.call_tool(
        "get_run", {"flow_run_id": FLOW_RUN_ID, "wait_seconds": 30}
    )

    assert route.call_count == 1
    assert clock.sleeps == []
    assert result.structured_content is not None
    assert result.structured_content["wait"] == {
        "seconds_waited": 0.0,
        "status_changed": False,
    }


async def test_get_run_rejects_a_negative_wait(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, clock: FakeClock
):
    route = cloud_api.get(RUN_PATH).respond(200, json=run_observation())

    result = await mcp_client.call_tool(
        "get_run",
        {"flow_run_id": FLOW_RUN_ID, "wait_seconds": -1},
        raise_on_error=False,
    )

    assert result.is_error
    assert not route.called


async def test_get_run_reports_retry_after_when_cloud_is_busy(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, clock: FakeClock
):
    cloud_api.get(RUN_PATH).respond(
        503,
        headers={"Retry-After": "2"},
        json={
            "detail": "Execution plan snapshot loading is temporarily unavailable."
        },
    )

    result = await mcp_client.call_tool(
        "get_run", {"flow_run_id": FLOW_RUN_ID}, raise_on_error=False
    )

    message = error_text(result)
    assert "503" in message
    assert "temporarily unavailable" in message
    assert "Retry in 2 seconds." in message


PLAN_OUTPUT_PATH = f"{RUN_PATH}/outputs/summary"
ACTIVATION_OUTPUT_PATH = f"{RUN_PATH}/activations/{ACTIVATION_ID}/outputs/done"


@pytest.mark.parametrize("value", [{"text": "3 runs failed."}, ["a", "b"], 7, None])
async def test_get_run_output_reads_a_plan_output(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, value: Any
):
    cloud_api.get(PLAN_OUTPUT_PATH).respond(200, json=value)

    result = await mcp_client.call_tool(
        "get_run_output", {"flow_run_id": FLOW_RUN_ID, "output_name": "summary"}
    )

    assert result.structured_content == {
        "available": True,
        "value": value,
        "output_status": "available",
        "reason": None,
        "detail": None,
        "retry_after_seconds": None,
    }


async def test_get_run_output_reads_an_activation_output(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(ACTIVATION_OUTPUT_PATH).respond(200, json={"approved": True})

    result = await mcp_client.call_tool(
        "get_run_output",
        {
            "flow_run_id": FLOW_RUN_ID,
            "output_name": "done",
            "activation_id": ACTIVATION_ID,
        },
    )

    assert result.structured_content is not None
    assert result.structured_content["available"] is True
    assert result.structured_content["value"] == {"approved": True}


async def test_get_run_output_reports_a_pending_activation_output(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(ACTIVATION_OUTPUT_PATH).respond(
        202,
        headers={"Retry-After": "2"},
        json={
            "title": "Execution plan node output pending",
            "status": 202,
            "detail": "The declared node output is not available yet.",
            "output_status": "waiting",
            "reason": "activation_incomplete",
        },
    )

    result = await mcp_client.call_tool(
        "get_run_output",
        {
            "flow_run_id": FLOW_RUN_ID,
            "output_name": "done",
            "activation_id": ACTIVATION_ID,
        },
    )

    assert result.structured_content == {
        "available": False,
        "value": None,
        "output_status": "waiting",
        "reason": "activation_incomplete",
        "detail": "The declared node output is not available yet.",
        "retry_after_seconds": 2.0,
    }


async def test_get_run_output_reports_an_unavailable_plan_output(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(PLAN_OUTPUT_PATH).respond(
        409,
        headers={"Content-Type": "application/problem+json"},
        json={
            "title": "Execution plan output unavailable",
            "status": 409,
            "detail": "The declared execution plan output is not available.",
            "output_status": "failed",
            "reason": "output_not_available",
        },
    )

    result = await mcp_client.call_tool(
        "get_run_output", {"flow_run_id": FLOW_RUN_ID, "output_name": "summary"}
    )

    assert result.structured_content == {
        "available": False,
        "value": None,
        "output_status": "failed",
        "reason": "output_not_available",
        "detail": "The declared execution plan output is not available.",
        "retry_after_seconds": None,
    }


@pytest.mark.parametrize("status_code", [409, 503])
async def test_get_run_output_passes_on_retry_after(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, status_code: int
):
    cloud_api.get(PLAN_OUTPUT_PATH).respond(
        status_code,
        headers={"Retry-After": "5"},
        json={"detail": "Execution plan snapshot loading is temporarily unavailable."},
    )

    result = await mcp_client.call_tool(
        "get_run_output", {"flow_run_id": FLOW_RUN_ID, "output_name": "summary"}
    )

    assert result.structured_content == {
        "available": False,
        "value": None,
        "output_status": None,
        "reason": None,
        "detail": "Execution plan snapshot loading is temporarily unavailable.",
        "retry_after_seconds": 5.0,
    }


async def test_get_run_output_reads_a_retry_after_date(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(PLAN_OUTPUT_PATH).respond(
        503,
        headers={"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"},
        json={"detail": "Busy."},
    )

    result = await mcp_client.call_tool(
        "get_run_output", {"flow_run_id": FLOW_RUN_ID, "output_name": "summary"}
    )

    assert result.structured_content is not None
    assert result.structured_content["retry_after_seconds"] == 0.0


async def test_get_run_output_reports_an_output_the_plan_does_not_declare(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(f"{RUN_PATH}/outputs/missing").respond(
        404, json={"detail": "Not Found"}
    )

    result = await mcp_client.call_tool(
        "get_run_output",
        {"flow_run_id": FLOW_RUN_ID, "output_name": "missing"},
        raise_on_error=False,
    )

    assert "404" in error_text(result)


RESPONSES_PATH = f"{RUN_PATH}/activations/{ACTIVATION_ID}/human-input/responses"


async def test_submit_human_input_sends_the_response(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post(RESPONSES_PATH).respond(204)

    result = await mcp_client.call_tool(
        "submit_human_input",
        {
            "flow_run_id": FLOW_RUN_ID,
            "activation_id": ACTIVATION_ID,
            "response": {"approved": True, "note": "Ship it."},
        },
    )

    assert json.loads(route.calls.last.request.content) == {
        "response": {"approved": True, "note": "Ship it."}
    }
    assert result.structured_content == {
        "submitted": True,
        "flow_run_id": FLOW_RUN_ID,
        "activation_id": ACTIVATION_ID,
    }


async def test_submit_human_input_reports_a_form_that_is_already_answered(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(RESPONSES_PATH).respond(
        409, json={"detail": "The HumanInput request has already been resolved."}
    )

    result = await mcp_client.call_tool(
        "submit_human_input",
        {
            "flow_run_id": FLOW_RUN_ID,
            "activation_id": ACTIVATION_ID,
            "response": {"approved": True},
        },
        raise_on_error=False,
    )

    message = error_text(result)
    assert "409" in message
    assert "already been resolved" in message


async def test_submit_human_input_reports_retry_after(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(RESPONSES_PATH).respond(
        503,
        headers={"Retry-After": "2"},
        json={
            "detail": (
                "Execution plan graph state is temporarily unavailable. "
                "The submission may have been accepted; reread before retrying."
            )
        },
    )

    result = await mcp_client.call_tool(
        "submit_human_input",
        {
            "flow_run_id": FLOW_RUN_ID,
            "activation_id": ACTIVATION_ID,
            "response": {"approved": True},
        },
        raise_on_error=False,
    )

    message = error_text(result)
    assert "reread before retrying" in message
    assert "Retry in 2 seconds." in message


async def test_submit_human_input_needs_an_object_response(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post(RESPONSES_PATH).respond(204)

    result = await mcp_client.call_tool(
        "submit_human_input",
        {
            "flow_run_id": FLOW_RUN_ID,
            "activation_id": ACTIVATION_ID,
            "response": "yes",
        },
        raise_on_error=False,
    )

    assert result.is_error
    assert not route.called


async def test_run_tools_have_the_right_hints(mcp_client: Client[Any]):
    tools = {tool.name: tool for tool in await mcp_client.list_tools()}
    expected = {
        "start_run": (False, True),
        "get_run": (True, False),
        "get_run_output": (True, False),
        "submit_human_input": (False, False),
    }

    for name, (read_only, destructive) in expected.items():
        annotations = tools[name].annotations
        assert annotations is not None, name
        assert annotations.readOnlyHint is read_only, name
        assert annotations.destructiveHint is destructive, name
        assert "alpha" in (tools[name].description or ""), name


async def test_there_is_no_cancel_tool(mcp_client: Client[Any]):
    names = {tool.name for tool in await mcp_client.list_tools()}

    assert not any("cancel" in name for name in names)
