"""Tests for run behavior beyond one request: listing runs, long polling, and
output states."""

from collections.abc import Iterator
from typing import Any

import httpx
import pytest
import respx
from fastmcp import Client
from support import (
    ACTIVATION_ID,
    FLOW_ID,
    FLOW_RUN_ID,
    OLDER_VERSION_ID,
    PLAN_PATH,
    RUN_PATH,
    RUNS_PATH,
    VERSION_ID,
    error_text,
    flow_run_response,
    node_observation,
    request_body,
    run_observation,
    run_summary,
)

from prefect_agentic_workflows_mcp.tools import runs


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


async def get_run(
    client: Client[Any], wait_seconds: float | None, **arguments: Any
) -> dict[str, Any]:
    result = await client.call_tool(
        "get_run",
        {"flow_run_id": FLOW_RUN_ID, "wait_seconds": wait_seconds, **arguments},
    )
    assert result.structured_content is not None
    return result.structured_content


@pytest.mark.parametrize(
    "observations",
    [
        [run_observation("running")] * 2 + [run_observation("completed")],
        [run_observation("running", [node_observation("running")])] * 2
        + [run_observation("running", [node_observation("suspended")])],
    ],
    ids=["run-status", "node-status"],
)
async def test_get_run_returns_as_soon_as_the_run_or_a_node_changes(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    clock: FakeClock,
    observations: list[dict[str, Any]],
):
    route = cloud_api.get(RUN_PATH).mock(
        side_effect=[httpx.Response(200, json=body) for body in observations]
        + [httpx.Response(500)]
    )

    result = await get_run(mcp_client, 30)

    assert route.call_count == 3
    assert result == {
        **observations[-1],
        "wait": {"seconds_waited": 4.0, "status_changed": True},
    }


@pytest.mark.parametrize(("wait_seconds", "waited"), [(5, 5), (30, 30), (3600, 30)])
async def test_get_run_waits_as_asked_but_never_past_the_cap(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    clock: FakeClock,
    wait_seconds: int,
    waited: int,
):
    cloud_api.get(RUN_PATH).respond(200, json=run_observation("running"))

    result = await get_run(mcp_client, wait_seconds)

    assert sum(clock.sleeps) == waited
    assert result["wait"] == {"seconds_waited": waited, "status_changed": False}


@pytest.mark.parametrize("status", ["completed", "failed", "cancelled", "blocked"])
async def test_get_run_does_not_wait_on_a_finished_or_blocked_run(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, clock: FakeClock, status: str
):
    route = cloud_api.get(RUN_PATH).respond(200, json=run_observation(status))

    result = await get_run(mcp_client, 30)

    assert route.call_count == 1
    assert clock.sleeps == []
    assert result["wait"] == {"seconds_waited": 0.0, "status_changed": False}


async def test_get_run_does_not_wait_while_a_form_waits_for_an_answer(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, clock: FakeClock
):
    waiting = node_observation(
        "suspended",
        node="approve",
        kind="HumanInputNode",
        wait={"kind": "human_input", "activation_id": ACTIVATION_ID},
    )
    observation = run_observation("awaiting_external_progress", [waiting])
    route = cloud_api.get(RUN_PATH).respond(200, json=observation)

    result = await get_run(mcp_client, 30)

    assert route.call_count == 1
    assert clock.sleeps == []
    assert result["wait"] == {"seconds_waited": 0.0, "status_changed": False}


async def test_get_run_bounds_each_read_by_the_time_left(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, clock: FakeClock
):
    read_timeouts: list[float] = []

    def respond(request: httpx.Request) -> httpx.Response:
        read_timeouts.append(request.extensions["timeout"]["read"])
        return httpx.Response(200, json=run_observation("running"))

    cloud_api.get(RUN_PATH).mock(side_effect=respond)

    await get_run(mcp_client, 5)

    # The first read happens before the wait starts. Each later read gets only
    # the seconds left before the 5-second deadline, and none starts after it.
    assert read_timeouts[1:] == [3.0, 1.0]


async def test_get_run_returns_the_last_observation_when_a_poll_times_out(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, clock: FakeClock
):
    observation = run_observation("running")
    cloud_api.get(RUN_PATH).mock(
        side_effect=[
            httpx.Response(200, json=observation),
            httpx.ReadTimeout("Cloud took too long"),
        ]
    )

    result = await get_run(mcp_client, 5)

    assert result["status"] == "running"
    assert result["wait"]["status_changed"] is False


def with_outputs(observation: dict[str, Any], status: str) -> dict[str, Any]:
    return {**observation, "outputs": [{"output": "summary", "status": status}]}


async def test_get_run_until_settled_waits_through_node_changes(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, clock: FakeClock
):
    observations = [
        run_observation("running", [node_observation("running")]),
        run_observation("running", [node_observation("completed")]),
        with_outputs(run_observation("completed"), "waiting"),
        with_outputs(run_observation("completed"), "available"),
    ]
    route = cloud_api.get(RUN_PATH).mock(
        side_effect=[httpx.Response(200, json=body) for body in observations]
    )

    result = await get_run(mcp_client, 30, until="settled")

    assert route.call_count == 4
    assert result == {
        **observations[-1],
        "wait": {"seconds_waited": 6.0, "status_changed": True},
    }


async def test_get_run_until_settled_reports_outputs_built_as_a_change(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, clock: FakeClock
):
    observations = [
        with_outputs(run_observation("completed"), "waiting"),
        with_outputs(run_observation("completed"), "available"),
    ]
    cloud_api.get(RUN_PATH).mock(
        side_effect=[httpx.Response(200, json=body) for body in observations]
    )

    result = await get_run(mcp_client, 30, until="settled")

    assert result["wait"]["status_changed"] is True


async def test_get_run_until_settled_stops_when_a_form_waits(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, clock: FakeClock
):
    waiting = node_observation(
        "suspended",
        node="approve",
        kind="HumanInputNode",
        wait={"kind": "human_input", "activation_id": ACTIVATION_ID},
    )
    observations = [
        run_observation("running"),
        run_observation("awaiting_external_progress", [waiting]),
    ]
    route = cloud_api.get(RUN_PATH).mock(
        side_effect=[httpx.Response(200, json=body) for body in observations]
    )

    result = await get_run(mcp_client, 30, until="settled")

    assert route.call_count == 2
    assert result["status"] == "awaiting_external_progress"


async def test_get_run_compact_leaves_out_edges_and_task_runs(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    node = node_observation(
        "completed",
        task_run={"id": ACTIVATION_ID, "state_type": "COMPLETED"},
        outputs=[
            {"output": "done", "status": "selected", "result": {"metadata": {}}},
            {"output": "blocked", "status": "skipped"},
        ],
        failure=None,
    )
    observation = {
        **run_observation("completed", [node]),
        "edges": [{"edge": "a_to_b", "status": "produced"}],
        "outputs": [
            {
                "output": "summary",
                "status": "failed",
                "failure_code": "required_field_missing",
                "provenance": [{"field_name": "reply"}],
            }
        ],
    }
    cloud_api.get(RUN_PATH).respond(200, json=observation)

    result = await get_run(mcp_client, None, compact=True)

    assert result == {
        "flow_run_id": FLOW_RUN_ID,
        "status": "completed",
        "nodes": [
            {
                "node": "summarize",
                "status": "completed",
                "activation_id": ACTIVATION_ID,
                "selected_outputs": ["done"],
                "failure": None,
                "wait": None,
            }
        ],
        "outputs": [
            {
                "output": "summary",
                "status": "failed",
                "failure_code": "required_field_missing",
            }
        ],
        "diagnostics": [],
        "wait": None,
    }


async def test_get_run_output_reports_a_missing_bucket_as_an_error(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(f"{RUN_PATH}/outputs/result").respond(
        409,
        json={"detail": "Workspace object storage bucket has not been provisioned."},
    )

    result = await mcp_client.call_tool(
        "get_run_output",
        {"flow_run_id": FLOW_RUN_ID, "output_name": "result"},
        raise_on_error=False,
    )

    assert "no object storage bucket" in error_text(result)


def problem(output_status: str, failure_code: str | None = None) -> dict[str, Any]:
    return {
        "detail": "Not yet.",
        "output_status": output_status,
        "reason": "incomplete",
        "failure_code": failure_code,
    }


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        (
            httpx.Response(202, headers={"Retry-After": "2"}, json=problem("waiting")),
            {**problem("waiting"), "retry_after_seconds": 2.0, "final": False},
        ),
        (
            httpx.Response(409, json=problem("pending")),
            {**problem("pending"), "retry_after_seconds": None, "final": False},
        ),
        (
            httpx.Response(409, json=problem("failed", "required_field_missing")),
            {
                **problem("failed", "required_field_missing"),
                "retry_after_seconds": None,
                "final": True,
            },
        ),
        (
            httpx.Response(409, json=problem("skipped")),
            {**problem("skipped"), "retry_after_seconds": None, "final": True},
        ),
        (
            httpx.Response(503, headers={"Retry-After": "5"}, text="Busy."),
            {
                "detail": "Busy.",
                "output_status": None,
                "reason": None,
                "failure_code": None,
                "retry_after_seconds": 5.0,
                "final": False,
            },
        ),
    ],
    ids=["waiting", "pending", "failed", "skipped", "busy"],
)
async def test_get_run_output_reports_an_output_it_cannot_read(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    response: httpx.Response,
    expected: dict[str, Any],
):
    cloud_api.get(f"{RUN_PATH}/outputs/summary").mock(return_value=response)

    result = await mcp_client.call_tool(
        "get_run_output", {"flow_run_id": FLOW_RUN_ID, "output_name": "summary"}
    )

    assert result.structured_content == {"available": False, "value": None, **expected}


async def test_start_run_reports_a_flow_without_an_active_plan(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    detail = "Active execution plan version not found."
    cloud_api.post(f"{PLAN_PATH}/runs").respond(404, json={"detail": detail})

    result = await mcp_client.call_tool(
        "start_run", {"flow_id": FLOW_ID}, raise_on_error=False
    )

    assert detail in error_text(result)


async def test_submit_human_input_reports_a_form_that_is_already_answered(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    detail = "The HumanInput request has already been resolved."
    cloud_api.post(
        f"{RUN_PATH}/activations/{ACTIVATION_ID}/human-input/responses"
    ).respond(409, json={"detail": detail})

    result = await mcp_client.call_tool(
        "submit_human_input",
        {
            "flow_run_id": FLOW_RUN_ID,
            "activation_id": ACTIVATION_ID,
            "response": {"approved": True},
        },
        raise_on_error=False,
    )

    assert detail in error_text(result)


OTHER_FLOW_RUN_ID = "06ae907e-ebfe-7f61-8000-ede83bf32dc2"


async def test_list_runs_merges_each_run_with_its_summary_in_filter_order(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    newer = flow_run_response(
        id=OTHER_FLOW_RUN_ID,
        name="amber-heron",
        state_type="FAILED",
        state_name="Failed",
        created="2026-11-02T22:00:00Z",
        start_time="2026-11-02T22:00:01Z",
    )
    older = flow_run_response(
        state_type="FAILED",
        state_name="Failed",
        created="2026-11-01T09:00:00Z",
        start_time="2026-11-01T09:00:01Z",
    )
    run_filter = cloud_api.post(f"{RUNS_PATH}/filter").respond(200, json=[newer, older])
    # The summaries route answers in request order, but the tool must not
    # depend on it.
    summaries = cloud_api.post(f"{RUNS_PATH}/summaries").respond(
        200,
        json=[
            run_summary(),
            run_summary(
                OTHER_FLOW_RUN_ID,
                status="failed",
                outputs=[
                    {
                        "output": "reply",
                        "status": "failed",
                        "failure_code": "required_field_missing",
                    }
                ],
                last_nodes=[],
            ),
        ],
    )

    result = await mcp_client.call_tool(
        "list_runs",
        {
            "flow_id": FLOW_ID,
            "state_types": ["FAILED", "CRASHED"],
            "version_id": OLDER_VERSION_ID,
            "limit": 2,
        },
    )

    assert request_body(run_filter.calls.last.request) == {
        "sort": "EXPECTED_START_TIME_DESC",
        "limit": 2,
        "state": {"type": {"any_": ["FAILED", "CRASHED"]}},
        "execution_plan_version_ids": [OLDER_VERSION_ID],
    }
    assert request_body(summaries.calls.last.request) == {
        "flow_run_ids": [OTHER_FLOW_RUN_ID, FLOW_RUN_ID]
    }
    assert result.structured_content == {
        "runs": [
            {
                "flow_run_id": OTHER_FLOW_RUN_ID,
                "name": "amber-heron",
                "state_type": "FAILED",
                "state_name": "Failed",
                "created": "2026-11-02T22:00:00Z",
                "start_time": "2026-11-02T22:00:01Z",
                "status": "failed",
                "execution_plan_version_id": VERSION_ID,
                "outputs": [
                    {
                        "output": "reply",
                        "status": "failed",
                        "failure_code": "required_field_missing",
                    }
                ],
                "last_nodes": [],
                "unavailable_reason": None,
            },
            {
                "flow_run_id": FLOW_RUN_ID,
                "name": "crimson-otter",
                "state_type": "FAILED",
                "state_name": "Failed",
                "created": "2026-11-01T09:00:00Z",
                "start_time": "2026-11-01T09:00:01Z",
                "status": "completed",
                "execution_plan_version_id": VERSION_ID,
                "outputs": [
                    {"output": "reply", "status": "available", "failure_code": None}
                ],
                "last_nodes": [{"node": "draft", "selected_outputs": ["drafted"]}],
                "unavailable_reason": None,
            },
        ]
    }


async def test_list_runs_reports_a_run_whose_plan_cannot_be_read(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(f"{RUNS_PATH}/filter").respond(200, json=[flow_run_response()])
    cloud_api.post(f"{RUNS_PATH}/summaries").respond(
        200, json=[{"flow_run_id": FLOW_RUN_ID, "unavailable_reason": "no_snapshot"}]
    )

    result = await mcp_client.call_tool("list_runs", {"flow_id": FLOW_ID})

    assert result.structured_content is not None
    [run] = result.structured_content["runs"]
    assert run["status"] is None
    assert run["outputs"] == []
    assert run["unavailable_reason"] == "no_snapshot"


async def test_list_runs_sends_only_the_filter_for_a_flow_with_no_runs(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    run_filter = cloud_api.post(f"{RUNS_PATH}/filter").respond(200, json=[])
    summaries = cloud_api.post(f"{RUNS_PATH}/summaries")

    result = await mcp_client.call_tool("list_runs", {"flow_id": FLOW_ID})

    assert request_body(run_filter.calls.last.request) == {
        "sort": "EXPECTED_START_TIME_DESC",
        "limit": 10,
    }
    assert not summaries.called
    assert result.structured_content == {"runs": []}


async def test_list_runs_says_when_to_retry_reading_summaries(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(f"{RUNS_PATH}/filter").respond(200, json=[flow_run_response()])
    detail = "Execution plan snapshot loading is temporarily unavailable."
    cloud_api.post(f"{RUNS_PATH}/summaries").respond(
        503, headers={"Retry-After": "2"}, json={"detail": detail}
    )

    result = await mcp_client.call_tool(
        "list_runs", {"flow_id": FLOW_ID}, raise_on_error=False
    )

    message = error_text(result)
    assert detail in message
    assert message.endswith("Retry in 2 seconds.")


async def test_list_runs_keeps_a_run_that_has_no_summary(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    # A run deleted between the two requests is left out of the summaries.
    cloud_api.post(f"{RUNS_PATH}/filter").respond(200, json=[flow_run_response()])
    cloud_api.post(f"{RUNS_PATH}/summaries").respond(200, json=[])

    result = await mcp_client.call_tool("list_runs", {"flow_id": FLOW_ID})

    assert result.structured_content is not None
    [run] = result.structured_content["runs"]
    assert run["flow_run_id"] == FLOW_RUN_ID
    assert (run["status"], run["outputs"], run["last_nodes"]) == (None, [], [])
