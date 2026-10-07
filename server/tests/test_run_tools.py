"""Tests for run behavior beyond one request: long polling and output states."""

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
    PLAN_PATH,
    RUN_PATH,
    error_text,
    node_observation,
    run_observation,
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


async def get_run(client: Client[Any], wait_seconds: float) -> dict[str, Any]:
    result = await client.call_tool(
        "get_run", {"flow_run_id": FLOW_RUN_ID, "wait_seconds": wait_seconds}
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


def problem(output_status: str) -> dict[str, Any]:
    return {
        "detail": "Not yet.",
        "output_status": output_status,
        "reason": "incomplete",
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
            httpx.Response(409, json=problem("failed")),
            {**problem("failed"), "retry_after_seconds": None, "final": True},
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
