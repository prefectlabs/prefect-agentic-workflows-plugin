"""Tests for `get_flow_cost`, which reports a flow's token use per run, by node,
and by model, from the agent session analytics routes."""

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import respx
from fastmcp import Client
from support import FLOW_ID, VERSION_ID, request_body

from prefect_agentic_workflows_mcp.tools.flow_cost import (
    RUN_USAGE_PATH,
    TOKEN_USAGE_PATH,
)

START = "2026-11-18T00:00:00Z"
END = "2026-11-25T00:00:00Z"


def quantity(
    reported: int | None, coverage: str = "complete", finality: str = "final"
) -> dict[str, Any]:
    """Return one token count as the analytics routes return it."""
    return {
        "reported": reported,
        "coverage": coverage,
        "finality": finality,
        "issues": [],
    }


def contributions(
    input_tokens: int,
    output_tokens: int,
    *,
    calls: int = 4,
    runs: int = 2,
    invocations: int = 2,
    coverage: str = "complete",
    finality: str = "final",
) -> dict[str, Any]:
    """Return the contributions of a group of model calls."""
    return {
        "usage": {
            "input_tokens": quantity(input_tokens, coverage, finality),
            "output_tokens": quantity(output_tokens, coverage, finality),
            "total_tokens": quantity(input_tokens + output_tokens, coverage, finality),
            "categories": [],
        },
        "contributing_run_count": runs,
        "contributing_attempt_count": invocations,
        "contributing_invocation_count": invocations,
        "observed_call_count": calls,
        "calls_missing_input_tokens": 0,
        "calls_missing_output_tokens": 0,
        "calls_missing_total_tokens": 0,
    }


def context() -> dict[str, Any]:
    return {
        "requested_range": {"start": START, "end": END},
        "effective_range": {"start": START, "end": END},
        "omitted_intervals": [],
        "provisional": False,
    }


def token_usage_response(
    breakdowns: list[tuple[dict[str, Any], dict[str, Any]]],
    total: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return `POST /agent-sessions/analytics/token-usage` for one flow, with a
    breakdown for each (group, contributions) pair. No pairs and no total
    means the flow made no model calls in the range."""
    total = total or contributions(0, 0, calls=0, runs=0, invocations=0)
    rows = (
        [
            {
                "group": {"workflow_id": FLOW_ID},
                "contributions": total,
                "breakdowns": [
                    {"group": group, "contributions": each}
                    for group, each in breakdowns
                ],
                "trend": [],
            }
        ]
        if breakdowns
        else []
    )
    return {
        "context": context(),
        "workspace_contributions": total,
        "workflow_rows": rows,
        "workspace_trend": [],
        "next_cursor": None,
    }


def distribution(
    mean: float | None, median: float | None, p95: float | None, **overrides: Any
) -> dict[str, Any]:
    return {
        "eligible_count": 3,
        "observed_count": 0 if mean is None else 3,
        "missing_count": 0,
        "mean": mean,
        "median": median,
        "p95": p95,
        "method": "exact",
        "approximation": None,
        "coverage": {"status": "complete", "reasons": []},
        "provisional": False,
        **overrides,
    }


def run_usage_response(
    finished_runs: int = 3, **distributions: dict[str, Any]
) -> dict[str, Any]:
    """Return `POST /agent-sessions/analytics/run-usage` for one flow."""
    tokens_per_run = {
        "input_tokens": distribution(1000.4, 900.0, 2000.0),
        "output_tokens": distribution(200.6, 150.0, 400.0),
        "total_tokens": distribution(1201.0, 1050.0, 2400.0),
        **distributions,
    }
    return {
        "context": context(),
        "groups": [
            {
                "group": {"workflow_id": FLOW_ID},
                "statistics": {
                    "eligible_finished_run_count": finished_runs,
                    "tokens_per_run": tokens_per_run,
                    "outcomes": [],
                },
                "buckets": [],
            }
        ],
        "next_cursor": None,
    }


def mock_token_usage(
    cloud_api: respx.MockRouter,
    by_node: dict[str, Any],
    by_model: dict[str, Any],
) -> respx.Route:
    """Answer each token-usage read with the response for its breakdown."""
    responses = {"authored_node_id": by_node, "provider_model": by_model}

    def respond(request: httpx.Request) -> httpx.Response:
        (breakdown,) = request_body(request)["breakdown_by"]
        return httpx.Response(200, json=responses[breakdown])

    return cloud_api.post(TOKEN_USAGE_PATH).mock(side_effect=respond)


async def get_flow_cost(client: Client[Any], **arguments: Any) -> dict[str, Any]:
    result = await client.call_tool(
        "get_flow_cost", {"flow_id": FLOW_ID, "start": START, "end": END, **arguments}
    )
    assert result.structured_content is not None
    return result.structured_content


async def test_get_flow_cost_reports_tokens_per_run_by_node_and_by_model(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    total = contributions(9000, 1000, calls=12, runs=3, invocations=6)
    cloud_api.post(RUN_USAGE_PATH).respond(200, json=run_usage_response())
    mock_token_usage(
        cloud_api,
        by_node=token_usage_response(
            [
                (
                    {"authored_node_id": "research"},
                    contributions(8000, 500, calls=9, invocations=3),
                ),
                (
                    {"authored_node_id": "draft_reply"},
                    contributions(1000, 500, calls=3, invocations=3),
                ),
            ],
            total,
        ),
        by_model=token_usage_response(
            [
                (
                    {"provider": "anthropic", "model": "claude-sonnet-5-5"},
                    contributions(9000, 1000, calls=12),
                )
            ],
            total,
        ),
    )

    found = await get_flow_cost(mcp_client)

    assert found == {
        "range": {"start": START, "end": END},
        "counted_range": {"start": START, "end": END},
        "per_run": {
            "finished_runs": 3,
            "input_tokens": {"mean": 1000, "median": 900, "p95": 2000},
            "output_tokens": {"mean": 201, "median": 150, "p95": 400},
            "total_tokens": {"mean": 1201, "median": 1050, "p95": 2400},
            "complete": True,
            "final": True,
        },
        "total": {
            "input_tokens": 9000,
            "output_tokens": 1000,
            "total_tokens": 10000,
            "model_calls": 12,
            "complete": True,
            "final": True,
            "runs": 3,
        },
        "nodes": [
            {
                "node": "research",
                "input_tokens": 8000,
                "output_tokens": 500,
                "total_tokens": 8500,
                "model_calls": 9,
                "complete": True,
                "final": True,
                "invocations": 3,
            },
            {
                "node": "draft_reply",
                "input_tokens": 1000,
                "output_tokens": 500,
                "total_tokens": 1500,
                "model_calls": 3,
                "complete": True,
                "final": True,
                "invocations": 3,
            },
        ],
        "models": [
            {
                "provider": "anthropic",
                "model": "claude-sonnet-5-5",
                "input_tokens": 9000,
                "output_tokens": 1000,
                "total_tokens": 10000,
                "model_calls": 12,
                "complete": True,
                "final": True,
            }
        ],
    }


async def test_get_flow_cost_filters_every_read_to_the_flow_and_its_version(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    runs = cloud_api.post(RUN_USAGE_PATH).respond(200, json=run_usage_response())
    usage = mock_token_usage(
        cloud_api, token_usage_response([]), token_usage_response([])
    )

    await get_flow_cost(mcp_client, version_id=VERSION_ID)

    expected = {
        "range": {
            "start": "2026-11-18T00:00:00+00:00",
            "end": "2026-11-25T00:00:00+00:00",
        },
        "filters": {"workflow_ids": [FLOW_ID], "workflow_version_ids": [VERSION_ID]},
    }
    assert request_body(runs.calls.last.request) == expected
    assert sorted(
        (request_body(call.request) for call in usage.calls),
        key=lambda body: body["breakdown_by"],
    ) == [
        {**expected, "breakdown_by": ["authored_node_id"]},
        {**expected, "breakdown_by": ["provider_model"]},
    ]


async def test_get_flow_cost_reads_the_last_7_days_by_default(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    runs = cloud_api.post(RUN_USAGE_PATH).respond(200, json=run_usage_response())
    mock_token_usage(cloud_api, token_usage_response([]), token_usage_response([]))

    await mcp_client.call_tool("get_flow_cost", {"flow_id": FLOW_ID})

    body_range = request_body(runs.calls.last.request)["range"]
    start = datetime.fromisoformat(body_range["start"])
    end = datetime.fromisoformat(body_range["end"])
    assert end - start == timedelta(days=7)
    assert abs(end - datetime.now(UTC)) < timedelta(minutes=1)


async def test_get_flow_cost_reports_a_flow_with_no_runs_in_the_range(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(RUN_USAGE_PATH).respond(
        200, json={**run_usage_response(), "groups": []}
    )
    mock_token_usage(cloud_api, token_usage_response([]), token_usage_response([]))

    found = await get_flow_cost(mcp_client)

    assert found["per_run"] == {
        "finished_runs": 0,
        "input_tokens": None,
        "output_tokens": None,
        "total_tokens": None,
        "complete": True,
        "final": True,
    }
    assert found["total"]["model_calls"] == 0
    assert found["nodes"] == []
    assert found["models"] == []


async def test_get_flow_cost_marks_counts_that_are_partial_or_can_still_change(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    partial = distribution(
        1000.0,
        1000.0,
        1000.0,
        coverage={"status": "partial", "reasons": ["missing_report"]},
    )
    cloud_api.post(RUN_USAGE_PATH).respond(
        200,
        json=run_usage_response(
            input_tokens=partial,
            output_tokens=distribution(10.0, 10.0, 10.0, provisional=True),
        ),
    )
    still_running = contributions(100, 10, finality="provisional")
    missing = contributions(100, 10, coverage="partial")
    mock_token_usage(
        cloud_api,
        by_node=token_usage_response(
            [({"authored_node_id": "research"}, still_running)], still_running
        ),
        by_model=token_usage_response(
            [({"provider": None, "model": None}, missing)], still_running
        ),
    )

    found = await get_flow_cost(mcp_client)

    assert (found["per_run"]["complete"], found["per_run"]["final"]) == (False, False)
    assert (found["total"]["complete"], found["total"]["final"]) == (True, False)
    assert (found["nodes"][0]["complete"], found["nodes"][0]["final"]) == (True, False)
    assert found["models"][0]["provider"] is None
    assert (found["models"][0]["complete"], found["models"][0]["final"]) == (
        False,
        True,
    )


async def test_get_flow_cost_reports_no_per_run_count_that_no_run_reports(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    unreported = distribution(
        None,
        None,
        None,
        missing_count=3,
        coverage={"status": "unknown", "reasons": ["missing_report"]},
    )
    cloud_api.post(RUN_USAGE_PATH).respond(
        200, json=run_usage_response(output_tokens=unreported)
    )
    mock_token_usage(cloud_api, token_usage_response([]), token_usage_response([]))

    found = await get_flow_cost(mcp_client)

    assert found["per_run"]["output_tokens"] is None
    assert found["per_run"]["input_tokens"] == {
        "mean": 1000,
        "median": 900,
        "p95": 2000,
    }
    assert found["per_run"]["complete"] is False
