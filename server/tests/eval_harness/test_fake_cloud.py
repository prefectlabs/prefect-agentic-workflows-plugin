"""Tests for the fake Prefect Cloud API that evaluations run against.

Each test calls the server's tools, so the fake is checked with the same
requests an agent makes during an evaluation. Only the behavior the scenarios
rely on is covered here.
"""

from collections.abc import Callable
from typing import Any

import pytest
from fastmcp import Client
from harness_plans import approval_plan, cyclic_plan
from support import error_text

from evals.fake_cloud import FakeCloud, NodeScript


async def call(client: Client[Any], tool: str, **arguments: Any) -> Any:
    result = await client.call_tool(tool, arguments)
    return result.structured_content


async def publish(client: Client[Any], plan: dict[str, Any]) -> str:
    """Publish and activate `plan` on a new flow, and return the flow ID."""
    flow = await call(client, "get_or_create_flow", name="release")
    result = await call(client, "publish_plan", flow_id=flow["flow"]["id"], plan=plan)
    assert result["activated"] is True, result
    return flow["flow"]["id"]


async def start(client: Client[Any], plan: dict[str, Any]) -> str:
    flow_id = await publish(client, plan)
    started = await call(
        client, "start_run", flow_id=flow_id, parameters={"topic": "release 1.2"}
    )
    return started["flow_run_id"]


async def wait_for_status(
    client: Client[Any], flow_run_id: str, status: str
) -> dict[str, Any]:
    for _ in range(10):
        run = await call(client, "get_run", flow_run_id=flow_run_id)
        if run["status"] == status:
            return run
    raise AssertionError(f"the run never reached {status!r}: {run!r}")


def node(run: dict[str, Any], node_id: str) -> dict[str, Any]:
    return next(item for item in run["nodes"] if item["node"] == node_id)


def broken_references() -> dict[str, Any]:
    plan = approval_plan()
    plan["edges"][1]["to"]["input"] = "text"
    plan["edges"][2]["from"]["node"] = "writer"
    return plan


def no_decision_enum() -> dict[str, Any]:
    plan = approval_plan()
    form = plan["nodes"]["approve"]["human_input"]["form_schema"]
    form["properties"]["decision"]["enum"] = ["yes", "no"]
    return plan


@pytest.mark.parametrize(
    ("build", "messages"),
    [
        (approval_plan, []),
        (cyclic_plan, ["cycle"]),
        (broken_references, ["input approve.text.", "output writer.drafted."]),
        (no_decision_enum, ["decision"]),
    ],
)
async def test_validate_plan(
    client: Client[Any], build: Callable[[], dict[str, Any]], messages: list[str]
):
    plan = build()

    result = await call(client, "validate_plan", plan=plan)

    assert result["valid"] is (not messages)
    assert len(result["errors"]) == len(messages)
    for error, message in zip(result["errors"], messages, strict=True):
        assert message in error["message"]


async def test_publish_plan_with_and_without_activation(client: Client[Any]):
    created = await call(client, "get_or_create_flow", name="release")
    flow_id = await publish(client, approval_plan())
    first = await call(client, "get_plan", flow_id=flow_id)
    changed = approval_plan()
    changed["nodes"]["draft"]["objective"] = "Write a longer draft."

    result = await call(
        client, "publish_plan", flow_id=flow_id, plan=changed, activate=False
    )
    active = await call(client, "get_plan", flow_id=flow_id)
    versions = await call(client, "list_plan_versions", flow_id=flow_id)

    assert created["created"] is True
    assert flow_id == created["flow"]["id"]
    assert first["active_version"]["plan"] == approval_plan()
    assert result["published"] is True
    assert result["activated"] is False
    assert active["active_version"]["id"] == first["active_version"]["id"]
    assert versions["count"] == 2
    assert versions["active_version_id"] == first["active_version"]["id"]


async def test_a_seeded_flow_has_its_active_version_and_schedule(
    fake_cloud: FakeCloud, client: Client[Any]
):
    flow = fake_cloud.add_flow("release")
    version = fake_cloud.add_version(flow["id"], approval_plan())
    schedule = fake_cloud.add_schedule(
        flow["id"], "weekly", {"type": "cron", "cron": "0 9 * * 1"}
    )

    found = await call(client, "get_or_create_flow", name="release")
    active = await call(client, "get_plan", flow_id=flow["id"])
    schedules = await call(client, "list_schedules", flow_id=flow["id"])

    assert found["created"] is False
    assert found["flow"]["id"] == flow["id"]
    assert active["active_version"]["id"] == version["id"]
    assert schedules == {"schedules": [schedule]}


async def test_a_scripted_run_waits_for_human_input_and_finishes_after_the_answer(
    fake_cloud: FakeCloud, client: Client[Any]
):
    fake_cloud.scripts["draft"] = NodeScript(value={"draft": "Adds dark mode."})
    flow_run_id = await start(client, approval_plan())

    waiting = await wait_for_status(client, flow_run_id, "awaiting_external_progress")
    await call(
        client,
        "submit_human_input",
        flow_run_id=flow_run_id,
        activation_id=node(waiting, "approve")["activation_id"],
        response={"decision": "approved"},
    )
    finished = await wait_for_status(client, flow_run_id, "completed")
    output = await call(
        client, "get_run_output", flow_run_id=flow_run_id, output_name="result"
    )

    assert node(waiting, "publish")["status"] == "pending"
    assert [item["status"] for item in finished["nodes"]] == ["completed"] * 3
    assert output["value"] == {"text": {"draft": "example"}}
    assert fake_cloud.runs[flow_run_id].nodes["draft"].value == {
        "draft": "Adds dark mode."
    }


async def test_a_lost_start_run_response_and_a_retry_with_its_key_start_one_run(
    fake_cloud: FakeCloud, client: Client[Any]
):
    flow_id = await publish(client, approval_plan())
    fake_cloud.lose_response("POST", r"/flows/[^/]+/execution-plan/runs")
    arguments = {
        "flow_id": flow_id,
        "parameters": {"topic": "release 1.2"},
        "idempotency_key": "test-run-1",
    }

    lost = await client.call_tool("start_run", arguments, raise_on_error=False)
    retried = await call(client, "start_run", **arguments)

    assert lost.is_error
    assert "HTTP 504" in error_text(lost)
    assert retried["created"] is False
    assert list(fake_cloud.runs) == [retried["flow_run_id"]]


async def test_a_scripted_expiry_selects_the_expiry_output(
    fake_cloud: FakeCloud, client: Client[Any]
):
    plan = approval_plan()
    approve = plan["nodes"]["approve"]
    approve["outputs"]["expired"] = {"schema": {"type": "object"}}
    approve["human_input"]["deadline"] = {
        "after": "P1D",
        "on_expiry": {"output": "expired", "value": {"reason": "No answer."}},
    }
    fake_cloud.scripts["HumanInputNode"] = NodeScript(expire=True)
    flow_run_id = await start(client, plan)

    finished = await wait_for_status(client, flow_run_id, "completed")

    assert node(finished, "approve")["outputs"] == [
        {"output": "approved", "status": "skipped"},
        {"output": "rejected", "status": "skipped"},
        {"output": "expired", "status": "available"},
    ]


async def test_the_fake_serves_the_single_item_reads_the_skill_uses(
    fake_cloud: FakeCloud, client: Client[Any]
):
    flow = fake_cloud.add_flow("nightly-load")
    deployment = fake_cloud.add_deployment(flow["id"], "nightly")
    schedule = fake_cloud.add_schedule(
        flow["id"], "mornings", {"cron": "0 9 * * *", "timezone": "UTC"}
    )

    found = await call(client, "get_flow", name="nightly-load")
    read = await call(client, "get_deployment", deployment_id=deployment["id"])
    one = await call(
        client, "get_schedule", flow_id=flow["id"], schedule_id=schedule["id"]
    )

    assert found["flow"]["id"] == flow["id"]
    assert read["flow_name"] == "nightly-load"
    assert one["id"] == schedule["id"]
