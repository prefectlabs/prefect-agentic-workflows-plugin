"""Tests for the fake Prefect Cloud API that evaluations run against.

Each test calls the server's tools, so the fake is checked with the same
requests an agent makes during an evaluation.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from fastmcp import Client
from harness_plans import approval_plan, edge, from_node, port
from support import error_text

from evals.fake_cloud import FakeCloud, NodeScript

EXAMPLES_DIR = (
    Path(__file__).resolve().parents[3]
    / "skills"
    / "agentic-workflows"
    / "references"
    / "examples"
)


def example_plan(name: str) -> dict[str, Any]:
    return json.loads((EXAMPLES_DIR / f"{name}.plan.json").read_text())


async def call(client: Client[Any], tool: str, **arguments: Any) -> Any:
    result = await client.call_tool(tool, arguments)
    return result.structured_content


def codes(result: dict[str, Any]) -> list[str]:
    return [error["code"] for error in result["errors"]]


async def publish(client: Client[Any], plan: dict[str, Any]) -> str:
    """Publish and activate `plan` on a new flow, and return the flow ID."""
    flow = await call(client, "get_or_create_flow", name="release")
    result = await call(client, "publish_plan", flow_id=flow["flow"]["id"], plan=plan)
    assert result["activated"] is True, result
    return flow["flow"]["id"]


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


async def test_get_schema_returns_the_current_and_newest_versions(
    client: Client[Any],
):
    current = await call(client, "get_schema")
    newest = await call(client, "get_schema", version="0.2")

    assert current["schema_version"] == "0.1"
    assert current["supported_schema_versions"] == ["0.1", "0.2"]
    assert newest["schema_version"] == "0.2"
    assert newest["schema"]["properties"]["schema_version"]["const"] == "0.2"


@pytest.mark.parametrize(
    "name",
    ["single-agent-with-mcp", "approval-then-deployment", "fan-out-and-join"],
)
async def test_validate_plan_accepts_the_skill_example_plans(
    client: Client[Any], name: str
):
    result = await call(client, "validate_plan", plan=example_plan(name))

    assert result == {"valid": True, "errors": []}


async def test_validate_plan_accepts_the_test_approval_plan(client: Client[Any]):
    result = await call(client, "validate_plan", plan=approval_plan())

    assert result == {"valid": True, "errors": []}


async def test_validate_plan_reports_a_document_shape_error(client: Client[Any]):
    plan = approval_plan()
    del plan["kind"]

    result = await call(client, "validate_plan", plan=plan)

    assert result["valid"] is False
    assert result["errors"][0]["phase"] == "document_shape"
    assert result["errors"][0]["code"] == "required"


async def test_validate_plan_reports_a_cycle(client: Client[Any]):
    plan = approval_plan()
    plan["nodes"]["draft"]["inputs"]["feedback"] = port({})
    plan["edges"].append(
        edge("publish-to-draft", from_node("publish", "published"), "draft", "feedback")
    )

    result = await call(client, "validate_plan", plan=plan)

    assert result["valid"] is False
    assert codes(result) == ["cycle_detected"]
    # Every node is in the cycle, so the first edge between two nodes is named.
    assert result["errors"][0]["path"] == ["edges", 1]


async def test_validate_plan_reports_edges_to_missing_nodes_and_ports(
    client: Client[Any],
):
    plan = approval_plan()
    plan["edges"][1]["to"]["input"] = "text"
    plan["edges"][2]["from"]["node"] = "writer"

    result = await call(client, "validate_plan", plan=plan)

    assert codes(result) == ["missing_target_input", "missing_source_node"]
    assert result["errors"][0]["path"] == ["edges", 1, "to", "input"]
    assert result["errors"][1]["path"] == ["edges", 2, "from", "node"]


async def test_validate_plan_requires_a_decision_enum_on_a_multi_output_approval(
    client: Client[Any],
):
    plan = approval_plan()
    decision = plan["nodes"]["approve"]["human_input"]["form_schema"]["properties"][
        "decision"
    ]
    del decision["enum"]

    result = await call(client, "validate_plan", plan=plan)

    assert codes(result) == ["incompatible_human_input_form_schema"]


async def test_validate_plan_requires_decision_choices_to_match_the_outputs(
    client: Client[Any],
):
    plan = approval_plan()
    decision = plan["nodes"]["approve"]["human_input"]["form_schema"]["properties"][
        "decision"
    ]
    decision["enum"] = ["yes", "no"]

    result = await call(client, "validate_plan", plan=plan)

    assert codes(result) == ["incompatible_human_input_form_schema"]
    assert "do not match" in result["errors"][0]["message"]


async def test_get_or_create_flow_creates_a_flow_once(client: Client[Any]):
    first = await call(client, "get_or_create_flow", name="release", tags=["eval"])
    second = await call(client, "get_or_create_flow", name="release")

    assert first["created"] is True
    assert second["created"] is False
    assert second["flow"] == first["flow"]
    assert first["flow"]["tags"] == ["eval"]


async def test_publish_plan_saves_and_activates_a_version(client: Client[Any]):
    flow_id = await publish(client, approval_plan())

    active = await call(client, "get_plan", flow_id=flow_id)
    versions = await call(client, "list_plan_versions", flow_id=flow_id)

    assert active["active_version"]["plan"] == approval_plan()
    assert versions["count"] == 1
    assert versions["active_version_id"] == active["active_version"]["id"]


async def test_publish_plan_without_activation_keeps_the_active_version(
    client: Client[Any],
):
    flow_id = await publish(client, approval_plan())
    first = await call(client, "get_plan", flow_id=flow_id)
    changed = approval_plan()
    changed["nodes"]["draft"]["objective"] = "Write a longer draft."

    result = await call(
        client, "publish_plan", flow_id=flow_id, plan=changed, activate=False
    )
    active = await call(client, "get_plan", flow_id=flow_id)
    saved = await call(
        client, "get_plan", flow_id=flow_id, version_id=result["version_id"]
    )

    assert result["published"] is True
    assert result["activated"] is False
    assert active["active_version"]["id"] == first["active_version"]["id"]
    assert saved["plan"] == changed


async def test_publish_plan_saves_nothing_for_an_invalid_plan(client: Client[Any]):
    flow = await call(client, "get_or_create_flow", name="release")
    plan = approval_plan()
    plan["edges"][0]["to"]["node"] = "writer"

    result = await call(client, "publish_plan", flow_id=flow["flow"]["id"], plan=plan)
    versions = await call(client, "list_plan_versions", flow_id=flow["flow"]["id"])

    assert result["published"] is False
    assert codes(result) == ["missing_target_node"]
    assert versions["count"] == 0


async def test_activate_plan_version_rolls_back_to_an_older_version(
    client: Client[Any],
):
    flow_id = await publish(client, approval_plan())
    first = await call(client, "get_plan", flow_id=flow_id)
    changed = approval_plan()
    changed["nodes"]["draft"]["objective"] = "Write a longer draft."
    await call(client, "publish_plan", flow_id=flow_id, plan=changed)

    await call(
        client,
        "activate_plan_version",
        flow_id=flow_id,
        version_id=first["active_version"]["id"],
    )
    active = await call(client, "get_plan", flow_id=flow_id)

    assert active["active_version"]["id"] == first["active_version"]["id"]


async def test_a_run_waits_for_human_input_and_finishes_after_the_answer(
    client: Client[Any], fake_cloud: FakeCloud
):
    flow_id = await publish(client, approval_plan())
    started = await call(
        client, "start_run", flow_id=flow_id, parameters={"topic": "release 1.2"}
    )
    flow_run_id = started["flow_run_id"]

    waiting = await wait_for_status(client, flow_run_id, "awaiting_external_progress")
    form = node(waiting, "approve")["wait"]
    await call(
        client,
        "submit_human_input",
        flow_run_id=flow_run_id,
        activation_id=form["activation_id"],
        response={"decision": "approved", "notes": "Ship it."},
    )
    finished = await wait_for_status(client, flow_run_id, "completed")
    output = await call(
        client, "get_run_output", flow_run_id=flow_run_id, output_name="result"
    )

    assert started["created"] is True
    assert form["form_schema"]["properties"]["decision"]["enum"] == [
        "approved",
        "rejected",
    ]
    assert [item["status"] for item in finished["nodes"]] == ["completed"] * 3
    assert output["available"] is True
    assert output["value"] == {"text": {"draft": "example"}}
    assert fake_cloud.runs[flow_run_id].responses == [
        {
            "node": "approve",
            "activation_id": form["activation_id"],
            "response": {"decision": "approved", "notes": "Ship it."},
        }
    ]


async def test_a_rejected_answer_skips_the_nodes_after_the_approval(
    client: Client[Any],
):
    flow_id = await publish(client, approval_plan())
    started = await call(
        client, "start_run", flow_id=flow_id, parameters={"topic": "release 1.2"}
    )
    flow_run_id = started["flow_run_id"]
    waiting = await wait_for_status(client, flow_run_id, "awaiting_external_progress")

    await call(
        client,
        "submit_human_input",
        flow_run_id=flow_run_id,
        activation_id=node(waiting, "approve")["activation_id"],
        response={"decision": "rejected"},
    )
    finished = await wait_for_status(client, flow_run_id, "completed")
    output = await call(
        client, "get_run_output", flow_run_id=flow_run_id, output_name="result"
    )

    assert node(finished, "publish")["status"] == "skipped"
    assert output["available"] is False
    assert output["final"] is True


async def test_submit_human_input_rejects_an_answer_that_does_not_match_the_form(
    client: Client[Any],
):
    flow_id = await publish(client, approval_plan())
    started = await call(
        client, "start_run", flow_id=flow_id, parameters={"topic": "release 1.2"}
    )
    waiting = await wait_for_status(
        client, started["flow_run_id"], "awaiting_external_progress"
    )

    result = await client.call_tool(
        "submit_human_input",
        {
            "flow_run_id": started["flow_run_id"],
            "activation_id": node(waiting, "approve")["activation_id"],
            "response": {"decision": "maybe"},
        },
        raise_on_error=False,
    )

    assert "422" in error_text(result)


async def test_a_scripted_node_selects_its_scripted_output_and_value(
    fake_cloud: FakeCloud, client: Client[Any]
):
    fake_cloud.scripts["draft"] = NodeScript(value={"draft": "Adds dark mode."})
    flow_id = await publish(client, approval_plan())
    started = await call(
        client, "start_run", flow_id=flow_id, parameters={"topic": "release 1.2"}
    )
    waiting = await wait_for_status(
        client, started["flow_run_id"], "awaiting_external_progress"
    )

    output = await call(
        client,
        "get_run_output",
        flow_run_id=started["flow_run_id"],
        output_name="drafted",
        activation_id=node(waiting, "draft")["activation_id"],
    )

    assert output["value"] == {"draft": "Adds dark mode."}


async def test_a_scripted_failure_fails_the_run(
    fake_cloud: FakeCloud, client: Client[Any]
):
    fake_cloud.scripts["draft"] = NodeScript(fail="The agent ran too long.")
    flow_id = await publish(client, approval_plan())
    started = await call(
        client, "start_run", flow_id=flow_id, parameters={"topic": "release 1.2"}
    )

    failed = await wait_for_status(client, started["flow_run_id"], "failed")

    assert node(failed, "draft")["failure"]["message"] == "The agent ran too long."


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
    fake_cloud.scripts["approve"] = NodeScript(expire=True)
    flow_id = await publish(client, plan)
    started = await call(
        client, "start_run", flow_id=flow_id, parameters={"topic": "release 1.2"}
    )

    finished = await wait_for_status(client, started["flow_run_id"], "completed")

    assert node(finished, "approve")["outputs"] == [
        {"output": "approved", "status": "skipped"},
        {"output": "rejected", "status": "skipped"},
        {"output": "expired", "status": "available"},
    ]


async def test_start_run_with_the_same_idempotency_key_starts_one_run(
    fake_cloud: FakeCloud, client: Client[Any]
):
    flow_id = await publish(client, approval_plan())
    arguments = {
        "flow_id": flow_id,
        "parameters": {"topic": "release 1.2"},
        "idempotency_key": "test-run-1",
    }

    first = await call(client, "start_run", **arguments)
    second = await call(client, "start_run", **arguments)

    assert first["created"] is True
    assert second["created"] is False
    assert second["flow_run_id"] == first["flow_run_id"]
    assert len(fake_cloud.runs) == 1


async def test_start_run_rejects_a_missing_required_input(client: Client[Any]):
    flow_id = await publish(client, approval_plan())

    result = await client.call_tool(
        "start_run", {"flow_id": flow_id}, raise_on_error=False
    )

    assert "missing_required_parameter" in error_text(result)


async def test_start_run_fails_on_a_flow_with_no_active_version(
    client: Client[Any],
):
    flow = await call(client, "get_or_create_flow", name="release")

    result = await client.call_tool(
        "start_run", {"flow_id": flow["flow"]["id"]}, raise_on_error=False
    )

    assert "Active execution plan version not found." in error_text(result)


async def test_schedules_can_be_created_read_changed_and_deleted(
    client: Client[Any],
):
    flow_id = await publish(client, approval_plan())

    created = await call(
        client,
        "create_schedule",
        flow_id=flow_id,
        name="weekly",
        schedule={"type": "cron", "cron": "0 9 * * 1", "timezone": "UTC"},
        parameters={"topic": "weekly notes"},
    )
    await call(
        client,
        "update_schedule",
        flow_id=flow_id,
        schedule_id=created["id"],
        active=False,
    )
    changed = await call(
        client, "get_schedule", flow_id=flow_id, schedule_id=created["id"]
    )
    await call(client, "delete_schedule", flow_id=flow_id, schedule_id=created["id"])
    remaining = await call(client, "list_schedules", flow_id=flow_id)

    assert created["schedule"] == {
        "type": "cron",
        "cron": "0 9 * * 1",
        "timezone": "UTC",
    }
    assert changed["active"] is False
    assert remaining == {"schedules": []}


async def test_list_secret_blocks_returns_the_seeded_blocks(
    fake_cloud: FakeCloud, client: Client[Any]
):
    github = fake_cloud.add_secret_block("github-token")
    slack = fake_cloud.add_secret_block("slack-token")

    result = await call(client, "list_secret_blocks")

    assert result == {
        "secret_blocks": [
            {"name": "github-token", "id": github},
            {"name": "slack-token", "id": slack},
        ]
    }


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
    assert active["active_version"]["id"] == version["id"]
    assert schedules == {"schedules": [schedule]}


def join_after_approval_plan() -> dict[str, Any]:
    """Return a plan where `finish` joins an approval branch and a skipped branch.

    `draft` selects `drafted`, which leads to the `approve` form. Its other
    output, `skipped`, leads to `notify`, which never runs. Both `notify` and
    `approve` feed the same input on `finish`.
    """
    plan = approval_plan()
    nodes = plan["nodes"]
    nodes["draft"]["outputs"]["skipped"] = {"schema": {"type": "object"}}
    nodes["notify"] = {
        "kind": "AgentNode",
        "objective": "Say that nothing was drafted.",
        "inputs": {"reason": port({})},
        "outputs": {"notified": {"schema": {"type": "object"}}},
        "orchestration": nodes["publish"]["orchestration"],
    }
    nodes["finish"] = {
        "kind": "AgentNode",
        "objective": "Wrap up.",
        "inputs": {"outcome": port({})},
        "outputs": {"done": {"schema": {"type": "object"}}},
        "orchestration": nodes["publish"]["orchestration"],
    }
    plan["edges"] += [
        edge("skipped-to-notify", from_node("draft", "skipped"), "notify", "reason"),
        edge("notify-to-finish", from_node("notify", "notified"), "finish", "outcome"),
        edge(
            "approved-to-finish", from_node("approve", "approved"), "finish", "outcome"
        ),
    ]
    return plan


async def test_a_join_waits_while_one_of_its_sources_is_waiting_for_an_answer(
    client: Client[Any],
):
    flow_id = await publish(client, join_after_approval_plan())
    started = await call(
        client, "start_run", flow_id=flow_id, parameters={"topic": "release 1.2"}
    )
    flow_run_id = started["flow_run_id"]
    waiting = await wait_for_status(client, flow_run_id, "awaiting_external_progress")
    for _ in range(3):
        waiting = await call(client, "get_run", flow_run_id=flow_run_id)

    assert node(waiting, "notify")["status"] == "skipped"
    assert node(waiting, "finish")["status"] == "pending"

    await call(
        client,
        "submit_human_input",
        flow_run_id=flow_run_id,
        activation_id=node(waiting, "approve")["activation_id"],
        response={"decision": "approved"},
    )
    finished = await wait_for_status(client, flow_run_id, "completed")

    assert node(finished, "finish")["status"] == "completed"


async def test_validate_plan_rejects_a_stdio_mcp_server(client: Client[Any]):
    plan = approval_plan()
    plan["nodes"]["draft"]["mcp"] = {
        "mcpServers": {
            "notes": {"type": "stdio", "command": "docker", "args": ["run", "notes"]}
        }
    }

    result = await call(client, "validate_plan", plan=plan)

    assert result["valid"] is False
    assert codes(result) == ["unsupported_mcp_server_type"]
    assert result["errors"][0]["path"] == [
        "nodes",
        "draft",
        "mcp",
        "mcpServers",
        "notes",
        "type",
    ]


async def test_list_deployments_returns_the_seeded_deployments(
    fake_cloud: FakeCloud, client: Client[Any]
):
    flow = fake_cloud.add_flow("collect-changes")
    fake_cloud.add_deployment(
        flow["id"], "nightly", description="Collects merged changes."
    )

    result = await call(client, "list_deployments")

    assert result == {
        "deployments": [
            {
                "id": result["deployments"][0]["id"],
                "name": "nightly",
                "flow_name": "collect-changes",
                "description": "Collects merged changes.",
            }
        ]
    }


async def test_list_deployments_is_empty_in_a_new_workspace(client: Client[Any]):
    result = await call(client, "list_deployments")

    assert result == {"deployments": []}
