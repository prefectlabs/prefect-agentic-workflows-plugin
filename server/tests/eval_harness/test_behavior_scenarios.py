"""Tests for the checks of the behavioral scenarios.

Each scenario's checks run on two hand-written outcomes: one that passes every
check, and a broken one that should fail exactly the listed checks. A check
that can never fail would make the scenario's pass rate meaningless.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from harness_plans import approval_plan, cyclic_plan

from evals.record import FlowState, Reply, ToolCall, Transcript
from evals.runner import SKILL_DIR
from evals.scenario import Outcome
from evals.scenarios import (
    no_infrastructure,
    rejected_approval,
    scheduled_edit,
    unsupported_loop,
)
from evals.scenarios import release_notes_conversion as release_notes

PREFIX = "eval-abc123-1-"
VALID = {"valid": True, "errors": []}
FEEDBACK_PLAN = SKILL_DIR / "references/examples/customer-feedback-reply.plan.json"


def feedback_plan() -> dict[str, Any]:
    return json.loads(FEEDBACK_PLAN.read_text())


def outcome(
    transcript: Transcript,
    plan: dict[str, Any] | None = None,
    flows: list[FlowState] | None = None,
) -> Outcome:
    plans = {"workflow.plan.json": plan} if plan is not None else {}
    by_name = {flow.name.removeprefix(PREFIX): flow for flow in flows or []}
    return Outcome(transcript, plans, by_name, Path("."), PREFIX)


def saved_flow(name: str, plan: dict[str, Any]) -> FlowState:
    """Return a flow with one saved and active version of the plan."""
    return FlowState(
        id="flow-1",
        name=f"{PREFIX}{name}",
        active_version_id="version-1",
        active_plan=plan,
        version_ids=["version-1"],
    )


def published(plan: dict[str, Any], flow: str, turn: int = 1) -> list[ToolCall]:
    return [
        ToolCall("get_or_create_flow", {"name": f"{PREFIX}{flow}"}, turn=turn),
        ToolCall("validate_plan", {"plan": plan}, VALID, turn=turn),
        ToolCall(
            "publish_plan",
            {"plan": plan},
            {"published": True, "activated": True},
            turn=turn,
        ),
    ]


def rejected(broken: bool) -> Outcome:
    plan = feedback_plan()
    flow = rejected_approval.FLOW_NAME
    calls = published(plan, flow)
    flows = [saved_flow(flow, plan)]
    if broken:
        plan["edges"] = [item for item in plan["edges"] if "rejected" not in item["id"]]
        del plan["outputs"]["category"]
        calls.append(ToolCall("start_run", {}, turn=2))
        flows = []
    return outcome(Transcript(tool_calls=calls), plan, flows)


def with_review_passes(plan: dict[str, Any]) -> dict[str, Any]:
    """Add the two review passes the user chooses in the loop scenario."""
    for number in (1, 2):
        plan["nodes"][f"review_{number}"] = {
            "kind": "AgentNode",
            "objective": f"Review pass {number}: check the draft and revise it.",
            "inputs": {},
            "outputs": {},
        }
    return plan


def mention_action_items(plan: dict[str, Any]) -> dict[str, Any]:
    """Make an agent node end the digest with action items."""
    for node in plan["nodes"].values():
        if node.get("kind") == "AgentNode":
            node["objective"] += " End with a short list of action items."
            break
    return plan


def loop(broken: bool) -> Outcome:
    plan = cyclic_plan() if broken else with_review_passes(approval_plan())
    flow = unsupported_loop.FLOW_NAME
    report = (
        "## Conversion report\n\n| S3 | Review and revise until the reviewer is "
        "happy | A plan can't have a cycle | Two fixed review-and-revise passes |"
    )
    replies = [Reply(1, "reachable-systems", ""), Reply(2, "loop-decision", "")]
    calls = published(plan, flow, turn=3)
    if broken:
        report = "## Conversion report\n\n| S3 | The review loop | Not supported |"
        replies = replies[:1]
        calls.append(ToolCall("start_run", {}, turn=4))
    transcript = Transcript(tool_calls=calls, agent_text=[report], replies=replies)
    return outcome(transcript, plan, [saved_flow(flow, plan)])


def scheduled(broken: bool) -> Outcome:
    plan = mention_action_items(approval_plan())
    schedule = {
        "id": "schedule-1",
        "name": scheduled_edit.SCHEDULE_NAME,
        "active": False,
        # Cloud adds the fields it defaults.
        "schedule": {**scheduled_edit.SCHEDULE, "day_or": True},
        "parameters": scheduled_edit.SCHEDULE_PARAMETERS,
        "next_scheduled_time": None,
    }
    flow = FlowState(
        id="flow-1",
        name=f"{PREFIX}{scheduled_edit.FLOW_NAME}",
        active_version_id="version-2",
        active_plan=plan,
        version_ids=["version-1", "version-2"],
        schedules=[schedule],
    )
    question = "Activate it? The schedule `monday-digest` will run the new version."
    list_turn, publish_turn = 3, 4
    calls = [ToolCall("validate_plan", {"plan": plan}, VALID, turn=3)]
    if broken:
        question = "The flow has an active version. Activate?"
        list_turn, publish_turn = 4, 3
        calls.append(ToolCall("update_schedule", {"active": True}, turn=4))
        calls.append(ToolCall("start_run", {}, turn=5))
        flow = FlowState(
            id=flow.id,
            name=flow.name,
            active_version_id="version-1",
            active_plan=scheduled_edit.PLAN,
            version_ids=flow.version_ids,
            schedules=[{**schedule, "active": True}],
        )
    calls += [
        ToolCall("list_schedules", {}, turn=list_turn),
        ToolCall("publish_plan", {"plan": plan}, turn=publish_turn),
    ]
    transcript = Transcript(
        tool_calls=calls,
        replies=[Reply(1, "design-approval", ""), Reply(3, "promotion-approval", "")],
        turn_results=["Summary.", "Validated.", question, "Done."],
    )
    return outcome(transcript, plan, [flow])


def no_infra(broken: bool) -> Outcome:
    question = "Before we design it: do you have a remote MCP server for Zendesk?"
    note = "No step can use Zendesk or Slack. You could paste the tickets in."
    answer_turn = 1
    calls = [
        ToolCall("list_secret_blocks", {}, turn=1),
        ToolCall("list_deployments", {}, turn=1),
    ]
    if broken:
        question += " Also, what time should it run each morning?"
        note = "Zendesk and Slack aren't reachable, so this can't be built."
        answer_turn = 2
        calls.append(ToolCall("validate_plan", {"plan": {}}, VALID, turn=2))
    transcript = Transcript(
        tool_calls=calls,
        replies=[Reply(answer_turn, "no-remote-servers", "")],
        turn_results=[question, note, note][: answer_turn + 1],
    )
    return outcome(transcript)


def release(broken: bool) -> Outcome:
    plan = approval_plan()
    flow = release_notes.FLOW_NAME
    report = (
        "## Conversion report: release-notes\n\n"
        "| 1 | Runs `scripts/collect_changes.py` | S1 | No shell |\n"
        "| 2 | stdio MCP server | Setup | Remote only |\n"
        "| 3 | Repeat-until-done loop | S4 | No cycles |\n"
        "| 4 | Human approval | S5 | Converts as a human-input node |\n"
    )
    approvals = 1
    calls = published(plan, flow)
    if broken:
        plan["nodes"]["approve"]["kind"] = "AgentNode"
        report = report.replace("stdio MCP server", "")
        approvals = 2
    transcript = Transcript(
        tool_calls=calls,
        agent_text=[report],
        replies=[Reply(1, "design-approval", "")] * approvals,
    )
    return outcome(transcript, plan, [] if broken else [saved_flow(flow, plan)])


SCENARIOS = {
    "rejected_approval": (
        rejected_approval,
        rejected,
        [
            "the rejected output leads to an agent node that revises the draft",
            "plan outputs include ['category', 'reply']",
            "the flow 'customer-feedback-reply' has a saved plan version",
        ],
    ),
    "unsupported_loop": (
        unsupported_loop,
        loop,
        [
            "the conversion report flags the loop and proposes a substitute",
            "the user decided on the loop",
            "nothing published before the user decided",
            "plan has no cycle",
            "the plan has two review passes",
        ],
    ),
    "scheduled_edit": (
        scheduled_edit,
        scheduled,
        [
            "list_schedules called before asking to activate",
            "the activation question names the schedule",
            "activation only after the user's yes",
            "the new version is active",
            "the active plan adds the action items",
            "update_schedule never called",
            "the schedule is unchanged",
        ],
    ),
    "no_infrastructure": (
        no_infrastructure,
        no_infra,
        [
            "the agent's first question is the infrastructure check",
            "the first question asks no design question",
            "the agent explains the limit and offers a version without the tools",
            "validate_plan never called",
        ],
    ),
    "release_notes_conversion": (
        release_notes,
        release,
        [
            "conversion report lists every unsupported part",
            "design approved once",
            "plan has at least 1 HumanInputNode",
            "the flow 'release-notes' has a saved plan version",
        ],
    ),
}


@pytest.mark.parametrize("broken", [False, True], ids=["expected", "broken"])
@pytest.mark.parametrize("scenario", SCENARIOS)
def test_scenario_checks(scenario: str, broken: bool):
    module, build, broken_checks = SCENARIOS[scenario]

    failed = [check.name for check in module.checks(build(broken)) if not check.passed]

    assert failed == (broken_checks if broken else [])


def test_two_review_passes_can_live_in_one_node():
    plan = approval_plan()
    for node in plan["nodes"].values():
        if node.get("kind") == "AgentNode":
            node["objective"] = (
                "Draft the post, then review and revise it in two passes."
            )
            break

    check = unsupported_loop.check_two_review_passes(outcome(Transcript(), plan))

    assert check.passed, check.detail
