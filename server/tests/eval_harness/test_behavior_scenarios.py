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
from evals.scenario import Outcome, next_rule
from evals.scenarios import (
    mcp_connection_setup,
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
    calls = published(plan, flow, turn=2)
    flows = [saved_flow(flow, plan)]
    replies = [Reply(1, "design-approval", "")]
    if broken:
        plan["edges"] = [item for item in plan["edges"] if "rejected" not in item["id"]]
        del plan["outputs"]["category"]
        calls.append(ToolCall("start_run", {}, turn=2))
        flows = []
        replies = []
    return outcome(Transcript(tool_calls=calls, replies=replies), plan, flows)


LINEAR_ID = "0199c3a2-0000-7000-8000-000000000001"
STRIPE_ID = "0199c3a2-0000-7000-8000-000000000002"


def created(connection_id: str, name: str, url: str, **arguments: Any) -> ToolCall:
    """Return a `create_mcp_connection` call for a client that can't open links."""
    return ToolCall(
        "create_mcp_connection",
        {"name": name, "url": url, **arguments},
        {
            "setup": "client_cannot_open_links",
            "connection": {
                "id": connection_id,
                "name": name,
                "setup_url": f"https://app.prefect.cloud/connections/{connection_id}/setup",
            },
        },
        turn=1,
    )


def connection_plan(*servers: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": "ExecutionPlan",
        "nodes": {
            "digest": {
                "kind": "AgentNode",
                "mcp": {"mcpServers": {f"server{n}": s for n, s in enumerate(servers)}},
            }
        },
    }


def connections(broken: bool) -> Outcome:
    account = f"{PREFIX}acme"
    linear, stripe = mcp_connection_setup.LINEAR_URL, mcp_connection_setup.STRIPE_URL
    linear_call = created(LINEAR_ID, f"Linear ({account})", linear, oauth={})
    stripe_call = created(
        STRIPE_ID,
        f"Stripe ({account})",
        stripe,
        headers=[{"name": "Authorization", "value_prefix": "Bearer "}],
    )
    plan = connection_plan(
        {"type": "connection", "connection_id": LINEAR_ID},
        {"type": "connection", "connection_id": STRIPE_ID},
    )
    discovers = [
        ToolCall("discover_mcp_server", {"url": linear}, turn=1),
        ToolCall("discover_mcp_server", {"url": stripe}, turn=1),
    ]
    links = [linear_call.result["connection"]["setup_url"]]
    links.append(stripe_call.result["connection"]["setup_url"])
    replies = [Reply(1, "design-approval", "")]
    if broken:
        discovers = discovers[:1]
        linear_call.arguments.pop("oauth")
        linear_call.arguments["headers"] = [{"name": "Authorization"}]
        stripe_call.arguments["oauth"] = {}
        # Each name is the other server's provider.
        linear_call.arguments["name"] = f"Stripe ({account})"
        stripe_call.arguments["name"] = f"Linear ({account})"
        # Both creates are refused, so no setup link can count as sent.
        linear_call.is_error = stripe_call.is_error = True
        plan = connection_plan(
            {"type": "connection", "connection_id": LINEAR_ID},
            {"type": "http", "url": stripe},
        )
        replies.append(Reply(1, "secret-asked", ""))
    calls = [*discovers, linear_call, stripe_call]
    calls += published(plan, mcp_connection_setup.FLOW_NAME, turn=2)
    transcript = Transcript(
        tool_calls=calls,
        agent_text=[f"Finish setup at {link}" for link in links],
        replies=replies,
    )
    flows = [saved_flow(mcp_connection_setup.FLOW_NAME, plan)]
    return outcome(transcript, plan, flows)


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
    # Plain prose with no report heading still flags the loop.
    report = (
        "The skill repeats review and revise until the reviewer is happy. A plan "
        "can't loop, so I propose two fixed review-and-revise passes. Which do "
        "you want?"
    )
    replies = [Reply(1, "reachable-systems", ""), Reply(2, "loop-decision", "")]
    calls = published(plan, flow, turn=3)
    if broken:
        report = "The skill has no parts that need a decision."
        replies = replies[:1]
        calls.append(ToolCall("start_run", {}, turn=4))
    transcript = Transcript(
        tool_calls=calls,
        agent_text=[report],
        replies=replies,
        turn_results=["Which business tools does this workflow need?", report],
    )
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
    calls = published(plan, flow, turn=2)
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


@pytest.mark.parametrize("approval_turn", [None, 4])
def test_conversion_decisions_do_not_authorize_validation(approval_turn: int | None):
    result = release(False)
    for call in result.transcript.tool_calls:
        call.turn = 3
    result.transcript.replies = [Reply(2, "conversion-decisions", "")]
    if approval_turn is not None:
        result.transcript.replies.append(Reply(approval_turn, "design-approval", ""))

    by_name = {check.name: check for check in release_notes.checks(result)}

    assert not by_name["nothing validated before the design approval"].passed
    assert by_name["design approved once"].passed is (approval_turn is not None)


SCENARIOS = {
    "rejected_approval": (
        rejected_approval,
        rejected,
        [
            "the rejected output leads to an agent node that revises the draft",
            "plan outputs include ['category', 'reply']",
            "the flow 'customer-feedback-reply' has a saved plan version",
            "the flow 'customer-feedback-reply' runs its newest version",
            "nothing validated before the design approval",
        ],
    ),
    "unsupported_loop": (
        unsupported_loop,
        loop,
        [
            "the agent flags the loop and proposes a substitute before the decision",
            "the user decided on the loop",
            "nothing published before the user decided",
            "plan has no cycle",
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
    "mcp_connection_setup": (
        mcp_connection_setup,
        connections,
        [
            "discover_mcp_server called for https://mcp.stripe.com before its "
            "connection is created",
            "the Linear connection uses oauth",
            "the Stripe connection uses an Authorization header with the Bearer prefix",
            "each connection is named after its provider and the account",
            "the agent never asks for the API key",
            "the agent sends each setup link the client didn't open",
            "the plan references both connections and no inline server for them",
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
            "the flow 'release-notes' runs its newest version",
        ],
    ),
}


@pytest.mark.parametrize(
    "message",
    [
        "What is your Stripe API key?",
        "Can you paste the restricted key here?",
        "Please share the token so I can add it?",
    ],
)
def test_mcp_connection_setup_hears_a_request_for_the_key(message: str):
    rule = next_rule(mcp_connection_setup.USER, message, Transcript())

    assert rule is not None
    assert rule.label == "secret-asked"


@pytest.mark.parametrize("broken", [False, True], ids=["expected", "broken"])
@pytest.mark.parametrize("scenario", SCENARIOS)
def test_scenario_checks(scenario: str, broken: bool):
    module, build, broken_checks = SCENARIOS[scenario]

    failed = [check.name for check in module.checks(build(broken)) if not check.passed]

    assert failed == (broken_checks if broken else [])


def test_mcp_connection_setup_checks_fail_on_headers_sent_as_a_string():
    result = connections(broken=False)
    for call in result.transcript.tool_calls:
        if call.name == "create_mcp_connection" and "headers" in call.arguments:
            call.arguments["headers"] = json.dumps(call.arguments["headers"])

    failed = [
        check.name for check in mcp_connection_setup.checks(result) if not check.passed
    ]

    assert failed == [
        "the Stripe connection uses an Authorization header with the Bearer prefix"
    ]
