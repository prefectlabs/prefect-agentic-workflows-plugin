"""Tests for the checks of the behavioral scenarios.

Each scenario's checks run on two hand-written outcomes: one that passes every
check, and a broken one that should fail exactly the listed checks. A check
that can never fail would make the scenario's pass rate meaningless.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from harness_plans import approval_plan, cyclic_plan, from_node

from evals.fake_cloud import FakeCloud, NodeScript
from evals.record import Reply, ToolCall, Transcript
from evals.runner import SKILL_DIR
from evals.scenario import Outcome
from evals.scenarios import test_expired_approval as expired_approval
from evals.scenarios import test_no_infrastructure as no_infrastructure
from evals.scenarios import test_rejected_approval as rejected_approval
from evals.scenarios import test_release_notes_conversion as release_notes
from evals.scenarios import test_run_retry as run_retry
from evals.scenarios import test_scheduled_edit as scheduled_edit
from evals.scenarios import test_unsupported_loop as unsupported_loop

API_URL = "http://127.0.0.1/api"
VALID = {"valid": True, "errors": []}
FEEDBACK_PLAN = SKILL_DIR / "references/examples/customer-feedback-reply.plan.json"


def feedback_plan() -> dict[str, Any]:
    return json.loads(FEEDBACK_PLAN.read_text())


def outcome(
    transcript: Transcript,
    plan: dict[str, Any] | None = None,
    fake: FakeCloud | None = None,
) -> Outcome:
    plans = {"workflow.plan.json": plan} if plan is not None else {}
    return Outcome(transcript, plans, fake or FakeCloud(API_URL), Path("."))


def published(plan: dict[str, Any], turn: int = 1) -> list[ToolCall]:
    return [
        ToolCall("validate_plan", {"plan": plan}, VALID, turn=turn),
        ToolCall("publish_plan", {"plan": plan}, {"activated": True}, turn=turn),
    ]


def run_plan(
    plan: dict[str, Any],
    parameters: dict[str, Any],
    answer: dict[str, Any] | None = None,
    scripts: dict[str, NodeScript] | None = None,
) -> FakeCloud:
    """Run a plan in a new fake until it can't move, answering each form."""
    fake = FakeCloud(API_URL, scripts=scripts)
    flow = fake.add_flow("flow")
    fake.add_version(flow["id"], plan)
    response = fake.start_run(flow_id=flow["id"], body={"parameters": parameters})
    run = fake.runs[response.json()["id"]]
    for _ in range(20):
        run.advance()
        for state in run.nodes.values():
            if state.status == "suspended" and answer is not None:
                run.answer(str(state.activation_id), answer)
    return fake


def rejected(broken: bool) -> Outcome:
    rejection = {"decision": "rejected", "notes": rejected_approval.NOTES}
    plan = feedback_plan()
    fake = run_plan(plan, {"feedback": rejected_approval.FEEDBACK}, rejection)
    response = rejection
    if broken:
        plan["edges"] = [item for item in plan["edges"] if "rejected" not in item["id"]]
        response = {"decision": "rejected", "notes": "Apologize and mention the fix."}
        fake = FakeCloud(API_URL)
    calls = [
        *published(plan),
        ToolCall("start_run", {}, turn=2),
        ToolCall("submit_human_input", {"response": response}, turn=3),
    ]
    return outcome(Transcript(tool_calls=calls), plan, fake)


def expiry_plan() -> dict[str, Any]:
    """Return the approval plan with a deadline whose output is a plan output."""
    plan = approval_plan()
    approve = plan["nodes"]["approve"]
    approve["outputs"]["expired"] = {"schema": {"type": "object"}}
    approve["human_input"]["deadline"] = {
        "after": "P1D",
        "on_expiry": {"output": "expired", "value": {"reviewed": False}},
    }
    source = from_node("approve", "expired")
    plan["outputs"]["unreviewed"] = {
        "fields": {"status": {"schema": {}, "source": source}}
    }
    return plan


def expired(broken: bool) -> Outcome:
    plan, run = expiry_plan(), expiry_plan()
    reads = 3
    final = "Nobody answered before the deadline, so the run took the expiry path."
    calls = published(plan)
    if broken:
        del plan["outputs"]["unreviewed"]
        del run["nodes"]["approve"]["human_input"]["deadline"]["on_expiry"]
        reads = expired_approval.MOST_RUN_READS + 1
        final = "The run finished. The draft is ready."
        calls.append(ToolCall("submit_human_input", {"response": {}}, turn=3))
    calls += [ToolCall("get_run", {}, turn=2) for _ in range(reads)]
    scripts = {"HumanInputNode": NodeScript(expire=True)}
    fake = run_plan(run, {"topic": "weekly notes"}, scripts=scripts)
    return outcome(Transcript(tool_calls=calls, turn_results=[final]), plan, fake)


def loop(broken: bool) -> Outcome:
    plan = cyclic_plan() if broken else approval_plan()
    report = (
        "## Conversion report\n\n| S3 | Review and revise until the reviewer is "
        "happy | A plan can't have a cycle | Two fixed review-and-revise passes |"
    )
    replies = [Reply(1, "reachable-systems", ""), Reply(2, "loop-decision", "")]
    if broken:
        report = "## Conversion report\n\n| S3 | The review loop | Not supported |"
        replies = replies[:1]
    transcript = Transcript(
        tool_calls=published(plan, turn=3), agent_text=[report], replies=replies
    )
    return outcome(transcript, plan)


def scheduled(broken: bool) -> Outcome:
    plan = approval_plan()
    fake = FakeCloud(API_URL)
    scheduled_edit.setup(fake)
    flow_id = scheduled_edit.seeded_flow_id(fake) or ""
    fake.add_version(flow_id, plan, activate=not broken)
    question = "Activate it? The schedule `monday-digest` will run the new version."
    list_turn, publish_turn = 3, 4
    calls = [ToolCall("validate_plan", {"plan": plan}, VALID, turn=3)]
    if broken:
        question = "The flow has an active version. Activate?"
        list_turn, publish_turn = 4, 3
        calls.append(ToolCall("update_schedule", {"active": False}, turn=4))
        for schedule in fake.schedules[flow_id].values():
            schedule["active"] = False
    calls += [
        ToolCall("list_schedules", {}, turn=list_turn),
        ToolCall("publish_plan", {"plan": plan}, turn=publish_turn),
    ]
    transcript = Transcript(
        tool_calls=calls,
        replies=[Reply(1, "design-approval", ""), Reply(3, "promotion-approval", "")],
        turn_results=["Summary.", "Validated.", question, "Done."],
    )
    return outcome(transcript, plan, fake)


def retry(broken: bool) -> Outcome:
    fake = FakeCloud(API_URL)
    flow = fake.add_flow("customer-feedback-reply")
    fake.add_version(flow["id"], feedback_plan())
    for key in ["run-1", "run-2"] if broken else ["run-1"]:
        fake.start_run(flow_id=flow["id"], body={"idempotency_key": key})
    keys = ["key-1", "key-2" if broken else "key-1"]
    calls = [
        ToolCall("start_run", {"parameters": {}, "idempotency_key": key}, turn=2)
        for key in keys
    ]
    return outcome(Transcript(tool_calls=calls), fake=fake)


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
    report = (
        "## Conversion report: release-notes\n\n"
        "| 1 | Runs `scripts/collect_changes.py` | S1 | No shell |\n"
        "| 2 | stdio MCP server | Setup | Remote only |\n"
        "| 3 | Repeat-until-done loop | S4 | No cycles |\n"
        "| 4 | Human approval | S5 | Converts as a human-input node |\n"
    )
    approvals = 1
    if broken:
        plan["nodes"]["approve"]["kind"] = "AgentNode"
        report = report.replace("stdio MCP server", "")
        approvals = 2
    transcript = Transcript(
        tool_calls=published(plan),
        agent_text=[report],
        replies=[Reply(1, "design-approval", "")] * approvals,
    )
    return outcome(transcript, plan)


SCENARIOS = {
    "rejected_approval": (
        rejected_approval,
        rejected,
        [
            "the rejected output leads to an agent node that revises the draft",
            "submit_human_input got the user's decision and notes unchanged",
            "a run finished with its reply output",
        ],
    ),
    "expired_approval": (
        expired_approval,
        expired,
        [
            "the approval's on_expiry output leads to a node or a plan output",
            "submit_human_input never called",
            "a run finished on the expiry path",
            f"get_run called at most {expired_approval.MOST_RUN_READS} times",
            "the final message reports the expiry",
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
            "update_schedule never called",
            "the schedule is unchanged",
        ],
    ),
    "run_retry": (
        run_retry,
        retry,
        [
            "the second start_run reused the first one's idempotency key",
            "only one run exists",
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
        ],
    ),
}


@pytest.mark.parametrize("broken", [False, True], ids=["expected", "broken"])
@pytest.mark.parametrize("scenario", SCENARIOS)
def test_scenario_checks(scenario: str, broken: bool):
    module, build, broken_checks = SCENARIOS[scenario]

    failed = [check.name for check in module.checks(build(broken)) if not check.passed]

    assert failed == (broken_checks if broken else [])
