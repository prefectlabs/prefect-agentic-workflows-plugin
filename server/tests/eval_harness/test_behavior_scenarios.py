"""Tests for the checks of the behavioral scenarios.

Each test runs a scenario's checks on a hand-written outcome: one that passes,
and one for each way the agent can fail. A check that can never fail would
make the scenario's pass rate meaningless.
"""

import json
from pathlib import Path
from typing import Any

from harness_plans import approval_plan, edge, from_node

from evals.fake_cloud import FakeCloud, FakeRun, NodeScript
from evals.record import Reply, ToolCall, Transcript
from evals.runner import SKILL_DIR
from evals.scenario import Outcome, Scenario
from evals.scenarios import (
    SCENARIOS,
    expired_approval,
    no_infrastructure,
    rejected_approval,
    scheduled_edit,
)

VALID = {"valid": True, "errors": []}
FEEDBACK_PLAN = json.loads(
    (
        SKILL_DIR / "references" / "examples" / "customer-feedback-reply.plan.json"
    ).read_text()
)


def feedback_plan() -> dict[str, Any]:
    return json.loads(json.dumps(FEEDBACK_PLAN))


def outcome(
    *,
    calls: list[ToolCall] | None = None,
    plan: dict[str, Any] | None = None,
    fake: FakeCloud | None = None,
    replies: list[Reply] | None = None,
    turn_results: list[str] | None = None,
    agent_text: list[str] | None = None,
) -> Outcome:
    transcript = Transcript(
        tool_calls=calls or [],
        replies=replies or [],
        turn_results=turn_results or [],
        agent_text=agent_text or [],
    )
    return Outcome(
        transcript,
        {"workflow.plan.json": plan} if plan is not None else {},
        fake or FakeCloud("http://127.0.0.1/api"),
        Path("."),
    )


def failed_checks(scenario: Scenario, result: Outcome) -> list[str]:
    return [check.name for check in scenario.checks(result) if not check.passed]


def published(plan: dict[str, Any], turn: int = 1) -> list[ToolCall]:
    return [
        ToolCall("validate_plan", {"plan": plan}, VALID, turn=turn),
        ToolCall("get_or_create_flow", {"name": "flow"}, turn=turn),
        ToolCall("publish_plan", {"plan": plan}, {"activated": True}, turn=turn),
    ]


def run_plan(
    plan: dict[str, Any],
    parameters: dict[str, Any],
    *,
    answer: dict[str, Any] | None = None,
    scripts: dict[str, NodeScript] | None = None,
) -> FakeCloud:
    """Run a plan in a new fake until it can't move, answering each form."""
    fake = FakeCloud("http://127.0.0.1/api", scripts=scripts)
    flow = fake.add_flow("flow")
    fake.add_version(flow["id"], plan)
    response = fake.start_run(flow_id=flow["id"], body={"parameters": parameters})
    run: FakeRun = fake.runs[response.json()["id"]]
    for _ in range(20):
        run.advance()
        for state in run.nodes.values():
            if state.status == "suspended" and answer is not None:
                run.answer(str(state.activation_id), answer)
    return fake


# Rejected approval.

REJECTED = SCENARIOS["rejected-approval"]
REJECTION = {"decision": "rejected", "notes": rejected_approval.NOTES}


def rejected_outcome(
    *,
    plan: dict[str, Any] | None = None,
    response: dict[str, Any] | None = None,
    fake: FakeCloud | None = None,
) -> Outcome:
    plan = plan or feedback_plan()
    response = response or REJECTION
    return outcome(
        plan=plan,
        calls=[
            *published(plan),
            ToolCall("start_run", {}, turn=2),
            ToolCall("submit_human_input", {"response": response}, turn=3),
        ],
        fake=fake
        or run_plan(
            feedback_plan(), {"feedback": rejected_approval.FEEDBACK}, answer=REJECTION
        ),
    )


def test_rejected_approval_passes_for_the_expected_outcome():
    assert failed_checks(REJECTED, rejected_outcome()) == []


def test_rejected_approval_fails_when_the_rejection_leads_nowhere():
    plan = feedback_plan()
    plan["edges"] = [
        edge_spec for edge_spec in plan["edges"] if "rejected" not in edge_spec["id"]
    ]

    assert failed_checks(REJECTED, rejected_outcome(plan=plan)) == [
        "the rejected output leads to an agent node that revises the draft"
    ]


def test_rejected_approval_fails_when_the_agent_rewords_the_notes():
    response = {"decision": "rejected", "notes": "Apologize and mention the fix."}

    assert failed_checks(REJECTED, rejected_outcome(response=response)) == [
        "submit_human_input got the user's decision and notes unchanged"
    ]


def test_rejected_approval_fails_when_the_run_never_finishes():
    fake = FakeCloud("http://127.0.0.1/api")

    assert failed_checks(REJECTED, rejected_outcome(fake=fake)) == [
        "a run finished with its reply output"
    ]


# Expired approval.

EXPIRED = SCENARIOS["expired-approval"]
EXPIRY_REPORT = "Nobody answered before the deadline, so the run took the expiry path."


def expiry_plan() -> dict[str, Any]:
    """Return the approval plan with a deadline whose output is a plan output."""
    plan = approval_plan()
    approve = plan["nodes"]["approve"]
    approve["outputs"]["expired"] = {"schema": {"type": "object"}}
    approve["human_input"]["deadline"] = {
        "after": "P1D",
        "on_expiry": {"output": "expired", "value": {"reviewed": False}},
    }
    plan["outputs"]["unreviewed"] = {
        "fields": {
            "status": {
                "schema": {"type": "object"},
                "source": from_node("approve", "expired"),
            }
        }
    }
    return plan


def expired_outcome(
    *,
    plan: dict[str, Any] | None = None,
    extra_calls: list[ToolCall] | None = None,
    reads: int = 3,
    final: str = EXPIRY_REPORT,
) -> Outcome:
    plan = plan or expiry_plan()
    fake = run_plan(
        expiry_plan(),
        {"topic": "weekly notes"},
        scripts={"HumanInputNode": NodeScript(expire=True)},
    )
    return outcome(
        plan=plan,
        calls=[
            *published(plan),
            ToolCall("start_run", {}, turn=2),
            *[ToolCall("get_run", {}, turn=2) for _ in range(reads)],
            *(extra_calls or []),
        ],
        fake=fake,
        turn_results=["Published.", final],
    )


def test_expired_approval_passes_for_the_expected_outcome():
    assert failed_checks(EXPIRED, expired_outcome()) == []


def test_expired_approval_accepts_an_expiry_output_that_leads_to_a_node():
    plan = expiry_plan()
    del plan["outputs"]["unreviewed"]
    plan["edges"].append(
        edge("expired-to-publish", from_node("approve", "expired"), "publish", "draft")
    )

    assert expired_approval.expiry_destinations(plan) == {"approve": ["publish"]}


def test_expired_approval_fails_when_the_expiry_output_leads_nowhere():
    plan = expiry_plan()
    del plan["outputs"]["unreviewed"]

    assert failed_checks(EXPIRED, expired_outcome(plan=plan)) == [
        "the approval's on_expiry output leads to a node or a plan output"
    ]


def test_expired_approval_fails_when_the_agent_answers_the_form():
    answered = ToolCall("submit_human_input", {"response": {}}, turn=3)

    assert failed_checks(EXPIRED, expired_outcome(extra_calls=[answered])) == [
        "submit_human_input never called"
    ]


def test_expired_approval_fails_when_the_agent_keeps_polling():
    reads = expired_approval.MOST_RUN_READS + 1

    assert failed_checks(EXPIRED, expired_outcome(reads=reads)) == [
        f"get_run called at most {expired_approval.MOST_RUN_READS} times"
    ]


def test_expired_approval_fails_when_the_final_message_skips_the_expiry():
    result = expired_outcome(final="The run finished. The draft is ready.")

    assert failed_checks(EXPIRED, result) == ["the final message reports the expiry"]


def test_expired_approval_fails_when_the_run_fails_on_expiry():
    plan = approval_plan()
    plan["nodes"]["approve"]["human_input"]["deadline"] = {"after": "P1D"}
    fake = run_plan(
        plan,
        {"topic": "weekly notes"},
        scripts={"HumanInputNode": NodeScript(expire=True)},
    )
    result = expired_outcome()
    result.fake = fake

    assert failed_checks(EXPIRED, result) == ["a run finished on the expiry path"]


# Unsupported loop.

LOOP = SCENARIOS["unsupported-loop"]
LOOP_REPORT = """\
## Conversion report: post-review

| S3 | Repeat review and revise until the reviewer is happy | A plan can't have a
cycle | Two fixed review-and-revise passes, or a human checkpoint |
"""


def loop_outcome(
    *,
    report: str = LOOP_REPORT,
    publish_turn: int = 3,
    decided: bool = True,
    plan: dict[str, Any] | None = None,
) -> Outcome:
    plan = plan or approval_plan()
    replies = [Reply(1, "reachable-systems", "")]
    if decided:
        replies.append(Reply(2, "loop-decision", ""))
    return outcome(
        plan=plan,
        calls=published(plan, turn=publish_turn),
        replies=replies,
        agent_text=["I'll read the skill.", report],
    )


def test_unsupported_loop_passes_for_the_expected_outcome():
    assert failed_checks(LOOP, loop_outcome()) == []


def test_unsupported_loop_fails_when_the_report_proposes_no_substitute():
    report = "## Conversion report\n\n| S3 | The review loop | Not supported |"

    assert failed_checks(LOOP, loop_outcome(report=report)) == [
        "the conversion report flags the loop and proposes a substitute"
    ]


def test_unsupported_loop_fails_when_the_agent_publishes_before_the_decision():
    assert failed_checks(LOOP, loop_outcome(publish_turn=2)) == [
        "nothing published before the user decided"
    ]


def test_unsupported_loop_fails_when_the_user_never_decides():
    assert failed_checks(LOOP, loop_outcome(decided=False)) == [
        "the user decided on the loop",
        "nothing published before the user decided",
    ]


def test_unsupported_loop_fails_for_a_plan_with_a_cycle():
    plan = approval_plan()
    plan["nodes"]["draft"]["inputs"]["notes"] = {"schema": {}}
    plan["edges"].append(
        edge("rejected-to-draft", from_node("approve", "rejected"), "draft", "notes")
    )

    assert failed_checks(LOOP, loop_outcome(plan=plan)) == ["plan has no cycle"]


# Editing a scheduled workflow.

SCHEDULED = SCENARIOS["scheduled-edit"]
PROMOTION_QUESTION = (
    "The flow has an active version, and the schedule `monday-digest` will run "
    "the new version. Activate it?"
)


def scheduled_fake(*, activate: bool = True) -> FakeCloud:
    fake = FakeCloud("http://127.0.0.1/api")
    scheduled_edit.setup(fake)
    flow_id = scheduled_edit.seeded_flow_id(fake) or ""
    fake.add_version(flow_id, approval_plan(), activate=activate)
    return fake


def scheduled_outcome(
    *,
    fake: FakeCloud | None = None,
    list_turn: int = 3,
    publish_turn: int = 4,
    question: str = PROMOTION_QUESTION,
    extra_calls: list[ToolCall] | None = None,
) -> Outcome:
    plan = approval_plan()
    return outcome(
        plan=plan,
        calls=[
            ToolCall("get_plan", {}, turn=1),
            ToolCall("validate_plan", {"plan": plan}, VALID, turn=3),
            ToolCall("list_schedules", {}, turn=list_turn),
            ToolCall("publish_plan", {"plan": plan}, turn=publish_turn),
            *(extra_calls or []),
        ],
        fake=fake or scheduled_fake(),
        replies=[
            Reply(1, "design-approval", ""),
            Reply(3, "promotion-approval", ""),
        ],
        turn_results=["Summary.", "Validated.", question, "Done."],
    )


def test_scheduled_edit_passes_for_the_expected_outcome():
    assert failed_checks(SCHEDULED, scheduled_outcome()) == []


def test_scheduled_edit_fails_when_the_agent_activates_before_the_yes():
    assert failed_checks(SCHEDULED, scheduled_outcome(publish_turn=3)) == [
        "activation only after the user's yes"
    ]


def test_scheduled_edit_fails_when_schedules_are_listed_after_the_question():
    assert failed_checks(SCHEDULED, scheduled_outcome(list_turn=4)) == [
        "list_schedules called before asking to activate"
    ]


def test_scheduled_edit_fails_when_the_question_does_not_name_the_schedule():
    result = scheduled_outcome(question="The flow has an active version. Activate?")

    assert failed_checks(SCHEDULED, result) == [
        "the activation question names the schedule"
    ]


def test_scheduled_edit_fails_when_the_new_version_is_not_active():
    assert failed_checks(
        SCHEDULED, scheduled_outcome(fake=scheduled_fake(activate=False))
    ) == ["the new version is active"]


def test_scheduled_edit_fails_when_the_agent_changes_the_schedule():
    fake = scheduled_fake()
    flow_id = scheduled_edit.seeded_flow_id(fake) or ""
    for schedule in fake.schedules[flow_id].values():
        schedule["active"] = False
    update = ToolCall("update_schedule", {"active": False}, turn=4)

    assert failed_checks(
        SCHEDULED, scheduled_outcome(fake=fake, extra_calls=[update])
    ) == ["update_schedule never called", "the schedule is unchanged"]


# Run retry.

RETRY = SCENARIOS["run-retry"]
PARAMETERS = {"feedback": "The export button is hard to find."}


def retry_outcome(
    *, second_key: str | None = "key-1", first_key: str | None = "key-1", runs: int = 1
) -> Outcome:
    fake = FakeCloud("http://127.0.0.1/api")
    flow = fake.add_flow("customer-feedback-reply")
    fake.add_version(flow["id"], feedback_plan())
    for index in range(runs):
        fake.start_run(
            flow_id=flow["id"],
            body={"parameters": PARAMETERS, "idempotency_key": f"run-{index}"},
        )
    return outcome(
        calls=[
            ToolCall(
                "start_run",
                {"parameters": PARAMETERS, "idempotency_key": first_key},
                "HTTP 504",
                is_error=True,
                turn=2,
            ),
            ToolCall(
                "start_run",
                {"parameters": PARAMETERS, "idempotency_key": second_key},
                {"created": False},
                turn=2,
            ),
        ],
        fake=fake,
    )


def test_run_retry_passes_for_the_expected_outcome():
    assert failed_checks(RETRY, retry_outcome()) == []


def test_run_retry_fails_when_the_retry_uses_a_new_key():
    assert failed_checks(RETRY, retry_outcome(second_key="key-2", runs=2)) == [
        "the second start_run reused the first one's idempotency key",
        "only one run exists",
    ]


def test_run_retry_fails_when_the_first_call_has_no_key():
    assert failed_checks(RETRY, retry_outcome(first_key=None, second_key=None)) == [
        "the second start_run reused the first one's idempotency key"
    ]


def test_run_retry_setup_loses_the_first_start_run_response():
    fake = FakeCloud("http://127.0.0.1/api")
    SCENARIOS["run-retry"].setup(fake)

    assert len(fake.flows) == 1
    assert [lost.remaining for lost in fake.lost_responses] == [1]


# No infrastructure.

NO_INFRA = SCENARIOS["no-infrastructure"]
INFRA_QUESTION = (
    "Before we design the workflow: do you have a remote MCP server, a web "
    "address that lets an agent use Zendesk or Slack?"
)
LIMIT_NOTE = (
    "Without a remote MCP server for Zendesk or Slack, no step can read tickets "
    "or post to Slack. I can build a version where you paste the tickets in as "
    "an input, and the run returns the summary for you to copy into Slack."
)


def infra_outcome(
    *,
    question: str = INFRA_QUESTION,
    note: str = LIMIT_NOTE,
    answer_turn: int = 1,
    extra_calls: list[ToolCall] | None = None,
) -> Outcome:
    return outcome(
        calls=[
            ToolCall("list_secret_blocks", {}, turn=1),
            ToolCall("list_deployments", {}, turn=1),
            *(extra_calls or []),
        ],
        replies=[Reply(answer_turn, "no-remote-servers", "")],
        turn_results=[question, note, note][: answer_turn + 1],
    )


def test_no_infrastructure_passes_for_the_expected_outcome():
    assert failed_checks(NO_INFRA, infra_outcome()) == []


def test_no_infrastructure_fails_when_the_first_message_asks_about_the_design():
    question = f"{INFRA_QUESTION} Also, what time should it run each morning?"

    assert failed_checks(NO_INFRA, infra_outcome(question=question)) == [
        "the first question asks no design question"
    ]


def test_no_infrastructure_fails_when_the_check_comes_after_a_design_round():
    assert "the agent's first question is the infrastructure check" in failed_checks(
        NO_INFRA, infra_outcome(answer_turn=2)
    )


def test_no_infrastructure_fails_without_an_offer_of_a_version_without_the_tools():
    note = "Zendesk and Slack aren't reachable, so this workflow can't be built."

    assert failed_checks(NO_INFRA, infra_outcome(note=note)) == [
        "the agent explains the limit and offers a version without the tools"
    ]


def test_no_infrastructure_fails_when_the_agent_drafts_a_plan():
    validate = ToolCall("validate_plan", {"plan": {}}, VALID, turn=2)

    assert failed_checks(NO_INFRA, infra_outcome(extra_calls=[validate])) == [
        "validate_plan never called"
    ]


def test_no_infrastructure_user_ends_after_answering_the_check():
    rules = no_infrastructure.USER

    assert [rule.label for rule in rules] == ["no-remote-servers", "end"]
    assert rules[0].max_uses == 1
    assert rules[1].reply is None
