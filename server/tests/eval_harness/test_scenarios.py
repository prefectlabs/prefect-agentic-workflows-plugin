"""Tests for the simulated user's rules and the checks of the release-notes scenario."""

from pathlib import Path
from typing import Any

from harness_plans import approval_plan

from evals.fake_cloud import FakeCloud
from evals.record import Reply, ToolCall, Transcript
from evals.scenario import Outcome, Rule, next_rule
from evals.scenarios import test_release_notes_conversion as release_notes_conversion

RULES = [
    Rule(label="run", after_tool="publish_plan", pattern=r"test run", reply="Yes."),
    Rule(label="design", before_tool="validate_plan", pattern=r"approve", reply="Ok."),
    Rule(label="default", reply="Use the default.", max_uses=1),
]


def transcript_with(*tools: str, replies: list[str] | None = None) -> Transcript:
    transcript = Transcript(tool_calls=[ToolCall(name, {}) for name in tools])
    transcript.replies = [Reply(1, label, "") for label in replies or []]
    return transcript


def label(message: str, transcript: Transcript) -> str | None:
    rule = next_rule(RULES, message, transcript)
    return rule.label if rule else None


def test_a_rule_applies_only_after_its_after_tool():
    assert label("Want a test run?", transcript_with()) == "default"
    assert label("Want a test run?", transcript_with("publish_plan")) == "run"


def test_a_rule_stops_applying_once_its_before_tool_is_called():
    assert label("Do you approve?", transcript_with()) == "design"
    assert label("Do you approve?", transcript_with("validate_plan")) == "default"


def test_a_rule_stops_applying_after_max_uses():
    assert label("Anything else?", transcript_with(replies=["default"])) is None


VALID = {"valid": True, "errors": []}
REPORT = """\
## Conversion report: release-notes

| 1 | Runs `scripts/collect_changes.py` | S1 | No shell |
| 2 | stdio MCP server started with `docker run -i` | Setup | Remote only |
| 3 | Repeat-until-done loop | S4 | No cycles |
| 4 | Human approval | S5 | Converts as a human-input node |
"""


def outcome(
    *, report: str = REPORT, approvals: int = 1, plan: dict[str, Any] | None = None
) -> Outcome:
    plan = plan if plan is not None else approval_plan()
    transcript = Transcript(
        agent_text=["I'll read the skill.", report],
        tool_calls=[
            ToolCall("validate_plan", {"plan": plan}, VALID),
            ToolCall("publish_plan", {"plan": plan}, {"published": True}),
        ],
        replies=[Reply(1, "design-approval", "")] * approvals,
    )
    return Outcome(
        transcript,
        {"release-notes.plan.json": plan},
        FakeCloud("http://127.0.0.1/api"),
        Path("."),
    )


def failed_checks(result: Outcome) -> list[str]:
    checks = release_notes_conversion.checks(result)
    return [check.name for check in checks if not check.passed]


def test_release_notes_checks_pass_for_the_expected_outcome():
    assert failed_checks(outcome()) == []


def test_release_notes_checks_fail_when_the_report_leaves_out_the_stdio_server():
    report = REPORT.replace("stdio MCP server started with `docker run -i`", "Setup")

    assert failed_checks(outcome(report=report)) == [
        "conversion report lists every unsupported part"
    ]


def test_release_notes_checks_fail_when_the_design_is_approved_twice():
    assert failed_checks(outcome(approvals=2)) == ["design approved once"]


def test_release_notes_checks_fail_without_a_human_input_node():
    plan = approval_plan()
    plan["nodes"]["approve"]["kind"] = "AgentNode"

    assert failed_checks(outcome(plan=plan)) == ["plan has at least 1 HumanInputNode"]


INFRASTRUCTURE_QUESTION = """\
Before we design the workflow, I need to know which of your business tools it
can use. A step that an AI agent runs in Prefect Cloud can only use a tool
through a remote MCP server: a web address that lets an agent use one of your
business tools. For each tool this workflow needs, do you have that web
address?
"""


def test_release_notes_user_answers_the_infrastructure_check_before_the_design():
    rule = next_rule(
        release_notes_conversion.USER, INFRASTRUCTURE_QUESTION, transcript_with()
    )

    assert rule is not None
    assert rule.label == "reachable-systems"
    assert rule.reply is not None
    assert "https://tools.example.com/mcp" in rule.reply


def test_release_notes_user_approves_the_design_after_the_infrastructure_check():
    transcript = transcript_with(replies=["reachable-systems"])
    rule = next_rule(
        release_notes_conversion.USER,
        "Here is the conversion report. Do you approve this design?",
        transcript,
    )

    assert rule is not None
    assert rule.label == "design-approval"
