"""Tests for the evaluators that turn the checks into named results."""

from pathlib import Path

from pydantic_evals import Case, Dataset
from pydantic_evals.evaluators import EvaluationReason

from evals.assertions import Check
from evals.dataset import CASE_NAMES, build_dataset
from evals.evaluators import CommonChecks, ScenarioChecks, common_checks, results
from evals.record import ToolCall, Transcript
from evals.scenario import Outcome, ScenarioInputs

PREFIX = "eval-abc123-1-"


def outcome(transcript: Transcript) -> Outcome:
    return Outcome(transcript, {}, {}, Path("."), PREFIX)


def test_results_names_each_check_and_gives_the_detail_as_the_reason():
    checks = [Check("passed", True), Check("failed", False, "found 0")]

    assert results(checks) == {
        "passed": EvaluationReason(True),
        "failed": EvaluationReason(False, reason="found 0"),
    }


def test_common_checks_pass_for_a_clean_run():
    calls = [ToolCall("get_or_create_flow", {"name": f"{PREFIX}digest"})]

    failed = [
        check.name
        for check in common_checks(outcome(Transcript(tool_calls=calls)))
        if not check.passed
    ]

    assert failed == []


def test_common_checks_fail_for_errors_unprefixed_flows_and_runs():
    transcript = Transcript(
        tool_calls=[
            ToolCall("get_or_create_flow", {"name": "digest"}),
            ToolCall("start_run", {}),
        ],
        errors=["turn 2 timed out"],
    )

    failed = {
        check.name: check.detail
        for check in common_checks(outcome(transcript))
        if not check.passed
    }

    assert failed == {
        "agent turns finished without error": "turn 2 timed out",
        f"every flow the agent created starts with {PREFIX!r}": (
            "get_or_create_flow called with ['digest']"
        ),
        "start_run never called": "called 1 times",
    }


async def test_the_report_lists_every_check_of_both_evaluators():
    def checks(found: Outcome) -> list[Check]:
        return [Check("the plan has a node", False, "found none")]

    async def task(inputs: ScenarioInputs) -> Outcome:
        return outcome(Transcript())

    data = Dataset[ScenarioInputs, Outcome, None](
        name="test",
        cases=[
            Case(
                name="only",
                inputs=ScenarioInputs(prompt="", user=[]),
                evaluators=[ScenarioChecks(checks)],
            )
        ],
        evaluators=[CommonChecks()],
    )

    report = await data.evaluate(task, progress=False)

    assertions = report.cases[0].assertions
    assert {name: result.value for name, result in assertions.items()} == {
        "agent turns finished without error": True,
        f"every flow the agent created starts with {PREFIX!r}": True,
        "start_run never called": True,
        "the plan has a node": False,
    }
    assert assertions["the plan has a node"].reason == "found none"


def test_build_dataset_has_a_case_for_each_scenario():
    assert [case.name for case in build_dataset().cases] == CASE_NAMES
    assert [case.name for case in build_dataset(["scheduled_edit"]).cases] == [
        "scheduled_edit"
    ]
