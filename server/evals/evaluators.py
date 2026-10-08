"""Pydantic Evals evaluators that run the checks on a case's outcome.

Each evaluator returns one named result per check, so the report lists every
check with the reason it failed.
"""

from collections.abc import Callable
from dataclasses import dataclass

from pydantic_evals.evaluators import EvaluationReason, Evaluator, EvaluatorContext

from evals import assertions
from evals.assertions import Check
from evals.scenario import Outcome, ScenarioInputs

ScenarioContext = EvaluatorContext[ScenarioInputs, Outcome, None]


def results(checks: list[Check]) -> dict[str, EvaluationReason]:
    """Return each check as an assertion, by its name, with its detail as the reason."""
    return {
        check.name: EvaluationReason(check.passed, reason=check.detail or None)
        for check in checks
    }


def common_checks(outcome: Outcome) -> list[Check]:
    """Return the checks that apply to every case.

    Every scenario checks authoring only, so none of them starts a run.
    """
    calls = outcome.transcript.tool_calls
    return [
        Check(
            "agent turns finished without error",
            not outcome.transcript.errors,
            "; ".join(outcome.transcript.errors),
        ),
        assertions.check_flows_named_with_prefix(calls, outcome.flow_prefix),
        assertions.check_never_called(calls, "start_run"),
    ]


@dataclass(repr=False)
class CommonChecks(Evaluator[ScenarioInputs, Outcome, None]):
    """Runs `common_checks` on every case of the dataset."""

    def evaluate(self, ctx: ScenarioContext) -> dict[str, EvaluationReason]:
        return results(common_checks(ctx.output))


@dataclass(repr=False)
class ScenarioChecks(Evaluator[ScenarioInputs, Outcome, None]):
    """Runs one scenario's own checks on its outcome."""

    checks: Callable[[Outcome], list[Check]]

    def evaluate(self, ctx: ScenarioContext) -> dict[str, EvaluationReason]:
        return results(self.checks(ctx.output))
