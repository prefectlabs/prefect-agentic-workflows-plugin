"""The Pydantic Evals dataset: one case for each scenario."""

from collections.abc import Sequence

from pydantic_ai.models import Model
from pydantic_evals import Case, Dataset
from pydantic_evals.evaluators import Evaluator

from evals.evaluators import CommonChecks, Judge, ScenarioChecks
from evals.scenario import Outcome, Scenario, ScenarioInputs
from evals.scenarios import (
    no_infrastructure,
    rejected_approval,
    release_notes_conversion,
    scheduled_edit,
    unsupported_loop,
)

DATASET_NAME = "agentic-workflows-behavior"
SCENARIOS: list[Scenario] = [
    no_infrastructure.SCENARIO,
    unsupported_loop.SCENARIO,
    release_notes_conversion.SCENARIO,
    scheduled_edit.SCENARIO,
    rejected_approval.SCENARIO,
]
CASE_NAMES = [scenario.name for scenario in SCENARIOS]


def scenario_case(
    scenario: Scenario, judge_model: Model | str | None = None
) -> Case[ScenarioInputs, Outcome, None]:
    """Return the scenario's case.

    With `judge_model`, the case also gets a `Judge` for each judgement.
    """
    evaluators: list[Evaluator[ScenarioInputs, Outcome, None]] = [
        ScenarioChecks(scenario.checks)
    ]
    if judge_model is not None:
        evaluators += [
            Judge(judgement, judge_model) for judgement in scenario.judgements
        ]
    return Case(name=scenario.name, inputs=scenario.inputs, evaluators=evaluators)


def build_dataset(
    names: Sequence[str] | None = None, judge_model: Model | str | None = None
) -> Dataset[ScenarioInputs, Outcome, None]:
    """Return the dataset with the cases in `names`, or with every case.

    With `judge_model`, the cases with judgements get an LLM judge that uses
    that model. Without it, no case calls a judge.

    Raises `ValueError` for a name that isn't a case.
    """
    unknown = sorted(set(names or []) - set(CASE_NAMES))
    if unknown:
        raise ValueError(
            f"Unknown case {', '.join(unknown)}. The cases are {', '.join(CASE_NAMES)}."
        )
    return Dataset(
        name=DATASET_NAME,
        cases=[
            scenario_case(scenario, judge_model)
            for scenario in SCENARIOS
            if not names or scenario.name in names
        ],
        evaluators=[CommonChecks()],
    )
