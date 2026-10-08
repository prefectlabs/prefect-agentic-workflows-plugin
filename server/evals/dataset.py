"""The Pydantic Evals dataset: one case for each scenario."""

from collections.abc import Sequence

from pydantic_evals import Case, Dataset

from evals.evaluators import CommonChecks, ScenarioChecks
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


def scenario_case(scenario: Scenario) -> Case[ScenarioInputs, Outcome, None]:
    return Case(
        name=scenario.name,
        inputs=scenario.inputs,
        evaluators=[ScenarioChecks(scenario.checks)],
    )


def build_dataset(
    names: Sequence[str] | None = None,
) -> Dataset[ScenarioInputs, Outcome, None]:
    """Return the dataset with the cases in `names`, or with every case.

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
            scenario_case(scenario)
            for scenario in SCENARIOS
            if not names or scenario.name in names
        ],
        evaluators=[CommonChecks()],
    )
