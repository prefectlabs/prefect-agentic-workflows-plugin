"""The scenarios the harness can run, by name.

Add a scenario by writing a module in this package that defines `SCENARIO`,
and adding it to `SCENARIOS` below.
"""

from evals.scenario import Scenario
from evals.scenarios import (
    expired_approval,
    no_infrastructure,
    rejected_approval,
    release_notes_conversion,
    run_retry,
    scheduled_edit,
    unsupported_loop,
)

SCENARIOS: dict[str, Scenario] = {
    scenario.name: scenario
    for scenario in [
        release_notes_conversion.SCENARIO,
        rejected_approval.SCENARIO,
        expired_approval.SCENARIO,
        unsupported_loop.SCENARIO,
        scheduled_edit.SCENARIO,
        run_retry.SCENARIO,
        no_infrastructure.SCENARIO,
    ]
}
