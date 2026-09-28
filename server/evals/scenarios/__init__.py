"""The scenarios the harness can run, by name.

Add a scenario by writing a module in this package that defines `SCENARIO`,
and adding it to `SCENARIOS` below.
"""

from evals.scenario import Scenario
from evals.scenarios import release_notes_conversion

SCENARIOS: dict[str, Scenario] = {
    scenario.name: scenario for scenario in [release_notes_conversion.SCENARIO]
}
