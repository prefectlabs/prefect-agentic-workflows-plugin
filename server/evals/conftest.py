"""Fixtures and the pass-rate report for the behavioral scenarios.

The default `pytest` run only collects `tests/`, so these run only when you
name this directory or a file in it.
"""

import os
from collections import defaultdict
from functools import partial
from pathlib import Path

import pytest

from evals.runner import converse
from evals.scenario import RunScenario

RESULTS: dict[str, list[bool]] = defaultdict(list)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--agent-model", help="Model for the agent to use.")


@pytest.fixture
def run_scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> RunScenario:
    """Return a function that runs one conversation in `tmp_path`."""
    # Only the settings the runner passes configure Prefect for the agent.
    for name in [name for name in os.environ if name.startswith("PREFECT_")]:
        monkeypatch.delenv(name)
    return partial(converse, tmp_path, model=request.config.getoption("--agent-model"))


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if report.when == "call" or (report.when == "setup" and report.failed):
        # pytest-repeat adds `[<n>-<count>]` to each repeat's ID.
        RESULTS[report.nodeid.split("[")[0]].append(report.passed)


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
    if not RESULTS:
        return
    terminalreporter.section("pass rate")
    for name, runs in RESULTS.items():
        passed = sum(runs)
        terminalreporter.write_line(
            f"{passed}/{len(runs)} ({passed / len(runs):.0%})  {name}"
        )
