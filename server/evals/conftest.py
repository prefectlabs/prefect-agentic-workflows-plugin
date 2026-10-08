"""Fixtures and the pass-rate report for the behavioral scenarios.

The default `pytest` run only collects `tests/`, so these run only when you
name this directory or a file in it.
"""

import itertools
import os
from collections import defaultdict
from collections.abc import Iterator
from functools import partial
from pathlib import Path

import pytest

from evals.report import pass_rate_lines, pass_rate_markdown
from evals.runner import converse
from evals.sandbox import (
    SandboxApi,
    SandboxConfigError,
    read_credentials,
    session_prefix,
)
from evals.scenario import RunScenario

RESULTS: dict[str, list[bool]] = defaultdict(list)
RUN_NUMBERS = itertools.count(1)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--agent-model", help="Model for the agent to use.")
    parser.addoption(
        "--pass-rate-file",
        help="Also write the pass-rate table to this file, as Markdown.",
    )


@pytest.fixture(scope="session")
def session_flow_prefix() -> str:
    """Return the prefix of every flow name in this test session."""
    return session_prefix()


@pytest.fixture(scope="session")
def sandbox(session_flow_prefix: str) -> Iterator[SandboxApi]:
    """Return a client for the sandbox workspace, and delete the session's flows after.

    Stops the session when `PREFECT_API_URL` or `PREFECT_API_KEY` is missing.
    """
    try:
        credentials = read_credentials()
    except SandboxConfigError as exc:
        pytest.exit(str(exc), returncode=pytest.ExitCode.USAGE_ERROR)
    api = SandboxApi(credentials)
    try:
        yield api
    finally:
        try:
            deleted = api.delete_flows_with_prefix(session_flow_prefix)
            print(f"\nDeleted {len(deleted)} flows named {session_flow_prefix}*")
        finally:
            api.close()


@pytest.fixture
def flow_prefix(session_flow_prefix: str) -> str:
    """Return the prefix for the flows of one scenario run.

    Each run gets its own prefix inside the session's, so a repeat with
    `--count` doesn't find the flows of the run before it.
    """
    return f"{session_flow_prefix}{next(RUN_NUMBERS)}-"


@pytest.fixture
def run_scenario(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    sandbox: SandboxApi,
    flow_prefix: str,
) -> RunScenario:
    """Return a function that runs one conversation in `tmp_path`."""
    # Only the settings the runner passes configure Prefect for the agent.
    for name in [name for name in os.environ if name.startswith("PREFECT_")]:
        monkeypatch.delenv(name)
    return partial(
        converse,
        tmp_path,
        sandbox=sandbox,
        flow_prefix=flow_prefix,
        model=request.config.getoption("--agent-model"),
    )


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if report.when == "call" or (report.when == "setup" and report.failed):
        # pytest-repeat adds `[<n>-<count>]` to each repeat's ID.
        RESULTS[report.nodeid.split("[")[0]].append(report.passed)


def pytest_terminal_summary(
    terminalreporter: pytest.TerminalReporter, config: pytest.Config
) -> None:
    if not RESULTS:
        return
    terminalreporter.section("pass rate")
    for line in pass_rate_lines(RESULTS):
        terminalreporter.write_line(line)
    path = config.getoption("--pass-rate-file")
    if path:
        Path(path).write_text(
            pass_rate_markdown(RESULTS, config.getoption("--agent-model"))
        )
