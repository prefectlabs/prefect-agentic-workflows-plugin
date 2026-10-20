"""Tests for the per-run setup and cleanup, with Pydantic Evals and a stub sandbox."""

import asyncio
from pathlib import Path
from typing import Any

import pytest
from pydantic_evals import Case, Dataset

from evals.evaluators import CommonChecks
from evals.lifecycle import Session, current_run
from evals.record import Transcript
from evals.sandbox import Credentials, SandboxApi
from evals.scenario import Outcome, ScenarioInputs

SESSION_PREFIX = "eval-abc123-"


class RecordingSandbox(SandboxApi):
    """Records the flows it creates and the prefixes it deletes, and sends nothing."""

    def __init__(self, fail_deletes: bool = False) -> None:
        super().__init__(Credentials("https://api.prefect.cloud/api/x", "pnu_key"))
        self.created: list[str] = []
        self.deleted_prefixes: list[str] = []
        self.fail_deletes = fail_deletes

    def create_flow(self, name: str) -> dict[str, Any]:
        self.created.append(name)
        return {"id": "flow-1", "name": name}

    def delete_flows_with_prefix(self, prefix: str) -> list[str]:
        self.deleted_prefixes.append(prefix)
        if self.fail_deletes:
            raise RuntimeError("HTTP 500")
        return []


def seed(sandbox: SandboxApi, flow_prefix: str) -> None:
    sandbox.create_flow(f"{flow_prefix}digest")


def dataset(*names: str) -> Dataset[ScenarioInputs, Outcome, None]:
    inputs = ScenarioInputs(prompt="Build it.", user=[], setup=seed)
    return Dataset(
        name="test",
        cases=[Case(name=name, inputs=inputs) for name in names],
        evaluators=[CommonChecks()],
    )


def session(tmp_path: Path, sandbox: SandboxApi) -> Session:
    return Session(sandbox, SESSION_PREFIX, tmp_path)


async def finished_task(inputs: ScenarioInputs) -> Outcome:
    run = current_run()
    # Let the other runs start, so they overlap.
    await asyncio.sleep(0.01)
    return Outcome(Transcript(cost_usd=0.5), {}, {}, run.directory, run.flow_prefix)


async def test_each_run_seeds_and_deletes_its_own_prefix(tmp_path: Path):
    sandbox = RecordingSandbox()
    current = session(tmp_path, sandbox)

    report = await dataset("first", "second").evaluate(
        finished_task,
        lifecycle=current.lifecycle,
        max_concurrency=2,
        repeat=2,
        progress=False,
    )

    prefixes = sorted(case.output.flow_prefix for case in report.cases)
    assert prefixes == [f"{SESSION_PREFIX}{number}-" for number in (1, 2, 3, 4)]
    assert sorted(sandbox.created) == [f"{prefix}digest" for prefix in prefixes]
    assert sorted(sandbox.deleted_prefixes) == prefixes
    directories = sorted(path.name for path in tmp_path.iterdir())
    assert directories == ["1-first", "2-first", "3-second", "4-second"]
    assert report.cases[0].metrics == {"agent_cost_usd": 0.5, "agent_turns": 0}
    assert current.cleanup_errors == []


async def test_cleanup_runs_when_the_task_raises(tmp_path: Path):
    sandbox = RecordingSandbox()

    async def failing_task(inputs: ScenarioInputs) -> Outcome:
        raise RuntimeError("the agent crashed")

    report = await dataset("only").evaluate(
        failing_task, lifecycle=session(tmp_path, sandbox).lifecycle, progress=False
    )

    assert [failure.name for failure in report.failures] == ["only"]
    assert sandbox.created == [f"{SESSION_PREFIX}1-digest"]
    assert sandbox.deleted_prefixes == [f"{SESSION_PREFIX}1-"]


async def test_cleanup_runs_when_seeding_raises(tmp_path: Path):
    sandbox = RecordingSandbox()

    def failing_seed(api: SandboxApi, flow_prefix: str) -> None:
        api.create_flow(f"{flow_prefix}digest")
        raise RuntimeError("HTTP 500")

    inputs = ScenarioInputs(prompt="Build it.", user=[], setup=failing_seed)
    failing = Dataset[ScenarioInputs, Outcome, None](
        name="test", cases=[Case(name="only", inputs=inputs)]
    )

    report = await failing.evaluate(
        finished_task, lifecycle=session(tmp_path, sandbox).lifecycle, progress=False
    )

    assert len(report.failures) == 1
    assert sandbox.deleted_prefixes == [f"{SESSION_PREFIX}1-"]


async def test_a_failed_cleanup_is_recorded_and_the_evaluation_continues(
    tmp_path: Path,
):
    sandbox = RecordingSandbox(fail_deletes=True)
    current = session(tmp_path, sandbox)

    report = await dataset("first", "second").evaluate(
        finished_task, lifecycle=current.lifecycle, progress=False
    )

    assert len(report.cases) == 2
    assert sorted(current.cleanup_errors) == [
        f"Could not delete the flows named {SESSION_PREFIX}1-*: HTTP 500",
        f"Could not delete the flows named {SESSION_PREFIX}2-*: HTTP 500",
    ]


def test_current_run_outside_a_case():
    with pytest.raises(RuntimeError, match="No run is in progress"):
        current_run()
