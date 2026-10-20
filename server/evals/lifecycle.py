"""Per-run setup and cleanup in the sandbox, as a Pydantic Evals `CaseLifecycle`.

Each run of a case gets its own directory and its own flow prefix inside the
session's prefix, such as `eval-3f9a1c-2-`. Runs that happen at the same
time, and repeats of one case, never read or delete each other's flows. A
case with `runs_alone` has the sandbox to itself while it runs.
"""

import asyncio
import itertools
from collections.abc import Iterator
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from pydantic_evals import Case
from pydantic_evals.evaluators import EvaluatorContext
from pydantic_evals.lifecycle import CaseLifecycle
from pydantic_evals.reporting import ReportCase, ReportCaseFailure

from evals.sandbox import SandboxApi
from evals.scenario import Outcome, ScenarioInputs

ScenarioCase = Case[ScenarioInputs, Outcome, None]


@dataclass(frozen=True)
class Run:
    """One run of one case: where the agent works, and the prefix of its flows."""

    sandbox: SandboxApi
    flow_prefix: str
    directory: Path


CURRENT_RUN: ContextVar[Run] = ContextVar("CURRENT_RUN")


def current_run() -> Run:
    """Return the run that `SandboxLifecycle.setup` started for this case.

    Pydantic Evals runs each case in its own asyncio task, and calls the
    task in the same context as `setup`, so each case reads its own run.
    """
    try:
        return CURRENT_RUN.get()
    except LookupError:
        raise RuntimeError(
            "No run is in progress. Evaluate the dataset with "
            "`lifecycle=session.lifecycle`."
        ) from None


Access = Literal["shared", "alone"]


class SandboxLock:
    """Lets runs share the sandbox, or gives one run the sandbox to itself.

    A run that takes it `alone` waits until no other run holds it, and no run
    takes it while that run holds it.
    """

    def __init__(self) -> None:
        self.condition = asyncio.Condition()
        self.sharing = 0
        self.held_alone = False

    async def acquire(self, access: Access) -> None:
        async with self.condition:
            if access == "alone":
                await self.condition.wait_for(
                    lambda: not self.held_alone and self.sharing == 0
                )
                self.held_alone = True
            else:
                await self.condition.wait_for(lambda: not self.held_alone)
                self.sharing += 1

    async def release(self, access: Access) -> None:
        async with self.condition:
            if access == "alone":
                self.held_alone = False
            else:
                self.sharing -= 1
            self.condition.notify_all()


@dataclass
class Session:
    """What every run of one evaluation shares.

    `flow_prefix` is the session's prefix, from `session_prefix()`. Each
    run's directory is in `output_dir`. `cleanup_errors` has one message for
    each run the harness couldn't clean up after.
    """

    sandbox: SandboxApi
    flow_prefix: str
    output_dir: Path
    cleanup_errors: list[str] = field(default_factory=list)
    lock: SandboxLock = field(default_factory=SandboxLock, repr=False)
    run_numbers: Iterator[int] = field(
        default_factory=lambda: itertools.count(1), repr=False
    )

    def new_run(self, case_name: str) -> Run:
        """Make the directory for the next run, and return the run."""
        number = next(self.run_numbers)
        directory = self.output_dir / f"{number}-{case_name}"
        directory.mkdir(parents=True)
        return Run(self.sandbox, f"{self.flow_prefix}{number}-", directory)

    def lifecycle(self, case: ScenarioCase) -> "SandboxLifecycle":
        """Return the lifecycle of one run. Pass this as `lifecycle` to `evaluate`."""
        return SandboxLifecycle(case, self)


class SandboxLifecycle(CaseLifecycle[ScenarioInputs, Outcome, None]):
    """Seeds the sandbox before a run, and cleans up after it.

    Pydantic Evals calls `teardown` after the task and the evaluators, also
    when the task or `setup` raised, so the case's own teardown runs and the
    run's flows are deleted either way. An error in `teardown` would stop the
    whole evaluation, so a failed cleanup is added to
    `Session.cleanup_errors` instead.
    """

    def __init__(self, case: ScenarioCase, session: Session) -> None:
        super().__init__(case)
        self.session = session
        self.access: Access | None = None
        self.run: Run | None = None
        self.token: Token[Run] | None = None

    async def setup(self) -> None:
        access: Access = "alone" if self.case.inputs.runs_alone else "shared"
        await self.session.lock.acquire(access)
        self.access = access
        self.run = self.session.new_run(self.case.name or "case")
        self.token = CURRENT_RUN.set(self.run)
        seed = self.case.inputs.setup
        if seed is not None:
            seed(self.run.sandbox, self.run.flow_prefix)

    async def prepare_context(
        self, ctx: EvaluatorContext[ScenarioInputs, Outcome, None]
    ) -> EvaluatorContext[ScenarioInputs, Outcome, None]:
        transcript = ctx.output.transcript
        ctx.metrics["agent_cost_usd"] = transcript.cost_usd
        ctx.metrics["agent_turns"] = len(transcript.turn_results)
        return ctx

    async def teardown(
        self,
        result: ReportCase[ScenarioInputs, Outcome, None]
        | ReportCaseFailure[ScenarioInputs, Outcome, None]
        | None,
    ) -> None:
        if self.token is not None:
            CURRENT_RUN.reset(self.token)
        try:
            if self.run is not None:
                self.clean_up(self.run)
        finally:
            if self.access is not None:
                await self.session.lock.release(self.access)

    def clean_up(self, run: Run) -> None:
        clean = self.case.inputs.teardown
        if clean is not None:
            try:
                clean(run.sandbox, run.flow_prefix)
            except Exception as exc:
                self.session.cleanup_errors.append(
                    f"The teardown of {run.directory.name} failed: {exc}"
                )
        try:
            self.session.sandbox.delete_flows_with_prefix(run.flow_prefix)
        except Exception as exc:
            self.session.cleanup_errors.append(
                f"Could not delete the flows named {run.flow_prefix}*: {exc}"
            )
