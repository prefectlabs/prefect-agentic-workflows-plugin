"""The simulated user's rules, and what a scenario's checks read after a run."""

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from evals.assertions import Check, check_flows_named_with_prefix
from evals.record import FlowState, Transcript
from evals.sandbox import SandboxApi


@dataclass
class Rule:
    """One rule of the simulated user's script.

    After each agent turn, the simulated user takes the first rule that
    applies to the agent's final message and sends its `reply`. A rule with
    `reply` set to None ends the conversation. When no rule applies, the
    conversation ends too.

    A rule applies when `pattern` matches the message (with `re.search`,
    ignoring case), when the agent has called `after_tool` by then, when it
    has not yet called `before_tool`, and while the rule has been used fewer
    than `max_uses` times. `label` names the rule in the transcript, so a
    check can count how often the agent asked for something.
    """

    label: str
    reply: str | None
    pattern: str = r"[\s\S]"
    after_tool: str | None = None
    before_tool: str | None = None
    max_uses: int | None = None

    def applies(self, message: str, transcript: Transcript) -> bool:
        called = {call.name for call in transcript.tool_calls}
        if self.after_tool is not None and self.after_tool not in called:
            return False
        if self.before_tool is not None and self.before_tool in called:
            return False
        if (
            self.max_uses is not None
            and len(transcript.replies_labeled(self.label)) >= self.max_uses
        ):
            return False
        return re.search(self.pattern, message, re.IGNORECASE) is not None


def next_rule(rules: list[Rule], message: str, transcript: Transcript) -> Rule | None:
    return next((rule for rule in rules if rule.applies(message, transcript)), None)


TEST_RUN_OFFER = r"test run|start (a|the) run|run it"

# Scenarios only check authoring, so the simulated user turns down the test
# run the skill offers after publishing.
DECLINE_TEST_RUN = Rule(
    label="decline-test-run",
    after_tool="publish_plan",
    before_tool="start_run",
    pattern=TEST_RUN_OFFER,
    reply="No test run for now.",
    max_uses=1,
)


@dataclass
class Outcome:
    """Everything a scenario's checks can read after the conversation ends.

    `plans` has each `workflows/*.plan.json` file the agent wrote, by file
    name. `flows` has the state of each flow in the sandbox whose name starts
    with `flow_prefix`, read after the conversation, by the flow name without
    the prefix. `workspace` is the agent's working directory.
    """

    transcript: Transcript
    plans: dict[str, Any]
    flows: dict[str, FlowState]
    workspace: Path
    flow_prefix: str = ""

    @property
    def plan(self) -> dict[str, Any]:
        """Return the only plan file, or an empty plan when there isn't exactly one."""
        if len(self.plans) != 1:
            return {}
        plan = next(iter(self.plans.values()))
        return plan if isinstance(plan, dict) else {}


class RunScenario(Protocol):
    """The `run_scenario` fixture: run a conversation and return its outcome.

    `files` copies files or directories into the agent's working directory,
    by the relative path they get there. `setup` adds state to the sandbox
    before the agent starts, such as a flow or a Secret block. It gets the
    `SandboxApi` and the run's flow prefix, and must name every flow it
    creates with that prefix. `max_turns` caps the number of agent turns in
    the conversation.
    """

    async def __call__(
        self,
        prompt: str,
        user: list[Rule],
        *,
        files: dict[str, Path] | None = None,
        setup: Callable[[SandboxApi, str], None] | None = None,
        max_turns: int = 12,
    ) -> Outcome: ...


def assert_passed(outcome: Outcome, checks: list[Check]) -> None:
    """Fail the test with every failed check, and with any error of the agent."""
    checks = [
        Check(
            "agent turns finished without error",
            not outcome.transcript.errors,
            "; ".join(outcome.transcript.errors),
        ),
        check_flows_named_with_prefix(
            outcome.transcript.tool_calls, outcome.flow_prefix
        ),
        *checks,
    ]
    failed = [f"{check.name}: {check.detail}" for check in checks if not check.passed]
    assert not failed, "\n".join(
        ["Failed checks:", *failed, f"Working directory: {outcome.workspace.parent}"]
    )
