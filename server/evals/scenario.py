"""What a scenario gives the runner, the simulated user's rules, and a run's outcome."""

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from evals.assertions import Check
from evals.record import FlowState, Transcript
from evals.sandbox import SandboxApi

FLOW_PREFIX = "{flow_prefix}"


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

    def published_plan(self, flow_name: str) -> dict[str, Any]:
        """Return the active plan of the flow in the sandbox, or `plan` without one."""
        flow = self.flows.get(flow_name)
        return (flow.active_plan if flow else None) or self.plan


@dataclass
class ScenarioInputs:
    """The inputs of one case: what the runner needs to hold the conversation.

    `prompt` is the first message to the agent. The runner replaces each
    `{flow_prefix}` in it with the run's flow prefix, so the agent names
    every flow it creates with that prefix. `user` is the simulated user's
    rules. `files` copies files or directories into the agent's working
    directory, by the relative path they get there. `setup` adds state to
    the sandbox before the agent starts, such as a flow or a Secret block.
    It gets the `SandboxApi` and the run's flow prefix, and must name every
    flow it creates with that prefix. `max_turns` caps the number of agent
    turns in the conversation.
    """

    prompt: str
    user: list[Rule]
    files: dict[str, Path] = field(default_factory=dict)
    setup: Callable[[SandboxApi, str], None] | None = None
    max_turns: int = 12

    def prompt_for(self, flow_prefix: str) -> str:
        return self.prompt.replace(FLOW_PREFIX, flow_prefix)


@dataclass(frozen=True)
class Judgement:
    """One question an LLM judge answers about a case's outcome.

    `name` is the assertion's name in the report. `rubric` is the statement
    the judge checks against the evidence. `evidence` returns the only text
    the judge reads, such as the plan and the agent's report, or an empty
    string when the outcome has nothing to judge, which fails the assertion
    without calling the judge.
    """

    name: str
    rubric: str
    evidence: Callable[[Outcome], str]


def section(tag: str, text: str) -> str:
    """Return the text between `<tag>` and `</tag>`, for a judge's evidence."""
    return f"<{tag}>\n{text.strip()}\n</{tag}>"


@dataclass
class Scenario:
    """One case of the dataset, and the checks that run on its outcome.

    `name` is the case name in the report and for `--case`. `checks`
    returns one `Check` for each thing the scenario expects of the agent.
    `judgements` are the questions an LLM judge answers about the outcome,
    when the judges are on.
    """

    name: str
    inputs: ScenarioInputs
    checks: Callable[[Outcome], list[Check]]
    judgements: list[Judgement] = field(default_factory=list)
