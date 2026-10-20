"""The record of one evaluation: what the agent said and did, and the flows it left."""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ToolCall:
    """One tool call the agent made.

    `name` is the tool's name. A tool from the `prefect-agentic-workflows`
    server has its bare name, such as `validate_plan`. Other tools keep the
    name the agent used, such as `Read` or `Bash`. `result` is the tool
    result parsed as JSON when it is JSON, and its text otherwise.
    """

    name: str
    arguments: dict[str, Any]
    result: Any = None
    is_error: bool = False
    turn: int = 0


@dataclass
class Reply:
    """One message the simulated user sent, and the rule of the script it came from."""

    turn: int
    label: str
    text: str


@dataclass
class Transcript:
    tool_calls: list[ToolCall] = field(default_factory=list)
    agent_text: list[str] = field(default_factory=list)
    turn_results: list[str] = field(default_factory=list)
    replies: list[Reply] = field(default_factory=list)
    cost_usd: float = 0.0
    errors: list[str] = field(default_factory=list)

    def replies_labeled(self, label: str) -> list[Reply]:
        return [reply for reply in self.replies if reply.label == label]

    def first_reply(self, label: str) -> Reply | None:
        """Return the first reply the simulated user sent with a rule, or None."""
        return next(iter(self.replies_labeled(label)), None)

    def final_message(self, turn: int) -> str:
        """Return the agent's final message of a turn, counting from 1."""
        if 1 <= turn <= len(self.turn_results):
            return self.turn_results[turn - 1]
        return ""


@dataclass(frozen=True)
class FlowState:
    """One flow in the sandbox workspace, read after the conversation ended.

    `active_plan` is the plan document of the active version, or None when
    no version is active. `version_ids` lists the flow's plan versions from
    oldest to newest. `schedules` has each schedule as the API returned it.
    """

    id: str
    name: str
    active_version_id: str | None = None
    active_plan: dict[str, Any] | None = None
    version_ids: list[str] = field(default_factory=list)
    schedules: list[dict[str, Any]] = field(default_factory=list)


def read_plan_files(workspace: Path) -> dict[str, Any]:
    """Return each `workflows/*.plan.json` file in the workspace, by file name.

    A file that isn't valid JSON is returned as its text.
    """
    plans: dict[str, Any] = {}
    for path in sorted((workspace / "workflows").glob("*.plan.json")):
        text = path.read_text()
        try:
            plans[path.name] = json.loads(text)
        except ValueError:
            plans[path.name] = text
    return plans
