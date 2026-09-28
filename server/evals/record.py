"""The record of one evaluation: what the agent said and which tools it called.

The runner starts `claude -p --output-format stream-json`, which prints one
JSON event per line. `Transcript.read_events` reads those events. It pairs each
`tool_use` block with its `tool_result`, and keeps the agent's text and the
final message of each turn.
"""

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from prefect_agentic_workflows_mcp.server import SERVER_NAME

SERVER_TOOL_PREFIX = f"mcp__{SERVER_NAME}__"


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
    session_id: str | None = None
    cost_usd: float = 0.0
    errors: list[str] = field(default_factory=list)

    def read_events(self, lines: Iterable[str], turn: int) -> str:
        """Add the events of one agent turn and return the turn's final message."""
        pending: dict[str, ToolCall] = {}
        final = ""
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if not isinstance(event, dict):
                continue
            kind = event.get("type")
            if event.get("session_id"):
                self.session_id = event["session_id"]
            if kind == "assistant":
                for block in message_blocks(event):
                    if block.get("type") == "text" and block.get("text"):
                        self.agent_text.append(block["text"])
                    elif block.get("type") == "tool_use":
                        call = ToolCall(
                            name=short_tool_name(str(block.get("name"))),
                            arguments=block.get("input") or {},
                            turn=turn,
                        )
                        pending[str(block.get("id"))] = call
                        self.tool_calls.append(call)
            elif kind == "user":
                for block in message_blocks(event):
                    if block.get("type") != "tool_result":
                        continue
                    call = pending.pop(str(block.get("tool_use_id")), None)
                    if call is not None:
                        call.result = parse_result(block.get("content"))
                        call.is_error = bool(block.get("is_error"))
            elif kind == "result":
                final = str(event.get("result") or "")
                self.cost_usd += float(event.get("total_cost_usd") or 0.0)
                if event.get("is_error"):
                    self.errors.append(final or str(event.get("subtype")))
        self.turn_results.append(final)
        return final

    @property
    def all_agent_text(self) -> str:
        return "\n\n".join(self.agent_text)

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

    def to_json(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "cost_usd": self.cost_usd,
            "replies": [vars(reply) for reply in self.replies],
            "turn_results": self.turn_results,
            "tool_calls": [vars(call) for call in self.tool_calls],
            "agent_text": self.agent_text,
            "errors": self.errors,
        }


def message_blocks(event: dict[str, Any]) -> list[dict[str, Any]]:
    message = event.get("message")
    if not isinstance(message, dict):
        return []
    content = message.get("content")
    if not isinstance(content, list):
        return []
    return [block for block in content if isinstance(block, dict)]


def short_tool_name(name: str) -> str:
    return name.removeprefix(SERVER_TOOL_PREFIX)


def parse_result(content: Any) -> Any:
    """Return a tool result's content as JSON when it parses, and as text otherwise."""
    if isinstance(content, list):
        text = "".join(
            str(block.get("text", ""))
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        )
    else:
        text = str(content or "")
    try:
        return json.loads(text)
    except ValueError:
        return text


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
