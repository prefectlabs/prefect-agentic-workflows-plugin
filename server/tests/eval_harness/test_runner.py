"""Tests for the simulated user's rules and the runner's conversation loop."""

from collections.abc import AsyncIterator, Sequence

import pytest
from claude_agent_sdk import (
    AssistantMessage,
    Message,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)

from evals.record import Reply, ToolCall, Transcript
from evals.runner import talk
from evals.scenario import Rule, next_rule
from evals.scenarios import test_release_notes_conversion as release_notes

INFRASTRUCTURE_QUESTION = (
    "Which business tools can an agent reach? Any remote MCP server?"
)


@pytest.mark.parametrize(
    ("message", "tools", "replies", "expected"),
    [
        ("Want a test run?", [], [], "go-ahead"),
        ("Want a test run?", ["publish_plan"], [], "test-run-approval"),
        ("Do you approve?", [], [], "design-approval"),
        ("Do you approve?", ["validate_plan"], [], "go-ahead"),
        ("Anything else?", [], ["go-ahead"] * 3, None),
        (INFRASTRUCTURE_QUESTION, [], [], "reachable-systems"),
        (INFRASTRUCTURE_QUESTION, [], ["reachable-systems"], "go-ahead"),
    ],
)
def test_next_rule(
    message: str, tools: list[str], replies: list[str], expected: str | None
):
    transcript = Transcript(tool_calls=[ToolCall(name, {}) for name in tools])
    transcript.replies = [Reply(1, label, "") for label in replies]

    rule = next_rule(release_notes.USER, message, transcript)

    assert (rule.label if rule else None) == expected


def result(text: str) -> ResultMessage:
    return ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="session",
        total_cost_usd=0.01,
        result=text,
    )


class ScriptedAgent:
    """Answers each query with the next list of SDK messages."""

    def __init__(self, *turns: Sequence[Message]) -> None:
        self.turns = list(turns)
        self.prompts: list[str] = []

    async def query(self, prompt: str) -> None:
        self.prompts.append(prompt)

    async def receive_response(self) -> AsyncIterator[Message]:
        for message in self.turns.pop(0):
            yield message


async def test_talk_records_tool_calls_and_answers_from_the_rules():
    plan = {"kind": "ExecutionPlan"}
    first_turn: list[Message] = [
        AssistantMessage([TextBlock("I'll validate the plan.")], model="m"),
        AssistantMessage(
            [
                ToolUseBlock(
                    "call-1",
                    "mcp__prefect-agentic-workflows__validate_plan",
                    {"plan": plan},
                ),
                ToolUseBlock("call-2", "Read", {"file_path": "SKILL.md"}),
            ],
            model="m",
        ),
        UserMessage(
            [
                ToolResultBlock(
                    "call-1", [{"type": "text", "text": '{"valid": true}'}]
                ),
                ToolResultBlock("call-2", "No such file", is_error=True),
            ]
        ),
        result("The plan is valid. Do you approve?"),
    ]
    agent = ScriptedAgent(first_turn, [result("Published.")])
    rules = [
        Rule(label="approve", pattern=r"approve", reply="Yes."),
        Rule(label="end", reply=None),
    ]
    transcript = Transcript()

    await talk(agent, transcript, "Convert the skill.", rules, max_turns=5)

    assert agent.prompts == ["Convert the skill.", "Yes."]
    assert transcript.tool_calls == [
        ToolCall("validate_plan", {"plan": plan}, {"valid": True}, False, 1),
        ToolCall("Read", {"file_path": "SKILL.md"}, "No such file", True, 1),
    ]
    assert transcript.turn_results == [
        "The plan is valid. Do you approve?",
        "Published.",
    ]
    assert transcript.replies == [Reply(1, "approve", "Yes.")]
    assert transcript.errors == []
