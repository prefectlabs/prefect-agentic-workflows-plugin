"""Tests for the simulated user's rules and the runner's conversation loop."""

import json
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import Any

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

from evals import runner
from evals.lifecycle import CURRENT_RUN, Run
from evals.record import FlowState, Reply, ToolCall, Transcript
from evals.runner import converse, scenario_task, talk
from evals.sandbox import Credentials, SandboxApi
from evals.scenario import Rule, ScenarioInputs, next_rule
from evals.scenarios import release_notes_conversion as release_notes

INFRASTRUCTURE_QUESTION = (
    "Which business tools can an agent reach? Any remote MCP server?"
)


@pytest.mark.parametrize(
    ("message", "tools", "replies", "expected"),
    [
        ("Want a test run?", [], [], "go-ahead"),
        ("Want a test run?", ["publish_plan"], [], "decline-test-run"),
        ("Want a test run?", ["publish_plan"], ["decline-test-run"], "end"),
        ("Published. Anything else?", ["publish_plan"], [], "end"),
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


class StubSandbox(SandboxApi):
    """Returns a fixed snapshot, and records the prefixes it was asked for."""

    def __init__(self) -> None:
        super().__init__(Credentials("https://api.prefect.cloud/api/x", "pnu_key"))
        self.snapshot_prefixes: list[str] = []

    def snapshot(self, prefix: str) -> dict[str, FlowState]:
        self.snapshot_prefixes.append(prefix)
        return {"digest": FlowState("flow-1", f"{prefix}digest")}


def stub_client(clients: list[Any], *turns: Sequence[Message]) -> type[ScriptedAgent]:
    """Return a stand-in for `ClaudeSDKClient` that plays `turns`."""

    class StubClient(ScriptedAgent):
        def __init__(self, options: Any) -> None:
            super().__init__(*turns)
            self.options = options
            clients.append(self)

        async def __aenter__(self) -> "StubClient":
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

    return StubClient


async def test_converse_snapshots_the_run_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    clients: list[Any] = []
    monkeypatch.setattr(
        runner, "ClaudeSDKClient", stub_client(clients, [result("Done.")])
    )
    sandbox = StubSandbox()

    outcome = await converse(
        tmp_path,
        "Build it.",
        [Rule(label="end", reply=None)],
        sandbox=sandbox,
        flow_prefix="eval-abc123-1-",
    )

    assert sandbox.snapshot_prefixes == ["eval-abc123-1-"]
    assert list(outcome.flows) == ["digest"]
    assert outcome.flow_prefix == "eval-abc123-1-"
    env = clients[0].options.env
    assert env["PREFECT_API_URL"] == "https://api.prefect.cloud/api/x"
    assert env["PREFECT_API_KEY"] == "pnu_key"
    assert env["PREFECT_HOME"] == str(tmp_path / "prefect-home")
    assert not any("LOOPBACK" in name for name in env)
    transcript = json.loads((tmp_path / "transcript.json").read_text())
    assert transcript["turn_results"] == ["Done."]


async def test_converse_saves_the_transcript_when_the_agent_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # A turn with no messages left raises IndexError in `receive_response`.
    monkeypatch.setattr(runner, "ClaudeSDKClient", stub_client([]))

    with pytest.raises(IndexError):
        await converse(
            tmp_path,
            "Build it.",
            [],
            sandbox=StubSandbox(),
            flow_prefix="eval-abc123-1-",
        )

    assert (tmp_path / "transcript.json").exists()


async def test_scenario_task_runs_the_conversation_of_the_current_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    clients: list[Any] = []
    monkeypatch.setattr(
        runner, "ClaudeSDKClient", stub_client(clients, [result("Done.")])
    )
    sandbox = StubSandbox()
    inputs = ScenarioInputs(
        prompt="Build {flow_prefix}digest.", user=[Rule(label="end", reply=None)]
    )
    token = CURRENT_RUN.set(Run(sandbox, "eval-abc123-2-", tmp_path))
    try:
        outcome = await scenario_task("claude-haiku-5-5")(inputs)
    finally:
        CURRENT_RUN.reset(token)

    assert clients[0].prompts == ["Build eval-abc123-2-digest."]
    assert clients[0].options.model == "claude-haiku-5-5"
    assert outcome.flow_prefix == "eval-abc123-2-"
    assert (tmp_path / "transcript.json").exists()


async def test_scenario_task_needs_a_current_run():
    inputs = ScenarioInputs(prompt="Build it.", user=[])

    with pytest.raises(RuntimeError, match="No run is in progress"):
        await scenario_task(None)(inputs)
