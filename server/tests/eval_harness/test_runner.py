"""Tests for the runner, with the fake served over HTTP and a scripted agent."""

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
from fastmcp import Client
from prefect.settings import PREFECT_API_URL, temporary_settings

from evals.fake_cloud import FakeCloud
from evals.record import Reply, ToolCall, Transcript, read_plan_files
from evals.runner import (
    LOCAL_HOST,
    WORKSPACE_PATH,
    agent_options,
    prefect_environment,
    serve,
    talk,
)
from evals.scenario import Rule
from prefect_agentic_workflows_mcp.server import SERVER_NAME, build_server


async def test_the_server_passes_its_preflight_check_against_the_served_fake():
    fake = FakeCloud(f"http://{LOCAL_HOST}{WORKSPACE_PATH}")

    with serve(fake) as api_url, temporary_settings(updates={PREFECT_API_URL: api_url}):
        async with Client(build_server()) as client:
            result = await client.call_tool("get_or_create_flow", {"name": "release"})

    assert api_url.startswith(f"http://{LOCAL_HOST}:")
    assert result.structured_content is not None
    assert result.structured_content["created"] is True
    assert [flow["name"] for flow in fake.flows.values()] == ["release"]


def test_prefect_environment_refuses_an_api_url_that_is_not_local(tmp_path: Path):
    with pytest.raises(ValueError, match="only run against a local fake"):
        prefect_environment(
            "https://api.prefect.cloud/api/accounts/a/workspaces/w", tmp_path
        )


def test_agent_options_load_only_the_skill_and_the_server(tmp_path: Path):
    env = prefect_environment(f"http://{LOCAL_HOST}:1{WORKSPACE_PATH}", tmp_path)

    options = agent_options(tmp_path, env, model=None)

    assert options.skills == ["agentic-workflows"]
    assert options.setting_sources == ["project"]
    assert options.strict_mcp_config is True
    assert isinstance(options.mcp_servers, dict)
    assert list(options.mcp_servers) == [SERVER_NAME]
    assert options.mcp_servers[SERVER_NAME].get("env") == env
    assert options.env == env


def result(text: str, *, is_error: bool = False) -> ResultMessage:
    return ResultMessage(
        subtype="error_during_execution" if is_error else "success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=is_error,
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


PLAN = {"kind": "ExecutionPlan"}
FIRST_TURN: list[Message] = [
    AssistantMessage([TextBlock("I'll validate the plan.")], model="m"),
    AssistantMessage(
        [
            ToolUseBlock(
                "call-1",
                "mcp__prefect-agentic-workflows__validate_plan",
                {"plan": PLAN},
            ),
            ToolUseBlock("call-2", "Read", {"file_path": "SKILL.md"}),
        ],
        model="m",
    ),
    UserMessage(
        [
            ToolResultBlock("call-1", [{"type": "text", "text": '{"valid": true}'}]),
            ToolResultBlock("call-2", "No such file", is_error=True),
        ]
    ),
    result("The plan is valid. Do you approve?"),
]
RULES = [
    Rule(label="approve", pattern=r"approve", reply="Yes."),
    Rule(label="end", reply=None),
]


async def test_talk_records_tool_calls_and_answers_from_the_rules():
    agent = ScriptedAgent(FIRST_TURN, [result("Published.")])
    transcript = Transcript()

    await talk(agent, transcript, "Convert the skill.", RULES, max_turns=5)

    assert agent.prompts == ["Convert the skill.", "Yes."]
    assert transcript.tool_calls == [
        ToolCall("validate_plan", {"plan": PLAN}, {"valid": True}, False, 1),
        ToolCall("Read", {"file_path": "SKILL.md"}, "No such file", True, 1),
    ]
    assert transcript.agent_text == ["I'll validate the plan."]
    assert transcript.turn_results == [
        "The plan is valid. Do you approve?",
        "Published.",
    ]
    assert transcript.replies == [Reply(1, "approve", "Yes.")]
    assert transcript.cost_usd == pytest.approx(0.02)
    assert transcript.errors == []


async def test_talk_stops_at_an_error_result():
    agent = ScriptedAgent([result("API error", is_error=True)])
    transcript = Transcript()

    await talk(agent, transcript, "Convert the skill.", RULES, max_turns=5)

    assert transcript.errors == ["turn 1: API error"]
    assert transcript.replies == []


async def test_talk_reports_a_conversation_that_reaches_the_turn_limit():
    agent = ScriptedAgent(*[[result("Do you approve?")] for _ in range(2)])
    transcript = Transcript()

    await talk(agent, transcript, "Convert the skill.", RULES, max_turns=2)

    assert transcript.errors == [
        "the conversation reached the scenario's limit of 2 turns"
    ]


def test_read_plan_files_reads_every_plan_in_the_workflows_directory(tmp_path: Path):
    workflows = tmp_path / "workflows"
    workflows.mkdir()
    (workflows / "release-notes.plan.json").write_text('{"kind": "ExecutionPlan"}')
    (workflows / "broken.plan.json").write_text("{")
    (workflows / "notes.md").write_text("ignored")

    plans: dict[str, Any] = read_plan_files(tmp_path)

    assert plans == {
        "broken.plan.json": "{",
        "release-notes.plan.json": {"kind": "ExecutionPlan"},
    }


def test_first_reply_and_final_message():
    transcript = Transcript(
        turn_results=["First.", "Second."],
        replies=[Reply(1, "design", "Yes."), Reply(2, "design", "Still yes.")],
    )

    assert transcript.first_reply("design") == Reply(1, "design", "Yes.")
    assert transcript.first_reply("run") is None
    assert transcript.final_message(2) == "Second."
    assert transcript.final_message(3) == ""
