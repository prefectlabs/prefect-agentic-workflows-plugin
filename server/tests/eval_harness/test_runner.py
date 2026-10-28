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
from evals.record import FlowState, Reply, ToolCall, Transcript, read_plan_files
from evals.runner import converse, scenario_task, talk
from evals.sandbox import Credentials, SandboxApi
from evals.scenario import Rule, ScenarioInputs, next_rule
from evals.scenarios import (
    no_infrastructure,
    rejected_approval,
    scheduled_edit,
    unsupported_loop,
)
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


@pytest.mark.parametrize(
    "scenario",
    [
        no_infrastructure,
        rejected_approval,
        release_notes,
        scheduled_edit,
        unsupported_loop,
    ],
)
@pytest.mark.parametrize(
    ("message", "expected"),
    [
        (INFRASTRUCTURE_QUESTION, True),
        ("Do you have a remote MCP server for each business tool?", True),
        ("Which web addresses can this workflow use?", True),
        ("For each of these, do you have that address?", True),
        ("For Zendesk and Slack, do you have those addresses?", True),
        ("For each of these, do you have such an address?", True),
        (
            "For this workflow, do you have a remote MCP server address for:\n"
            "1. **Zendesk**, to read new tickets?\n2. **Slack**, to post?",
            True,
        ),
        ("I don't have that address. Do you approve the design?", False),
        ("We have those addresses. Should I build it?", False),
        ("The workflow has no business tools. Do you approve the design?", False),
        ("No remote MCP server is needed. Should I build it?", False),
        ("The MCP inventory is empty.\n\nReply yes to approve this design.", False),
    ],
)
def test_infrastructure_rule_answers_questions_not_design_summaries(
    scenario: Any, message: str, expected: bool
):
    rule = next_rule(scenario.USER, message, Transcript())

    assert (
        bool(rule and rule.label in {"reachable-systems", "no-remote-servers"})
        is expected
    )


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


async def test_no_infrastructure_answers_a_follow_up_address_question():
    question = (
        "An agent step in Prefect Cloud can only use a business tool through a "
        "remote MCP server: a web address that lets the agent use that tool.\n\n"
        "For each of these, do you have that address?"
    )
    agent = ScriptedAgent(
        [result(question)],
        [result("Paste the Zendesk tickets in, then copy the summary to Slack.")],
    )
    transcript = Transcript()

    await talk(
        agent,
        transcript,
        "Build the support summary workflow.",
        no_infrastructure.USER,
        max_turns=3,
    )

    assert agent.prompts == [
        "Build the support summary workflow.",
        no_infrastructure.NO_SERVERS,
    ]
    assert transcript.replies == [
        Reply(1, "no-remote-servers", no_infrastructure.NO_SERVERS)
    ]
    assert len(transcript.turn_results) == 2


@pytest.mark.parametrize(
    ("summary", "label"),
    [
        ("", "conversion-decisions"),
        ("## Summary\nInputs, outputs, steps, and model.\nApprove?", "design-approval"),
        (
            "## Plan summary\nInputs, outputs, steps, and model.\nApprove?",
            "design-approval",
        ),
        (
            "## Summary of the planned workflow\n"
            "Inputs, outputs, steps, and model.\nApprove?",
            "design-approval",
        ),
        (
            "**Workflow summary**\nInputs, outputs, steps, and model.\nApprove?",
            "design-approval",
        ),
    ],
)
def test_release_notes_distinguishes_conversion_decisions_from_design_approval(
    summary: str, label: str
):
    message = (
        "Here is the conversion report. Please decide each row. "
        "I'll write the summary after that.\n\n"
        "## Conversion report: release-notes\n\n"
        "### Parts that need a decision\n"
        "Use two check passes and a remote tool for collect_changes.\n\n"
        "Can you confirm the token scopes?\n\n"
        f"{summary}"
    )
    transcript = Transcript(replies=[Reply(1, "reachable-systems", "")])

    rule = next_rule(release_notes.USER, message, transcript)

    assert rule is not None and rule.label == label
    if label == "conversion-decisions":
        assert rule.reply is not None
        assert "before writing or validating" in rule.reply
        assert "I approve the design" not in rule.reply


async def test_release_notes_approves_report_choices_before_the_design():
    report = (
        "Here is the conversion report. Please decide each row. "
        "I'll write the summary after that.\n\n"
        "## Conversion report: release-notes\n\n"
        "### Parts that need a decision\n"
        "Use two check passes and a remote tool for collect_changes.\n\n"
        "Can you confirm the token scopes?"
    )
    summary = "## Summary: release-notes\nInputs, outputs, steps, and model.\nApprove?"
    agent = ScriptedAgent(
        [result("Which business tools can this workflow reach?")],
        [result(report)],
        [result(summary)],
        [
            AssistantMessage(
                [
                    ToolUseBlock(
                        "validate-1",
                        "mcp__prefect-agentic-workflows__validate_plan",
                        {"plan": {}},
                    )
                ],
                model="m",
            ),
            result("Validated."),
        ],
    )
    transcript = Transcript()

    await talk(agent, transcript, "Convert the skill.", release_notes.USER, max_turns=5)

    assert [reply.label for reply in transcript.replies] == [
        "reachable-systems",
        "conversion-decisions",
        "design-approval",
    ]
    assert transcript.first_reply("design-approval") == Reply(
        3, "design-approval", release_notes.DESIGN_APPROVAL
    )
    assert transcript.tool_calls[0].turn == 4


async def test_rejected_approval_summary_gets_design_approval_before_validation():
    summary = (
        "The infrastructure check is done. This workflow uses no business tools, "
        "so there's nothing to reach.\n\n"
        "Reply with **yes** to approve this design, and tell me the model choice. "
        "I'll write the plan file, validate it, and publish it after that."
    )
    agent = ScriptedAgent(
        [result(summary)],
        [
            AssistantMessage(
                [
                    ToolUseBlock(
                        "validate-1",
                        "mcp__prefect-agentic-workflows__validate_plan",
                        {"plan": {}},
                    )
                ],
                model="m",
            ),
            result("The plan is valid."),
        ],
    )
    transcript = Transcript()

    await talk(
        agent,
        transcript,
        "Build the feedback workflow.",
        rejected_approval.USER,
        max_turns=3,
    )

    assert agent.prompts == ["Build the feedback workflow.", "Yes, build it."]
    assert transcript.replies == [Reply(1, "design-approval", "Yes, build it.")]
    assert transcript.tool_calls[0].name == "validate_plan"
    assert transcript.tool_calls[0].turn == 2


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


@pytest.mark.parametrize("directory", ["workflows", "release-notes/workflows"])
@pytest.mark.parametrize(
    ("content", "expected"),
    [
        (
            '{"nodes": {"review": {"kind": "HumanInputNode"}}}',
            {"nodes": {"review": {"kind": "HumanInputNode"}}},
        ),
        ("not valid JSON", "not valid JSON"),
    ],
)
def test_read_plan_files_in_workflow_directories(
    tmp_path: Path, directory: str, content: str, expected: Any
):
    path = tmp_path / directory / "release-notes.plan.json"
    path.parent.mkdir(parents=True)
    path.write_text(content)

    assert read_plan_files(tmp_path) == {
        f"{directory}/release-notes.plan.json": expected
    }


def test_read_plan_files_keeps_identical_names_in_different_directories(tmp_path: Path):
    for directory in ["workflows", "release-notes/workflows"]:
        path = tmp_path / directory / "release-notes.plan.json"
        path.parent.mkdir(parents=True)
        path.write_text("{}")

    assert list(read_plan_files(tmp_path)) == [
        "release-notes/workflows/release-notes.plan.json",
        "workflows/release-notes.plan.json",
    ]


def test_read_plan_files_excludes_installed_skills_and_source_plans(tmp_path: Path):
    for name in [
        ".claude/skills/example/workflows/example.plan.json",
        "release-notes/example.plan.json",
    ]:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}")

    assert read_plan_files(tmp_path) == {}


def test_read_plan_files_excludes_symlinks_outside_workspace(tmp_path: Path):
    workspace = tmp_path / "agent"
    workflows = workspace / "workflows"
    workflows.mkdir(parents=True)
    outside = tmp_path / "outside.plan.json"
    outside.write_text("{}")
    (workflows / "linked.plan.json").symlink_to(outside)

    assert read_plan_files(workspace) == {}


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


async def test_converse_collects_a_nested_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(
        runner, "ClaudeSDKClient", stub_client([], [result("Published.")])
    )
    plan = {"nodes": {"review": {"kind": "HumanInputNode"}}}
    source = tmp_path / "source.plan.json"
    source.write_text(json.dumps(plan))
    name = "release-notes/workflows/release-notes.plan.json"

    outcome = await converse(
        tmp_path / "run",
        "Convert the skill.",
        [Rule(label="end", reply=None)],
        sandbox=StubSandbox(),
        flow_prefix="eval-abc123-1-",
        files={name: source},
    )

    assert outcome.plans == {name: plan}
    assert outcome.plan["nodes"]["review"]["kind"] == "HumanInputNode"


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


@pytest.mark.parametrize(
    ("message", "label"),
    [
        ("I checked the MCP inventory and will design this next.", "end"),
        (
            "Which of these tools has a remote MCP server you can reach?",
            "no-remote-servers",
        ),
    ],
    ids=["mention", "question"],
)
def test_no_infrastructure_answers_only_a_question_about_servers(
    message: str, label: str
):
    rule = next_rule(no_infrastructure.USER, message, Transcript())

    assert rule is not None and rule.label == label
