"""Run a conversation between the agent and the simulated user in the sandbox.

The agent is Claude Code, driven with the Claude Agent SDK. Each run gets a
working directory with the `agentic-workflows` skill in `.claude/skills/` and
the scenario's files. The agent loads only project settings, that skill, and
the `prefect-agentic-workflows` server from this checkout. The server and the
agent get the sandbox workspace's API URL and key, and an empty Prefect home,
so no profile on the machine is used.
"""

import asyncio
import json
import shutil
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any, Protocol

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    Message,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from claude_agent_sdk.types import McpStdioServerConfig

from evals.lifecycle import current_run
from evals.record import Reply, ToolCall, Transcript, read_plan_files
from evals.sandbox import Credentials, SandboxApi
from evals.scenario import Outcome, Rule, ScenarioInputs, next_rule
from prefect_agentic_workflows_mcp.server import SERVER_NAME

SERVER_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SERVER_DIR.parent
SKILL_NAME = "agentic-workflows"
SKILL_DIR = REPO_ROOT / "skills" / SKILL_NAME
SERVER_TOOL_PREFIX = f"mcp__{SERVER_NAME}__"

ALLOWED_TOOLS = [
    "Read",
    "Write",
    "Edit",
    "Glob",
    "Grep",
    "Bash(find:*)",
    "Bash(ls:*)",
    "Bash(cat:*)",
    "Bash(mkdir:*)",
    "Bash(prefect config view:*)",
    f"mcp__{SERVER_NAME}",
]
DISALLOWED_TOOLS = ["WebFetch", "WebSearch"]
AGENT_TURN_LIMIT = 80
AGENT_TURN_TIMEOUT_SECONDS = 900


def prefect_environment(credentials: Credentials, prefect_home: Path) -> dict[str, str]:
    """Return the Prefect settings that point the server and agent at the sandbox."""
    return {
        "PREFECT_API_URL": credentials.api_url,
        "PREFECT_API_KEY": credentials.api_key,
        "PREFECT_HOME": str(prefect_home),
        "PREFECT_PROFILES_PATH": str(prefect_home / "profiles.toml"),
    }


def agent_options(
    workspace: Path, prefect_env: dict[str, str], model: str | None
) -> ClaudeAgentOptions:
    """Return the SDK options for an agent with the skill and this checkout's server."""
    server = McpStdioServerConfig(
        command="uv",
        args=[
            "run",
            "--quiet",
            "--directory",
            str(SERVER_DIR),
            "prefect-agentic-workflows-mcp",
        ],
        env=prefect_env,
    )
    return ClaudeAgentOptions(
        cwd=workspace,
        system_prompt={"type": "preset", "preset": "claude_code"},
        setting_sources=["project"],
        skills=[SKILL_NAME],
        mcp_servers={SERVER_NAME: server},
        strict_mcp_config=True,
        permission_mode="acceptEdits",
        allowed_tools=ALLOWED_TOOLS,
        disallowed_tools=DISALLOWED_TOOLS,
        max_turns=AGENT_TURN_LIMIT,
        model=model,
        env=prefect_env,
    )


def prepare_workspace(workspace: Path, files: dict[str, Path]) -> None:
    """Install the skill and copy the scenario's files into the working directory."""
    shutil.copytree(SKILL_DIR, workspace / ".claude" / "skills" / SKILL_NAME)
    for destination, source in files.items():
        target = workspace / destination
        if source.is_dir():
            shutil.copytree(source, target)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)


def parse_result(content: Any) -> Any:
    """Return a tool result's content as JSON when it parses, and as text otherwise."""
    if isinstance(content, list):
        content = "".join(
            str(block.get("text", ""))
            for block in content
            if block.get("type") == "text"
        )
    try:
        return json.loads(content or "")
    except ValueError:
        return content


class Agent(Protocol):
    """The part of `ClaudeSDKClient` a conversation uses."""

    async def query(self, prompt: str) -> None: ...

    def receive_response(self) -> Any: ...


async def record_turn(agent: Agent, transcript: Transcript, turn: int) -> str:
    """Record one agent turn's text and tool calls, and return its final message."""
    pending: dict[str, ToolCall] = {}
    final = ""
    message: Message
    async for message in agent.receive_response():
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock) and block.text:
                    transcript.agent_text.append(block.text)
                elif isinstance(block, ToolUseBlock):
                    call = ToolCall(
                        block.name.removeprefix(SERVER_TOOL_PREFIX),
                        block.input,
                        turn=turn,
                    )
                    pending[block.id] = call
                    transcript.tool_calls.append(call)
        elif isinstance(message, UserMessage) and isinstance(message.content, list):
            for block in message.content:
                if isinstance(block, ToolResultBlock) and block.tool_use_id in pending:
                    call = pending.pop(block.tool_use_id)
                    call.result = parse_result(block.content)
                    call.is_error = bool(block.is_error)
        elif isinstance(message, ResultMessage):
            final = message.result or ""
            transcript.cost_usd += message.total_cost_usd or 0.0
            if message.is_error:
                transcript.errors.append(f"turn {turn}: {final or message.subtype}")
    transcript.turn_results.append(final)
    return final


async def talk(
    agent: Agent, transcript: Transcript, prompt: str, user: list[Rule], max_turns: int
) -> None:
    """Send agent turns, and answer each from the simulated user's rules."""
    message = prompt
    for turn in range(1, max_turns + 1):
        await agent.query(message)
        try:
            final = await asyncio.wait_for(
                record_turn(agent, transcript, turn), AGENT_TURN_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            transcript.errors.append(f"turn {turn} timed out")
            return
        if transcript.errors:
            return
        rule = next_rule(user, final, transcript)
        if rule is None or rule.reply is None:
            return
        transcript.replies.append(Reply(turn, rule.label, rule.reply))
        message = rule.reply
    transcript.errors.append(
        f"the conversation reached the scenario's limit of {max_turns} turns"
    )


async def converse(
    workspace: Path,
    prompt: str,
    user: list[Rule],
    *,
    sandbox: SandboxApi,
    flow_prefix: str,
    files: dict[str, Path] | None = None,
    max_turns: int = 12,
    model: str | None = None,
) -> Outcome:
    """Run the conversation in `workspace` against the sandbox.

    `workspace` must be an empty directory. The agent works in
    `workspace/agent`, and its Prefect home is `workspace/prefect-home`. The
    transcript is saved to `workspace/transcript.json`, also when the agent
    raises. After the conversation, the state of each flow whose name starts
    with `flow_prefix` is read into the outcome.
    """
    agent_dir = workspace / "agent"
    prefect_home = workspace / "prefect-home"
    prefect_home.mkdir(parents=True)
    prepare_workspace(agent_dir, files or {})
    transcript = Transcript()
    options = agent_options(
        agent_dir, prefect_environment(sandbox.credentials, prefect_home), model
    )
    try:
        async with ClaudeSDKClient(options) as client:
            await talk(client, transcript, prompt, user, max_turns)
    finally:
        (workspace / "transcript.json").write_text(
            json.dumps(asdict(transcript), indent=2, default=str)
        )
    return Outcome(
        transcript,
        read_plan_files(agent_dir),
        sandbox.snapshot(flow_prefix),
        agent_dir,
        flow_prefix,
    )


def scenario_task(model: str | None) -> Callable[[ScenarioInputs], Awaitable[Outcome]]:
    """Return the task the dataset evaluates: one conversation for one case.

    The task reads the run's directory, flow prefix, and sandbox from
    `current_run()`, which `SandboxLifecycle.setup` sets before the task
    starts. `model` is the agent's model, or None for the SDK's default.
    """

    async def run_scenario(inputs: ScenarioInputs) -> Outcome:
        run = current_run()
        return await converse(
            run.directory,
            inputs.prompt_for(run.flow_prefix),
            inputs.user,
            sandbox=run.sandbox,
            flow_prefix=run.flow_prefix,
            files=inputs.files,
            max_turns=inputs.max_turns,
            model=model,
        )

    return run_scenario
