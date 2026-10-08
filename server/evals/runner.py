"""Run a conversation between the agent and the simulated user against the fake Cloud.

The agent is Claude Code, driven with the Claude Agent SDK. Each run gets a
working directory with the `agentic-workflows` skill in `.claude/skills/` and
the scenario's files. The agent loads only project settings, that skill, and
the `prefect-agentic-workflows` server from this checkout. The server and the
agent get a Prefect profile that points at the fake, which listens on
127.0.0.1, so no request from a run reaches Prefect Cloud.
"""

import asyncio
import json
import shutil
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx
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

from evals.fake_cloud import FakeCloud
from evals.record import Reply, ToolCall, Transcript, read_plan_files
from evals.scenario import Outcome, Rule, next_rule
from prefect_agentic_workflows_mcp.server import SERVER_NAME
from prefect_agentic_workflows_mcp.workspace_api import ALLOW_LOOPBACK_VARIABLE

SERVER_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SERVER_DIR.parent
SKILL_NAME = "agentic-workflows"
SKILL_DIR = REPO_ROOT / "skills" / SKILL_NAME

ACCOUNT_ID = "eeeeeeee-0000-4000-8000-000000000001"
WORKSPACE_ID = "eeeeeeee-0000-4000-8000-000000000002"
WORKSPACE_PATH = f"/api/accounts/{ACCOUNT_ID}/workspaces/{WORKSPACE_ID}"
FAKE_API_KEY = "pnu_eval_fake_key"
LOCAL_HOST = "127.0.0.1"
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


@contextmanager
def serve(fake: FakeCloud) -> Iterator[str]:
    """Serve the fake on a free port of 127.0.0.1 and yield its workspace API URL."""

    class Handler(BaseHTTPRequestHandler):
        def answer(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            request = httpx.Request(
                self.command,
                f"http://{LOCAL_HOST}{self.path}",
                headers=dict(self.headers),
                content=self.rfile.read(length),
            )
            response = fake.handle(request)
            self.send_response(response.status_code)
            for key, value in response.headers.items():
                if key.lower() != "content-length":
                    self.send_header(key, value)
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)

        do_GET = do_POST = do_PATCH = do_DELETE = answer

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer((LOCAL_HOST, 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://{LOCAL_HOST}:{server.server_port}{WORKSPACE_PATH}"
    finally:
        server.shutdown()
        server.server_close()


def prefect_environment(api_url: str, prefect_home: Path) -> dict[str, str]:
    """Return the Prefect settings that point the server and the agent at the fake.

    Raises `ValueError` for an API URL that isn't on 127.0.0.1, so a run
    can't send requests to a real workspace.
    """
    if urlsplit(api_url).hostname != LOCAL_HOST:
        raise ValueError(f"Evaluations only run against a local fake, not {api_url}.")
    return {
        "PREFECT_API_URL": api_url,
        "PREFECT_API_KEY": FAKE_API_KEY,
        "PREFECT_HOME": str(prefect_home),
        "PREFECT_PROFILES_PATH": str(prefect_home / "profiles.toml"),
        # The server only accepts a loopback API URL when this is set.
        ALLOW_LOOPBACK_VARIABLE: "1",
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
    files: dict[str, Path] | None = None,
    setup: Callable[[FakeCloud], None] | None = None,
    max_turns: int = 12,
    model: str | None = None,
) -> Outcome:
    """Run the conversation in `workspace` against a new fake Cloud.

    `workspace` must be an empty directory. The agent works in
    `workspace/agent`, and its Prefect home is `workspace/prefect-home`. The
    transcript is saved to `workspace/transcript.json`.
    """
    agent_dir = workspace / "agent"
    prefect_home = workspace / "prefect-home"
    prefect_home.mkdir(parents=True)
    prepare_workspace(agent_dir, files or {})
    fake = FakeCloud(f"http://{LOCAL_HOST}{WORKSPACE_PATH}")
    if setup is not None:
        setup(fake)
    transcript = Transcript()
    with serve(fake) as api_url:
        options = agent_options(
            agent_dir, prefect_environment(api_url, prefect_home), model
        )
        async with ClaudeSDKClient(options) as client:
            await talk(client, transcript, prompt, user, max_turns)
    (workspace / "transcript.json").write_text(
        json.dumps(asdict(transcript), indent=2, default=str)
    )
    return Outcome(transcript, read_plan_files(agent_dir), fake, agent_dir)
