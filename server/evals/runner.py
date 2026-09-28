"""Run a scenario against the fake Cloud API, turn by turn, and check the outcome.

Each run gets a new temporary working directory with the `agentic-workflows`
skill in `.claude/skills/`, and the scenario's files. The agent is Claude Code
in print mode (`claude -p`), started once per turn and resumed with
`--resume` for the next turn. It only loads the `prefect-agentic-workflows`
server from this checkout, and that server and the agent both get a Prefect
profile that points at the fake. The fake listens on 127.0.0.1, so no request
from a run reaches Prefect Cloud.
"""

import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
import uvicorn

from evals.assertions import Check
from evals.fake_cloud import FakeCloud
from evals.record import Reply, Transcript, read_plan_files
from evals.scenario import Outcome, Scenario, next_rule
from prefect_agentic_workflows_mcp.server import SERVER_NAME

SERVER_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SERVER_DIR.parent
SKILL_DIR = REPO_ROOT / "skills" / "agentic-workflows"

ACCOUNT_ID = "eeeeeeee-0000-4000-8000-000000000001"
WORKSPACE_ID = "eeeeeeee-0000-4000-8000-000000000002"
WORKSPACE_PATH = f"/api/accounts/{ACCOUNT_ID}/workspaces/{WORKSPACE_ID}"
FAKE_API_KEY = "pnu_eval_fake_key"
LOCAL_HOST = "127.0.0.1"

ALLOWED_TOOLS = [
    "Read",
    "Write",
    "Edit",
    "Glob",
    "Grep",
    "Skill",
    "Bash(find:*)",
    "Bash(ls:*)",
    "Bash(cat:*)",
    "Bash(mkdir:*)",
    "Bash(prefect config view:*)",
    f"mcp__{SERVER_NAME}",
]
DISALLOWED_TOOLS = ["WebFetch", "WebSearch"]
DEFAULT_AGENT_TURN_LIMIT = 80
AGENT_TURN_TIMEOUT_SECONDS = 900


def asgi_app(fake: FakeCloud) -> Any:
    """Return an ASGI app that passes each HTTP request to `fake.handle`."""

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        body = b""
        more_body = True
        while more_body:
            message = await receive()
            body += message.get("body", b"")
            more_body = message.get("more_body", False)
        query = scope.get("query_string", b"").decode()
        url = f"http://{LOCAL_HOST}{scope['path']}" + (f"?{query}" if query else "")
        request = httpx.Request(
            scope["method"],
            url,
            headers=[(key.decode(), value.decode()) for key, value in scope["headers"]],
            content=body,
        )
        response = fake.handle(request)
        await send(
            {
                "type": "http.response.start",
                "status": response.status_code,
                "headers": [
                    (key.encode(), value.encode())
                    for key, value in response.headers.items()
                    if key.lower() != "content-length"
                ]
                + [(b"content-length", str(len(response.content)).encode())],
            }
        )
        await send({"type": "http.response.body", "body": response.content})

    return app


@contextmanager
def serve(fake: FakeCloud) -> Iterator[str]:
    """Serve the fake on a free port of 127.0.0.1 and yield its workspace API URL."""
    config = uvicorn.Config(
        asgi_app(fake), host=LOCAL_HOST, port=0, log_level="warning", lifespan="off"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline or not thread.is_alive():
            raise RuntimeError("The fake Cloud API server didn't start.")
        time.sleep(0.05)
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"http://{LOCAL_HOST}:{port}{WORKSPACE_PATH}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)


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
    }


def mcp_config(prefect_env: dict[str, str]) -> dict[str, Any]:
    """Return the MCP config that runs this checkout's server against the fake."""
    return {
        "mcpServers": {
            SERVER_NAME: {
                "command": "uv",
                "args": [
                    "run",
                    "--quiet",
                    "--directory",
                    str(SERVER_DIR),
                    "prefect-agentic-workflows-mcp",
                ],
                "env": prefect_env,
            }
        }
    }


def agent_arguments(
    agent: Sequence[str],
    *,
    mcp_config_path: Path,
    session_id: str | None,
    agent_turn_limit: int,
    model: str | None,
) -> list[str]:
    """Return the command for one turn of Claude Code in print mode.

    The message goes to the command's standard input.
    """
    arguments = [
        *agent,
        "-p",
        "--output-format",
        "stream-json",
        "--verbose",
        "--mcp-config",
        str(mcp_config_path),
        "--strict-mcp-config",
        "--setting-sources",
        "project",
        "--permission-mode",
        "acceptEdits",
        f"--allowedTools={','.join(ALLOWED_TOOLS)}",
        f"--disallowedTools={','.join(DISALLOWED_TOOLS)}",
        "--max-turns",
        str(agent_turn_limit),
    ]
    if model is not None:
        arguments += ["--model", model]
    if session_id is not None:
        arguments += ["--resume", session_id]
    return arguments


def prepare_workspace(scenario: Scenario, workspace: Path) -> None:
    """Install the skill and copy the scenario's files into the working directory."""
    shutil.copytree(SKILL_DIR, workspace / ".claude" / "skills" / "agentic-workflows")
    for destination, source in scenario.files.items():
        target = workspace / destination
        if source.is_dir():
            shutil.copytree(source, target)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)


@dataclass
class RunResult:
    scenario: str
    attempt: int
    checks: list[Check] = field(default_factory=list)
    transcript: Transcript = field(default_factory=Transcript)
    workspace: Path | None = None
    log_path: Path | None = None

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)


def run_scenario(
    scenario: Scenario,
    *,
    attempt: int = 1,
    agent: Sequence[str] = ("claude",),
    agent_turn_limit: int = DEFAULT_AGENT_TURN_LIMIT,
    model: str | None = None,
    log_dir: Path | None = None,
    keep_workspace: bool = False,
) -> RunResult:
    """Run one attempt of a scenario and return its checks.

    `agent` is the command that starts Claude Code. When `log_dir` is set,
    the transcript and the plan files are saved there.
    """
    scratch = Path(tempfile.mkdtemp(prefix=f"eval-{scenario.name}-"))
    workspace = scratch / "workspace"
    workspace.mkdir()
    prefect_home = scratch / "prefect-home"
    prefect_home.mkdir()
    prepare_workspace(scenario, workspace)

    fake = FakeCloud(f"http://{LOCAL_HOST}{WORKSPACE_PATH}")
    scenario.setup(fake)
    transcript = Transcript()
    result = RunResult(scenario.name, attempt, transcript=transcript)

    try:
        with serve(fake) as api_url:
            prefect_env = prefect_environment(api_url, prefect_home)
            config_path = scratch / "mcp.json"
            config_path.write_text(json.dumps(mcp_config(prefect_env), indent=2))
            converse(
                scenario,
                transcript,
                agent=agent,
                cwd=workspace,
                env={**os.environ, **prefect_env},
                mcp_config_path=config_path,
                agent_turn_limit=agent_turn_limit,
                model=model,
            )
        outcome = Outcome(transcript, read_plan_files(workspace), fake, workspace)
        result.checks = [
            Check(
                "agent turns finished without error",
                not transcript.errors,
                "; ".join(transcript.errors),
            ),
            *scenario.checks(outcome),
        ]
        if log_dir is not None:
            result.log_path = save_log(log_dir, result, outcome)
    finally:
        if keep_workspace:
            result.workspace = workspace
        else:
            shutil.rmtree(scratch, ignore_errors=True)
    return result


def converse(
    scenario: Scenario,
    transcript: Transcript,
    *,
    agent: Sequence[str],
    cwd: Path,
    env: dict[str, str],
    mcp_config_path: Path,
    agent_turn_limit: int,
    model: str | None,
) -> None:
    """Run agent turns and answer each from the scenario's script until it ends."""
    message = scenario.prompt
    for turn in range(1, scenario.max_turns + 1):
        arguments = agent_arguments(
            agent,
            mcp_config_path=mcp_config_path,
            session_id=transcript.session_id,
            agent_turn_limit=agent_turn_limit,
            model=model,
        )
        try:
            process = subprocess.run(
                arguments,
                input=message,
                cwd=cwd,
                env=env,
                capture_output=True,
                text=True,
                timeout=AGENT_TURN_TIMEOUT_SECONDS,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            transcript.errors.append(f"turn {turn}: {exc}")
            return
        final = transcript.read_events(process.stdout.splitlines(), turn)
        if process.returncode != 0:
            transcript.errors.append(
                f"turn {turn}: the agent exited with {process.returncode}: "
                f"{process.stderr.strip()[-500:]}"
            )
            return
        rule = next_rule(scenario.user, final, transcript)
        if rule is None or rule.reply is None:
            return
        transcript.replies.append(Reply(turn, rule.label, rule.reply))
        message = rule.reply
    transcript.errors.append(
        f"the conversation reached the scenario's limit of {scenario.max_turns} turns"
    )


def save_log(log_dir: Path, result: RunResult, outcome: Outcome) -> Path:
    path = log_dir / result.scenario / f"attempt-{result.attempt}"
    path.mkdir(parents=True, exist_ok=True)
    (path / "transcript.json").write_text(
        json.dumps(outcome.transcript.to_json(), indent=2, default=str)
    )
    (path / "checks.json").write_text(
        json.dumps([vars(check) for check in result.checks], indent=2)
    )
    for name, plan in outcome.plans.items():
        text = plan if isinstance(plan, str) else json.dumps(plan, indent=2)
        (path / name).write_text(text)
    return path
