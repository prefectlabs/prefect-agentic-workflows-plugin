"""Tests for the runner, with the fake served over HTTP and a stub agent."""

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from fastmcp import Client
from harness_plans import approval_plan
from prefect.settings import PREFECT_API_URL, temporary_settings

from evals import assertions
from evals.__main__ import format_table
from evals.assertions import Check
from evals.fake_cloud import FakeCloud
from evals.runner import (
    LOCAL_HOST,
    WORKSPACE_PATH,
    RunResult,
    agent_arguments,
    prefect_environment,
    run_scenario,
    serve,
)
from evals.scenario import Outcome, Rule, Scenario
from evals.scenarios import SCENARIOS
from prefect_agentic_workflows_mcp.server import build_server

STUB_AGENT = Path(__file__).with_name("stub_agent.py")


async def test_the_server_passes_its_preflight_check_against_the_served_fake():
    fake = FakeCloud(f"http://{LOCAL_HOST}{WORKSPACE_PATH}")

    with serve(fake) as api_url, temporary_settings(updates={PREFECT_API_URL: api_url}):
        async with Client(build_server()) as client:
            result = await client.call_tool("get_or_create_flow", {"name": "release"})

    assert api_url.startswith(f"http://{LOCAL_HOST}:")
    assert result.structured_content is not None
    assert result.structured_content["created"] is True
    assert [(request.method, request.path) for request in fake.requests] == [
        ("GET", "/execution-plans/schema"),
        ("GET", "/flows/name/release"),
        ("POST", "/flows/"),
    ]


def test_prefect_environment_refuses_an_api_url_that_is_not_local(tmp_path: Path):
    with pytest.raises(ValueError, match="only run against a local fake"):
        prefect_environment(
            "https://api.prefect.cloud/api/accounts/a/workspaces/w", tmp_path
        )


def stub_checks(outcome: Outcome) -> list[Check]:
    calls = outcome.transcript.tool_calls
    return [
        assertions.check_no_cycle(outcome.plan),
        assertions.check_published_only_after_valid(calls),
        Check(
            "design approved once",
            len(outcome.transcript.replies_labeled("design-approval")) == 1,
        ),
        Check(
            "the agent read the schema from the fake",
            any(
                request.path == "/execution-plans/schema"
                for request in outcome.fake.requests
            ),
        ),
        Check(
            "the skill and the scenario files are in the working directory",
            (outcome.workspace / ".claude/skills/agentic-workflows/SKILL.md").exists()
            and (outcome.workspace / "notes.txt").read_text() == "notes",
        ),
    ]


def stub_scenario(tmp_path: Path) -> Scenario:
    notes = tmp_path / "notes.txt"
    notes.write_text("notes")
    return Scenario(
        name="stub",
        description="A conversation with the stub agent.",
        prompt="Convert the demo skill.",
        user=[
            Rule(
                label="design-approval",
                pattern=r"approve the design",
                reply="Yes.",
                before_tool="validate_plan",
            ),
        ],
        checks=stub_checks,
        files={"notes.txt": notes},
    )


def test_run_scenario_runs_the_conversation_and_the_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(approval_plan()))
    monkeypatch.setenv("STUB_AGENT_PLAN", str(plan_path))

    result = run_scenario(
        stub_scenario(tmp_path),
        agent=[sys.executable, str(STUB_AGENT)],
        log_dir=tmp_path / "logs",
    )

    assert [check for check in result.checks if not check.passed] == []
    assert result.passed is True
    assert result.transcript.session_id == "stub-session"
    assert [reply.label for reply in result.transcript.replies] == ["design-approval"]
    assert [call.name for call in result.transcript.tool_calls] == [
        "validate_plan",
        "publish_plan",
    ]
    assert result.log_path is not None
    saved: dict[str, Any] = json.loads((result.log_path / "demo.plan.json").read_text())
    assert saved == approval_plan()


def test_run_scenario_fails_when_the_agent_command_fails(tmp_path: Path):
    result = run_scenario(
        stub_scenario(tmp_path), agent=[sys.executable, "-c", "raise SystemExit(3)"]
    )

    assert result.passed is False
    assert "exited with 3" in result.checks[0].detail


def test_format_table_reports_the_pass_rate_and_each_failed_check():
    results = [
        RunResult("convert", 1, [Check("no cycle", True)]),
        RunResult("convert", 2, [Check("no cycle", False, "nodes: a, b")]),
    ]

    assert format_table(results) == (
        "Scenario  Runs  Passed  Pass rate  Result\n"
        "convert   2     1       50%        FAIL\n"
        "\n"
        "convert, attempt 2:\n"
        "  FAIL no cycle: nodes: a, b"
    )


def test_the_release_notes_scenario_is_registered():
    scenario = SCENARIOS["release-notes-conversion"]

    assert (scenario.files["release-notes"] / "SKILL.md").exists()


def test_the_agent_may_load_the_skill_with_the_skill_tool(tmp_path: Path):
    arguments = agent_arguments(
        ["claude"],
        mcp_config_path=tmp_path / "mcp.json",
        session_id=None,
        agent_turn_limit=10,
        model=None,
    )

    allowed = next(item for item in arguments if item.startswith("--allowedTools="))
    assert "Skill" in allowed.removeprefix("--allowedTools=").split(",")
