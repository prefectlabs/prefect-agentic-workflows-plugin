"""Tests for `python -m evals`, with the sandbox and the agent replaced by stubs."""

import os
from pathlib import Path
from typing import Any, ClassVar

import pytest
from support import API_KEY, WORKSPACE_API_URL

from evals import __main__ as cli
from evals.lifecycle import current_run
from evals.record import Transcript
from evals.sandbox import Credentials, SandboxApi
from evals.scenario import Outcome, ScenarioInputs


def test_parse_args_defaults():
    args = cli.parse_args([])

    assert args.repeat == 1
    assert args.model is None
    assert args.max_concurrency == cli.DEFAULT_MAX_CONCURRENCY
    assert args.cases is None
    assert args.summary is None
    assert args.judge is True
    assert args.judge_model == "anthropic:claude-haiku-5-5"
    assert cli.judge_model(args) == "anthropic:claude-haiku-5-5"


def test_parse_args_judge_options():
    assert cli.judge_model(cli.parse_args(["--no-judge"])) is None
    args = cli.parse_args(["--judge-model", "anthropic:claude-sonnet-5"])
    assert cli.judge_model(args) == "anthropic:claude-sonnet-5"


def test_parse_args_takes_several_cases():
    args = cli.parse_args(
        [
            "--repeat",
            "3",
            "--model",
            "claude-haiku-5-5",
            "--max-concurrency",
            "2",
            "--case",
            "scheduled_edit",
            "rejected_approval",
            "--case",
            "no_infrastructure",
            "--summary",
            "report.md",
        ]
    )

    assert args.repeat == 3
    assert args.model == "claude-haiku-5-5"
    assert args.max_concurrency == 2
    assert args.cases == ["scheduled_edit", "rejected_approval", "no_infrastructure"]
    assert args.summary == Path("report.md")


@pytest.mark.parametrize(
    "argv", [["--case", "unknown"], ["--repeat", "0"], ["--max-concurrency", "0"]]
)
def test_parse_args_rejects_bad_values(argv: list[str]):
    with pytest.raises(SystemExit):
        cli.parse_args(argv)


def test_main_refuses_to_start_without_the_sandbox(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    monkeypatch.delenv("PREFECT_API_URL")

    assert cli.main([]) == cli.USAGE_ERROR
    assert "PREFECT_API_URL" in capsys.readouterr().err


def test_main_refuses_to_start_the_judges_without_an_anthropic_key(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    monkeypatch.setenv("PREFECT_API_URL", WORKSPACE_API_URL)
    monkeypatch.setenv("PREFECT_API_KEY", API_KEY)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    assert cli.main([]) == cli.USAGE_ERROR
    assert "--no-judge" in capsys.readouterr().err


class StubSandbox(SandboxApi):
    instances: ClassVar[list["StubSandbox"]] = []

    def __init__(self, credentials: Credentials) -> None:
        super().__init__(credentials)
        self.deleted_prefixes: list[str] = []
        StubSandbox.instances.append(self)

    def delete_flows_with_prefix(self, prefix: str) -> list[str]:
        self.deleted_prefixes.append(prefix)
        return []


def stub_task(model: str | None) -> Any:
    async def task(inputs: ScenarioInputs) -> Outcome:
        run = current_run()
        return Outcome(Transcript(), {}, {}, run.directory, run.flow_prefix)

    return task


def test_main_writes_the_summary_and_fails_when_a_check_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    monkeypatch.setenv("PREFECT_API_URL", WORKSPACE_API_URL)
    monkeypatch.setenv("PREFECT_API_KEY", API_KEY)
    monkeypatch.setenv("PREFECT_PROFILE", "local")
    # `main` deletes every PREFECT_ variable. Set each one through
    # monkeypatch, so it comes back after the test.
    for name in [name for name in os.environ if name.startswith("PREFECT_")]:
        monkeypatch.setenv(name, os.environ[name])
    monkeypatch.setattr(cli, "SandboxApi", StubSandbox)
    monkeypatch.setattr(cli, "scenario_task", stub_task)
    summary = tmp_path / "summary.md"

    code = cli.main(
        [
            "--case",
            "no_infrastructure",
            "--repeat",
            "2",
            "--no-judge",
            "--summary",
            str(summary),
            "--output-dir",
            str(tmp_path / "runs"),
        ]
    )

    # The stub agent did nothing, so the scenario's checks fail.
    assert code == 1
    text = summary.read_text()
    assert "| `no_infrastructure` | 0 | 2 | 0% |" in text
    assert "LLM judges: off." in text
    assert "the agent's first question is the infrastructure check" in text
    sandbox = StubSandbox.instances[-1]
    session_prefix = sandbox.deleted_prefixes[-1]
    assert sorted(sandbox.deleted_prefixes) == [
        session_prefix,
        f"{session_prefix}1-",
        f"{session_prefix}2-",
    ]
    session_dir = tmp_path / "runs" / session_prefix.rstrip("-")
    assert sorted(path.name for path in session_dir.iterdir()) == [
        "1-no_infrastructure",
        "2-no_infrastructure",
    ]
    # The agent gets only the Prefect settings the runner passes.
    assert not any(name.startswith("PREFECT_") for name in os.environ)
