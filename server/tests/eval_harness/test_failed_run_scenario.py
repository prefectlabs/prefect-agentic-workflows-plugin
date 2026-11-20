"""Tests for the failed-run scenario: its setup, its prompt, and its checks.

The setup runs against a stub sandbox. The checks run on a hand-written
outcome that passes, and on a broken one that should fail exactly the listed
checks.
"""

import re
from typing import Any

import pytest
from harness_outcomes import PREFIX, outcome
from support import API_KEY

from evals.record import ToolCall, Transcript
from evals.sandbox import Credentials, SandboxApi
from evals.scenarios import failed_run_diagnosis as scenario

API_URL = "https://api.prefect.cloud/api/accounts/account-1/workspaces/workspace-1"
FAILURE = {
    "code": "internal_error",
    "message": "Something went wrong while running this node.",
    "action": "Try again.",
    "diagnostic_id": "task-run-1",
}


class FailingRunSandbox(SandboxApi):
    """Answers the setup's requests with a run whose node ended as `node_status`."""

    def __init__(self, node_status: str = "failed") -> None:
        super().__init__(Credentials(API_URL, API_KEY))
        self.node_status = node_status
        self.published: list[dict[str, Any]] = []

    def create_flow(self, name: str) -> dict[str, Any]:
        return {"id": "flow-1", "name": name}

    def prefect_provider_connection_id(self) -> str:
        return "prefect-1"

    def publish_version(self, flow_id: str, plan: dict[str, Any]) -> dict[str, Any]:
        self.published.append(plan)
        return {"id": "version-1"}

    def start_run(self, flow_id: str) -> dict[str, Any]:
        return {"id": "run-1"}

    def wait_for_run_end(
        self, flow_run_id: str, timeout_seconds: float
    ) -> dict[str, Any]:
        failed = self.node_status == "failed"
        node = {
            "node": scenario.NODE_KEY,
            "status": self.node_status,
            "task_run_id": "task-run-1",
            "failure": FAILURE if failed else None,
        }
        return {"status": "failed" if failed else "completed", "nodes": [node]}


def test_setup_returns_a_value_for_every_placeholder_in_the_prompt():
    sandbox = FailingRunSandbox()

    values = scenario.setup(sandbox, PREFIX)
    prompt = scenario.SCENARIO.inputs.prompt_for(PREFIX, values)

    assert re.findall(r"\{\w+\}", prompt) == []
    assert f'Flow name: "{PREFIX}docs-lookup"' in prompt
    assert "Flow run ID: run-1" in prompt
    assert "Failure code: internal_error" in prompt
    assert (
        "Run page: https://app.prefect.cloud/account/account-1/workspace/workspace-1"
        "/runs/flow-run/run-1?execution-plan-node=look_up"
    ) in prompt
    [plan] = sandbox.published
    node = plan["nodes"][scenario.NODE_KEY]
    assert node["model"] == {"connection_id": "prefect-1", "id": scenario.NODE_MODEL}
    assert node["orchestration"]["on_failure"] == {"retry": {"max_attempts": 1}}


def test_setup_raises_when_the_node_did_not_fail():
    with pytest.raises(RuntimeError, match="without the expected failure"):
        scenario.setup(FailingRunSandbox(node_status="completed"), PREFIX)


def diagnosed(broken: bool):
    calls = [
        ToolCall("get_run", {"flow_run_id": "run-1"}, prefect_tool=True),
        ToolCall("get_run_activity", {"flow_run_id": "run-1"}, prefect_tool=True),
    ]
    report = (
        "The `search_prefect` call failed with 'No results found' because the "
        "objective asks for an empty query. The plan caused it."
    )
    if broken:
        calls = [
            *calls[:1],
            ToolCall("publish_plan", {"plan": {}}, prefect_tool=True),
        ]
        report = "Prefect had an internal error."
    return outcome(Transcript(tool_calls=calls, turn_results=[report]))


BROKEN_CHECKS = [
    "get_run_activity called",
    "the report names the failed tool",
    "publish_plan never called",
]


@pytest.mark.parametrize("broken", [False, True], ids=["expected", "broken"])
def test_scenario_checks(broken: bool):
    failed = [
        check.name for check in scenario.checks(diagnosed(broken)) if not check.passed
    ]

    assert failed == (BROKEN_CHECKS if broken else [])


def test_the_judge_reads_only_the_agent_report():
    [judgement] = scenario.SCENARIO.judgements

    evidence = judgement.evidence(diagnosed(broken=False))

    assert evidence.startswith("<Report>\nThe `search_prefect` call failed")
    assert "get_run_activity" not in evidence
    assert judgement.evidence(outcome(Transcript())) == ""
