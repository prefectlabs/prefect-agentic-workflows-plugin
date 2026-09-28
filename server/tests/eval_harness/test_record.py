"""Tests for reading an agent's stream-json events into a transcript."""

import json
from pathlib import Path
from typing import Any

from evals.record import Reply, ToolCall, Transcript, read_plan_files


def event(**body: Any) -> str:
    return json.dumps(body)


def assistant(*blocks: dict[str, Any]) -> str:
    return event(
        type="assistant", message={"role": "assistant", "content": list(blocks)}
    )


def tool_result(tool_use_id: str, text: str, *, is_error: bool = False) -> str:
    return event(
        type="user",
        message={
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": [{"type": "text", "text": text}],
                    "is_error": is_error,
                }
            ],
        },
    )


def test_read_events_pairs_tool_calls_with_results_and_keeps_the_text():
    transcript = Transcript()
    lines = [
        event(type="system", subtype="init", session_id="session-1"),
        assistant({"type": "text", "text": "I'll validate the plan."}),
        assistant(
            {
                "type": "tool_use",
                "id": "call-1",
                "name": "mcp__prefect-agentic-workflows__validate_plan",
                "input": {"plan": {"nodes": {}}},
            },
            {
                "type": "tool_use",
                "id": "call-2",
                "name": "Read",
                "input": {"file_path": "SKILL.md"},
            },
        ),
        tool_result("call-1", '{"valid": true, "errors": []}'),
        tool_result("call-2", "No such file", is_error=True),
        "not json",
        "",
        event(
            type="result",
            subtype="success",
            result="The plan is valid.",
            session_id="session-1",
            total_cost_usd=0.25,
        ),
    ]

    final = transcript.read_events(lines, turn=1)

    assert final == "The plan is valid."
    assert transcript.session_id == "session-1"
    assert transcript.cost_usd == 0.25
    assert transcript.agent_text == ["I'll validate the plan."]
    assert transcript.tool_calls == [
        ToolCall(
            "validate_plan",
            {"plan": {"nodes": {}}},
            {"valid": True, "errors": []},
            False,
            1,
        ),
        ToolCall("Read", {"file_path": "SKILL.md"}, "No such file", True, 1),
    ]


def test_read_events_records_an_error_result():
    transcript = Transcript()

    transcript.read_events(
        [event(type="result", subtype="error_max_turns", is_error=True)], turn=1
    )

    assert transcript.errors == ["error_max_turns"]


def test_read_plan_files_reads_every_plan_in_the_workflows_directory(tmp_path: Path):
    workflows = tmp_path / "workflows"
    workflows.mkdir()
    (workflows / "release-notes.plan.json").write_text('{"kind": "ExecutionPlan"}')
    (workflows / "broken.plan.json").write_text("{")
    (workflows / "notes.md").write_text("ignored")

    assert read_plan_files(tmp_path) == {
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
