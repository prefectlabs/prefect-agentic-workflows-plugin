"""Tests for the checks scenarios run on a plan and on the agent's tool calls."""

from typing import Any

from harness_plans import approval_plan, edge, from_node, port

from evals import assertions
from evals.record import ToolCall


def cyclic_plan() -> dict[str, Any]:
    plan = approval_plan()
    plan["nodes"]["draft"]["inputs"]["feedback"] = port({})
    plan["edges"].append(
        edge("publish-to-draft", from_node("publish", "published"), "draft", "feedback")
    )
    return plan


def test_node_kinds_and_nodes_of_kind():
    plan = approval_plan()

    assert assertions.node_kinds(plan) == {
        "draft": "AgentNode",
        "approve": "HumanInputNode",
        "publish": "AgentNode",
    }
    assert assertions.nodes_of_kind(plan, "HumanInputNode") == ["approve"]


def test_branches_lists_the_nodes_each_output_leads_to():
    assert assertions.branches(approval_plan(), "approve") == {
        "approved": {"publish"},
        "rejected": set(),
    }


def test_plan_inputs_and_outputs():
    plan = approval_plan()

    assert assertions.plan_inputs(plan) == {"topic"}
    assert assertions.plan_outputs(plan) == {"result"}


def test_check_no_cycle():
    assert assertions.check_no_cycle(approval_plan()).passed is True
    failed = assertions.check_no_cycle(cyclic_plan())
    assert failed.passed is False
    assert failed.detail == "nodes in or after a cycle: approve, draft, publish"


def test_check_has_node_kind():
    assert assertions.check_has_node_kind(approval_plan(), "HumanInputNode").passed
    failed = assertions.check_has_node_kind(approval_plan(), "DeploymentNode")
    assert failed.passed is False
    assert failed.detail == "found 0: none"


def test_check_branches():
    plan = approval_plan()

    assert assertions.check_branches(
        plan, "approve", {"approved": {"publish"}, "rejected": set()}
    ).passed
    assert not assertions.check_branches(
        plan, "approve", {"rejected": {"draft"}}
    ).passed


def test_check_plan_inputs_and_outputs():
    plan = approval_plan()

    assert assertions.check_plan_inputs(plan, {"topic"}, exact=True).passed
    assert not assertions.check_plan_inputs(plan, {"tag"}).passed
    assert assertions.check_plan_outputs(plan, {"result"}).passed
    assert not assertions.check_plan_outputs(plan, set(), exact=True).passed


PLAN = approval_plan()
OTHER_PLAN = cyclic_plan()


def calls(*items: tuple[str, dict[str, Any], Any]) -> list[ToolCall]:
    return [ToolCall(name, arguments, result) for name, arguments, result in items]


VALID = {"valid": True, "errors": []}
INVALID = {"valid": False, "errors": [{"code": "cycle_detected"}]}


def test_check_called_and_never_called():
    sequence = calls(
        ("get_schema", {}, {}),
        ("validate_plan", {"plan": PLAN}, VALID),
        ("validate_plan", {"plan": PLAN}, VALID),
    )

    assert assertions.check_called(sequence, "validate_plan").passed
    assert assertions.check_called(sequence, "validate_plan", times=2).passed
    assert not assertions.check_called(sequence, "get_schema", times=2).passed
    assert assertions.check_called(
        sequence, "get_schema", where=lambda call: call.arguments == {}
    ).passed
    assert assertions.check_never_called(sequence, "start_run").passed
    assert not assertions.check_never_called(sequence, "get_schema").passed


def test_check_called_in_order_allows_other_calls_between():
    sequence = calls(
        ("get_schema", {}, {}),
        ("Read", {}, ""),
        ("validate_plan", {"plan": PLAN}, VALID),
        ("publish_plan", {"plan": PLAN}, {}),
    )

    assert assertions.check_called_in_order(
        sequence, ["get_schema", "validate_plan", "publish_plan"]
    ).passed
    failed = assertions.check_called_in_order(
        sequence, ["publish_plan", "validate_plan"]
    )
    assert failed.passed is False
    assert failed.detail == "never reached validate_plan"


def test_check_published_only_after_valid_passes_after_a_passing_validation():
    sequence = calls(
        ("validate_plan", {"plan": OTHER_PLAN}, INVALID),
        ("validate_plan", {"plan": PLAN}, VALID),
        ("publish_plan", {"plan": PLAN}, {}),
    )

    assert assertions.check_published_only_after_valid(sequence).passed


def test_check_published_only_after_valid_fails_without_a_passing_validation():
    sequence = calls(
        ("validate_plan", {"plan": PLAN}, INVALID),
        ("publish_plan", {"plan": PLAN}, {}),
    )

    assert not assertions.check_published_only_after_valid(sequence).passed


def test_check_published_only_after_valid_fails_when_the_plan_changed():
    sequence = calls(
        ("validate_plan", {"plan": PLAN}, VALID),
        ("publish_plan", {"plan": OTHER_PLAN}, {}),
    )

    failed = assertions.check_published_only_after_valid(sequence)

    assert failed.passed is False
    assert "tool call 2" in failed.detail


def test_check_text_mentions():
    text = "S1 runs scripts/collect_changes.py. S4 repeats until clean."

    assert assertions.check_text_mentions(
        "report", text, {"script": r"collect_changes", "loop": r"repeat|loop"}
    ).passed
    failed = assertions.check_text_mentions("report", text, {"stdio": r"stdio"})
    assert failed.passed is False
    assert failed.detail == "missing: stdio"
