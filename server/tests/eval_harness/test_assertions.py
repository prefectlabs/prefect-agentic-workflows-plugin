"""Tests for the plan checks that scenarios build on."""

from harness_plans import approval_plan, cyclic_plan

from evals import assertions
from evals.record import FlowState, ToolCall


def test_branches_lists_the_nodes_each_output_leads_to():
    assert assertions.branches(approval_plan(), "approve") == {
        "approved": {"publish"},
        "rejected": set(),
    }


def test_check_no_cycle():
    assert assertions.check_no_cycle(approval_plan()).passed is True
    failed = assertions.check_no_cycle(cyclic_plan())
    assert failed.passed is False
    assert failed.detail == "nodes in or after a cycle: approve, draft, publish"


def test_check_flows_named_with_prefix():
    named = [ToolCall("get_or_create_flow", {"name": "eval-abc123-1-digest"})]
    unnamed = [*named, ToolCall("get_or_create_flow", {"name": "digest"})]

    assert assertions.check_flows_named_with_prefix(named, "eval-abc123-1-").passed
    failed = assertions.check_flows_named_with_prefix(unnamed, "eval-abc123-1-")
    assert failed.passed is False
    assert failed.detail == "get_or_create_flow called with ['digest']"


def test_check_flow_saved():
    saved = FlowState("flow-1", "eval-abc123-1-digest", version_ids=["version-1"])
    empty = FlowState("flow-1", "eval-abc123-1-digest")

    assert assertions.check_flow_saved({"digest": saved}, "digest").passed
    assert not assertions.check_flow_saved({"digest": empty}, "digest").passed
    missing = assertions.check_flow_saved({}, "digest")
    assert missing.passed is False
    assert missing.detail == "no flow 'digest'; found none"
