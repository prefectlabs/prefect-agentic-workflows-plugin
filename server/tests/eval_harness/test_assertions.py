"""Tests for the plan checks that scenarios build on."""

from harness_plans import approval_plan, cyclic_plan

from evals import assertions


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
