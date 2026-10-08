"""Build the quickstart feedback-reply workflow, with a revision after a rejection.

The user asks for the workflow with the request from the README quickstart:
a manager approves the draft or rejects it with notes, and an agent revises a
rejected draft once. The user turns down the test run.

Expected results:

- the plan has a human-input node whose rejected output leads to the agent
  node that revises the draft
- the plan has the `reply` and `category` outputs
- `publish_plan` is called only after `validate_plan` passes on the same plan
- a version of the flow is saved, and no run is started
"""

from evals import assertions, graph
from evals.assertions import Check
from evals.scenario import (
    DECLINE_TEST_RUN,
    Outcome,
    Rule,
    RunScenario,
    assert_passed,
)

FLOW_NAME = "customer-feedback-reply"


def prompt(flow_prefix: str) -> str:
    return f"""\
Use the agentic-workflows skill to build a Prefect workflow named
{flow_prefix}{FLOW_NAME}. Start from the customer feedback reply example.
Each run takes one piece of customer feedback as text. An AI agent sorts it
into bug, feature request, praise, or complaint, and drafts a reply. A
manager approves the draft, or rejects it with notes. If the manager rejects
it, an agent revises the draft once, using the notes. The workflow returns
the final reply and the category. It doesn't connect to any other tools.
"""


USER = [
    DECLINE_TEST_RUN,
    Rule(label="end", after_tool="publish_plan", reply=None),
    Rule(
        label="reachable-systems",
        before_tool="validate_plan",
        pattern=r"reach|business tool|web address|remote MCP server",
        reply="None. The workflow only uses the feedback I paste in.",
        max_uses=1,
    ),
    Rule(
        label="design-approval",
        before_tool="validate_plan",
        pattern=r"confirm|approv|right|build|go ahead|proceed",
        reply="Yes, build it.",
    ),
    Rule(label="go-ahead", pattern=r"\?", reply="The defaults are fine.", max_uses=3),
]


def rejected_outputs(plan: graph.Plan) -> dict[str, set[str]]:
    """Return the nodes each human-input node's rejected output leads to.

    The keys are `<node>.<output>` for each output whose name starts with
    `reject`.
    """
    found = {}
    for node_id in assertions.nodes_of_kind(plan, "HumanInputNode"):
        for output, targets in assertions.branches(plan, node_id).items():
            if output.lower().startswith("reject"):
                found[f"{node_id}.{output}"] = targets
    return found


def check_rejection_leads_to_revision(plan: graph.Plan) -> Check:
    found = rejected_outputs(plan)
    kinds = assertions.node_kinds(plan)
    leads_to_agent = any(
        kinds.get(target) == "AgentNode"
        for targets in found.values()
        for target in targets
    )
    return Check(
        "the rejected output leads to an agent node that revises the draft",
        leads_to_agent,
        "" if leads_to_agent else f"rejected outputs lead to {found or 'nothing'}",
    )


def checks(outcome: Outcome) -> list[Check]:
    calls = outcome.transcript.tool_calls
    return [
        assertions.check_has_node_kind(outcome.plan, "HumanInputNode"),
        check_rejection_leads_to_revision(outcome.plan),
        assertions.check_plan_outputs(outcome.plan, {"reply", "category"}),
        assertions.check_publish_succeeded(calls),
        assertions.check_published_only_after_valid(calls),
        assertions.check_flow_saved(outcome.flows, FLOW_NAME),
        assertions.check_never_called(calls, "start_run"),
    ]


async def test_rejected_approval(run_scenario: RunScenario, flow_prefix: str) -> None:
    outcome = await run_scenario(prompt(flow_prefix), USER)
    assert_passed(outcome, checks(outcome))
