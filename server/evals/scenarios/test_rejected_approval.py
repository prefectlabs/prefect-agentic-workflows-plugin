"""Build the quickstart feedback-reply workflow, run it, and reject the draft.

The user asks for the workflow with the request from the README quickstart.
In the test run, the simulated manager rejects the draft and gives notes for
the revision.

Expected results:

- the approval node's rejected output leads to the node that revises the draft
- `submit_human_input` gets the user's decision and notes as they wrote them
- the run finishes with its `reply` output
"""

import re
from typing import Any

from evals import assertions, graph
from evals.assertions import Check
from evals.scenario import Outcome, Rule, RunScenario, assert_passed

PROMPT = """\
Use the agentic-workflows skill to build a Prefect workflow named
customer-feedback-reply. Start from the customer feedback reply example.
Each run takes one piece of customer feedback as text. An AI agent sorts it
into bug, feature request, praise, or complaint, and drafts a reply. A
manager approves the draft, or rejects it with notes. If the manager rejects
it, an agent revises the draft once, using the notes. The workflow returns
the final reply and the category. It doesn't connect to any other tools.
"""

FEEDBACK = (
    "The app crashed twice this week while I was saving an invoice, and I lost "
    "my changes both times."
)
NOTES = "Apologize for the lost work and say the team is looking into the crash."

USER = [
    Rule(
        label="test-run-approval",
        after_tool="publish_plan",
        before_tool="start_run",
        pattern=r"test run|start (a|the) run|run it",
        reply=f'Yes, start a test run with this feedback: "{FEEDBACK}"',
    ),
    Rule(
        label="form-answer",
        after_tool="start_run",
        before_tool="submit_human_input",
        pattern=r"approv|reject|decision|form|review",
        reply=f"Reject the draft. Use these notes, word for word: {NOTES}",
        max_uses=1,
    ),
    Rule(
        label="keep-watching",
        after_tool="submit_human_input",
        pattern=r"watch|keep|continue|check",
        reply="Yes, keep watching until the run finishes.",
        max_uses=2,
    ),
    Rule(label="end", after_tool="start_run", reply=None),
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


def normalized(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text)).strip()


def check_answer_passed_on(outcome: Outcome) -> Check:
    calls = assertions.calls_to(outcome.transcript.tool_calls, "submit_human_input")
    name = "submit_human_input got the user's decision and notes unchanged"
    if len(calls) != 1:
        return Check(name, False, f"submit_human_input called {len(calls)} times")
    response = calls[0].arguments.get("response")
    if not isinstance(response, dict):
        return Check(name, False, f"response was {response!r}")
    has_notes = any(normalized(value) == NOTES for value in response.values())
    passed = response.get("decision") == "rejected" and has_notes
    return Check(name, passed, "" if passed else f"response was {response!r}")


def check_run_finished_with_reply(outcome: Outcome) -> Check:
    finished = [
        run
        for run in outcome.fake.runs.values()
        if run.status() == "completed" and run.plan_output("reply")[0] == "available"
    ]
    statuses = [run.status() for run in outcome.fake.runs.values()]
    return Check(
        "a run finished with its reply output",
        bool(finished),
        "" if finished else f"run statuses: {statuses or 'no runs'}",
    )


def checks(outcome: Outcome) -> list[Check]:
    return [
        assertions.check_has_node_kind(outcome.plan, "HumanInputNode"),
        check_rejection_leads_to_revision(outcome.plan),
        assertions.check_plan_outputs(outcome.plan, {"reply", "category"}),
        assertions.check_published_only_after_valid(outcome.transcript.tool_calls),
        check_answer_passed_on(outcome),
        check_run_finished_with_reply(outcome),
    ]


async def test_rejected_approval(run_scenario: RunScenario) -> None:
    outcome = await run_scenario(PROMPT, USER)
    assert_passed(outcome, checks(outcome))
