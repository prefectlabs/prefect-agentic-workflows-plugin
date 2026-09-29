"""Build a workflow whose approval has a deadline, and let the deadline pass.

The user describes a weekly update that a manager has one day to approve. When
nobody answers, the workflow should finish and return the draft marked as not
reviewed. In the test run the user doesn't answer the form, and the fake
reports that the deadline passed.

Expected results:

- the approval node sets `deadline.on_expiry` to an output that leads to a
  node or a plan output
- the agent never answers the form
- the run finishes on the expiry path, and the agent tells the user so without
  polling on and on
"""

from evals import assertions, graph
from evals.assertions import Check
from evals.fake_cloud import FakeCloud, NodeScript
from evals.scenario import Outcome, Rule, RunScenario, assert_passed

PROMPT = """\
Use the agentic-workflows skill to build a Prefect workflow named
weekly-update-review. Each run takes my team's notes for the week as text. An
AI agent turns them into a short weekly update. My manager approves the
update or rejects it. The manager has one day to answer. If nobody answers by
then, the workflow shouldn't wait: it should finish and return the draft
marked as not reviewed. It doesn't connect to any other tools.
"""

NOTES = "Shipped the billing export. Fixed two login bugs. Hiring is on hold."
MOST_RUN_READS = 12

USER = [
    Rule(
        label="test-run-approval",
        after_tool="publish_plan",
        before_tool="start_run",
        pattern=r"test run|start (a|the) run|run it",
        reply=f'Yes, start a test run with these notes: "{NOTES}"',
    ),
    Rule(
        label="leave-form",
        after_tool="start_run",
        pattern=r"approv|reject|decision|form|answer",
        reply=(
            "Don't answer the form. I want to see what happens when nobody "
            "answers before the deadline. Keep watching the run."
        ),
        max_uses=1,
    ),
    Rule(label="end", after_tool="start_run", reply=None),
    Rule(
        label="reachable-systems",
        before_tool="validate_plan",
        pattern=r"reach|business tool|web address|remote MCP server",
        reply="None. The workflow only uses the notes I paste in.",
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


def setup(fake: FakeCloud) -> None:
    fake.scripts["HumanInputNode"] = NodeScript(expire=True)


def expiry_destinations(plan: graph.Plan) -> dict[str, list[str]]:
    """Return where each human-input node's expiry output leads, by node ID.

    A destination is a node ID, or `output:<name>` for a plan output that
    reads the expiry output. A node with no expiry output is left out.
    """
    found: dict[str, list[str]] = {}
    for node_id, node in graph.nodes(plan).items():
        expiry = graph.human_input_expiry_output(node)
        if node.get("kind") != "HumanInputNode" or expiry is None:
            continue
        targets = sorted(assertions.branches(plan, node_id).get(expiry, set()))
        for name, output in (plan.get("outputs") or {}).items():
            fields = (output or {}).get("fields") or {}
            for field_spec in fields.values():
                source = (field_spec or {}).get("source") or {}
                options = source.get("one_of") or [source]
                if any(
                    ref.get("node") == node_id and ref.get("output") == expiry
                    for ref in options
                    if isinstance(ref, dict)
                ):
                    targets.append(f"output:{name}")
        found[node_id] = targets
    return found


def check_expiry_leads_somewhere(plan: graph.Plan) -> Check:
    found = expiry_destinations(plan)
    passed = any(found.values())
    return Check(
        "the approval's on_expiry output leads to a node or a plan output",
        passed,
        "" if passed else f"expiry outputs lead to {found or 'no expiry output'}",
    )


def check_run_took_expiry_path(outcome: Outcome) -> Check:
    for run in outcome.fake.runs.values():
        for node_id, state in run.nodes.items():
            node = graph.nodes(run.plan)[node_id]
            expiry = graph.human_input_expiry_output(node)
            if expiry and state.output == expiry and run.status() == "completed":
                return Check("a run finished on the expiry path", True)
    statuses = [run.status() for run in outcome.fake.runs.values()]
    return Check(
        "a run finished on the expiry path",
        False,
        f"run statuses: {statuses or 'no runs'}",
    )


def checks(outcome: Outcome) -> list[Check]:
    calls = outcome.transcript.tool_calls
    reads = len(assertions.calls_to(calls, "get_run"))
    final = (
        outcome.transcript.turn_results[-1] if outcome.transcript.turn_results else ""
    )
    return [
        check_expiry_leads_somewhere(outcome.plan),
        assertions.check_published_only_after_valid(calls),
        assertions.check_never_called(calls, "submit_human_input"),
        check_run_took_expiry_path(outcome),
        Check(
            f"get_run called at most {MOST_RUN_READS} times",
            reads <= MOST_RUN_READS,
            f"called {reads} times",
        ),
        assertions.check_text_mentions(
            "the final message reports the expiry",
            final,
            {"the deadline passed": r"expir|deadline|nobody answered|no one answered"},
        ),
    ]


async def test_expired_approval(run_scenario: RunScenario) -> None:
    outcome = await run_scenario(PROMPT, USER, setup=setup)
    assert_passed(outcome, checks(outcome))
