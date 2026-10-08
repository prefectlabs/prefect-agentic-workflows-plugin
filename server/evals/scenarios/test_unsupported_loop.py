"""Convert a skill with a repeat-until-happy loop into a workflow.

The `post-review` fixture skill drafts a blog post, then reviews and revises it
until the reviewer is happy. A plan can't have a cycle, so the conversion
report has to flag the loop and propose a substitute. The simulated user picks
two fixed review passes, and turns down the test run.

Expected results:

- the conversion report flags the loop and proposes a fixed number of passes
  or a human checkpoint
- the plan has no cycle
- nothing is published before the user decides
- a version of the flow is saved, and no run is started
"""

from evals import assertions, graph
from evals.assertions import Check
from evals.runner import REPO_ROOT
from evals.scenario import (
    DECLINE_TEST_RUN,
    Outcome,
    Rule,
    RunScenario,
    assert_passed,
)

FIXTURE = REPO_ROOT / "tests" / "fixtures" / "skills" / "post-review"
FLOW_NAME = "post-review"


def prompt(flow_prefix: str) -> str:
    return f"""\
Convert the skill in ./post-review into a Prefect Cloud workflow named
{flow_prefix}{FLOW_NAME}. Each run should take the topic of the post as its
input.
"""


PUBLISHING_TOOLS = {"get_or_create_flow", "publish_plan", "activate_plan_version"}
REPORT_PARTS = {
    "the loop": r"loop|repeat|until the reviewer",
    "a fixed number of passes or a human checkpoint": (
        r"fixed number|\b(two|three|2|3)\b[^.\n]*(pass|round|cycle)"
        r"|human (checkpoint|review|approval)|checkpoint"
    ),
}

USER = [
    DECLINE_TEST_RUN,
    Rule(label="end", after_tool="publish_plan", reply=None),
    Rule(
        label="reachable-systems",
        before_tool="validate_plan",
        pattern=r"reach|business tool|web address|remote MCP server",
        reply="None. The workflow only uses the topic I give it.",
        max_uses=1,
    ),
    Rule(
        label="loop-decision",
        before_tool="validate_plan",
        pattern=r"loop|repeat|pass|checkpoint|decid|decision|confirm|approv",
        reply=(
            "For the review loop, use two fixed review-and-revise passes. "
            "Everything else in the report and the summary is right. Go ahead."
        ),
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


def conversion_report(outcome: Outcome) -> str:
    return "\n\n".join(
        text
        for text in outcome.transcript.agent_text
        if "conversion report" in text.lower()
    )


def check_two_review_passes(outcome: Outcome) -> Check:
    """Check that the plan has the two review passes the user chose."""
    reviews = [
        node_id
        for node_id, node in graph.nodes(outcome.plan).items()
        if node.get("kind") == "AgentNode"
        and "review" in f"{node_id} {node.get('objective', '')}".lower()
    ]
    return Check(
        "the plan has two review passes",
        len(reviews) >= 2,
        "" if len(reviews) >= 2 else f"found review nodes {reviews}",
    )


def checks(outcome: Outcome) -> list[Check]:
    calls = outcome.transcript.tool_calls
    report = conversion_report(outcome)
    report_check = assertions.check_text_mentions(
        "the conversion report flags the loop and proposes a substitute",
        report,
        REPORT_PARTS,
    )
    if not report:
        report_check = Check(report_check.name, False, "no conversion report found")
    decision = outcome.transcript.first_reply("loop-decision")
    return [
        report_check,
        Check(
            "the user decided on the loop",
            decision is not None,
            "the agent never asked for a decision on the loop",
        ),
        assertions.check_only_after_reply(
            "nothing published before the user decided",
            [call for call in calls if call.name in PUBLISHING_TOOLS],
            decision,
        ),
        assertions.check_no_cycle(outcome.plan),
        check_two_review_passes(outcome),
        assertions.check_publish_succeeded(calls),
        assertions.check_published_only_after_valid(calls),
        assertions.check_flow_saved(outcome.flows, FLOW_NAME),
        assertions.check_never_called(calls, "start_run"),
    ]


async def test_unsupported_loop(run_scenario: RunScenario, flow_prefix: str) -> None:
    outcome = await run_scenario(
        prompt(flow_prefix), USER, files={"post-review": FIXTURE}
    )
    assert_passed(outcome, checks(outcome))
