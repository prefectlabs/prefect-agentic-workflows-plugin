"""Convert the `release-notes` fixture skill into a workflow, publish it, and test it.

The fixture skill has one part for each case of the conversion report: a local
script whose output later steps read, a stdio MCP server, a repeat-until-done
loop, and a human approval. See `tests/fixtures/skills/README.md`.

When the infrastructure check asks which business tools an agent can reach,
the simulated user names the GitHub server and a tools server that needs no
credentials, and says the Docker-based server runs only on their machine.

Expected results:

- the conversion report lists every one of those parts
- the user approves the design once
- the plan has no cycle
- the approval step is a human-input node
- `publish_plan` is called only after `validate_plan` passes on the same plan
"""

from evals import assertions
from evals.assertions import Check
from evals.fake_cloud import FakeCloud
from evals.runner import REPO_ROOT
from evals.scenario import Outcome, Rule, RunScenario, assert_passed

FIXTURE = REPO_ROOT / "tests" / "fixtures" / "skills" / "release-notes"

PROMPT = """\
Convert the skill in ./release-notes into a Prefect Cloud workflow. Name the
flow `release-notes`. Our GitHub MCP server is hosted at
https://api.githubcopilot.com/mcp/, and its token is in the Secret block
`github-token`.
"""

DESIGN_APPROVAL = """\
I accept every proposed substitute in the report, and the summary is right.
For the check loop, use two check-and-fix passes. The changes script is
available as the `collect_changes` tool on our remote MCP server at
https://tools.example.com/mcp, which needs no credentials. Go ahead.
"""

REACHABLE_SYSTEMS = """\
GitHub is reachable through https://api.githubcopilot.com/mcp/ with the token
in the Secret block `github-token`. We also host a remote MCP server at
https://tools.example.com/mcp that needs no credentials. The Docker-based
server in the skill runs only on my machine. Nothing else is reachable.
"""

REPORT_PARTS = {
    "local script": r"collect_changes",
    # When the tools check already settled the remote replacement, the report
    # can name the replacement instead of the stdio server.
    "stdio MCP server": r"stdio|docker run|GitHub MCP server|githubcopilot",
    "repeat-until-done loop": r"loop|repeat|cycle",
    "human approval": r"approv",
}

USER = [
    Rule(
        label="test-run-approval",
        after_tool="publish_plan",
        before_tool="start_run",
        pattern=r"test run|start (a|the) run|run it",
        reply=(
            "Yes, start the test run. Use v1.4.0 for the tag, and acme/widgets "
            "for the repository if the plan asks for one."
        ),
    ),
    Rule(
        label="form-answer",
        after_tool="start_run",
        pattern=r"approv|decision|form",
        reply="I approve the draft.",
        max_uses=1,
    ),
    Rule(label="end", after_tool="start_run", reply=None),
    Rule(
        label="reachable-systems",
        before_tool="validate_plan",
        pattern=r"reach|business tool|web address|remote MCP server",
        reply=REACHABLE_SYSTEMS,
        max_uses=1,
    ),
    Rule(
        label="design-approval",
        before_tool="validate_plan",
        pattern=r"confirm|approv|decision|decide|go ahead|look right|proceed",
        reply=DESIGN_APPROVAL,
    ),
    Rule(label="go-ahead", pattern=r"\?", reply="Yes, go ahead.", max_uses=3),
]


def setup(fake: FakeCloud) -> None:
    fake.add_secret_block("github-token")


def conversion_report(outcome: Outcome) -> str:
    return "\n\n".join(
        text
        for text in outcome.transcript.agent_text
        if "conversion report" in text.lower()
    )


def checks(outcome: Outcome) -> list[Check]:
    calls = outcome.transcript.tool_calls
    report = conversion_report(outcome)
    design_approvals = outcome.transcript.replies_labeled("design-approval")
    report_check = assertions.check_text_mentions(
        "conversion report lists every unsupported part", report, REPORT_PARTS
    )
    if not report:
        report_check = Check(report_check.name, False, "no conversion report found")
    return [
        report_check,
        Check(
            "design approved once",
            len(design_approvals) == 1,
            f"the simulated user approved the design {len(design_approvals)} times",
        ),
        Check(
            "one plan file written",
            len(outcome.plans) == 1,
            f"found {sorted(outcome.plans) or 'none'}",
        ),
        assertions.check_no_cycle(outcome.plan),
        assertions.check_has_node_kind(outcome.plan, "HumanInputNode"),
        assertions.check_publish_succeeded(calls),
        assertions.check_published_only_after_valid(calls),
    ]


async def test_release_notes_conversion(run_scenario: RunScenario) -> None:
    outcome = await run_scenario(
        PROMPT, USER, files={"release-notes": FIXTURE}, setup=setup
    )
    assert_passed(outcome, checks(outcome))
