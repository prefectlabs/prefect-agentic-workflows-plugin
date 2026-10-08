"""Convert the `release-notes` fixture skill into a workflow and publish it.

The fixture skill has one part for each case of the conversion report: a local
script whose output later steps read, a stdio MCP server, a repeat-until-done
loop, and a human approval. See `tests/fixtures/skills/README.md`.

When the infrastructure check asks which business tools an agent can reach,
the simulated user names the GitHub server and a tools server that needs no
credentials, and says the Docker-based server runs only on their machine.
The GitHub token is the sandbox's `eval-github-token` Secret block, which
holds a placeholder value. The user turns down the test run.

Expected results:

- the conversion report lists every one of those parts
- the user approves the design once
- the plan has no cycle
- the approval step is a human-input node
- `publish_plan` is called only after `validate_plan` passes on the same plan
- a version of the flow is saved, and no run is started
"""

from evals import assertions
from evals.assertions import Check
from evals.runner import REPO_ROOT
from evals.sandbox import SandboxApi
from evals.scenario import (
    DECLINE_TEST_RUN,
    Outcome,
    Rule,
    RunScenario,
    assert_passed,
)

FIXTURE = REPO_ROOT / "tests" / "fixtures" / "skills" / "release-notes"
FLOW_NAME = "release-notes"
SECRET_BLOCK = "eval-github-token"
# Cloud checks that every MCP server's hostname resolves when it saves a plan,
# so the tools server is on a hostname that resolves.
TOOLS_SERVER = "https://example.com/mcp"


def prompt(flow_prefix: str) -> str:
    return f"""\
Convert the skill in ./release-notes into a Prefect Cloud workflow. Name the
flow `{flow_prefix}{FLOW_NAME}`. Our GitHub MCP server is hosted at
https://api.githubcopilot.com/mcp/, and its token is in the Secret block
`{SECRET_BLOCK}`.
"""


DESIGN_APPROVAL = f"""\
I accept every proposed substitute in the report, and the summary is right.
For the check loop, use two check-and-fix passes. The changes script is
available as the `collect_changes` tool on our remote MCP server at
{TOOLS_SERVER}, which needs no credentials. Go ahead.
"""

REACHABLE_SYSTEMS = f"""\
GitHub is reachable through https://api.githubcopilot.com/mcp/ with the token
in the Secret block `{SECRET_BLOCK}`. We also host a remote MCP server at
{TOOLS_SERVER} that needs no credentials. The Docker-based server in the
skill runs only on my machine. Nothing else is reachable.
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
    DECLINE_TEST_RUN,
    Rule(label="end", after_tool="publish_plan", reply=None),
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


def setup(sandbox: SandboxApi, flow_prefix: str) -> None:
    sandbox.ensure_secret_block(SECRET_BLOCK)


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
        assertions.check_flow_saved(outcome.flows, FLOW_NAME),
        assertions.check_never_called(calls, "start_run"),
    ]


async def test_release_notes_conversion(
    run_scenario: RunScenario, flow_prefix: str
) -> None:
    outcome = await run_scenario(
        prompt(flow_prefix), USER, files={"release-notes": FIXTURE}, setup=setup
    )
    assert_passed(outcome, checks(outcome))
