"""Ask for a workflow that needs remote MCP servers the user doesn't have.

The user wants a daily summary of new Zendesk tickets posted to Slack, and has
no remote MCP server for either one. The workspace has no Secret blocks and no
deployments. The conversation ends after the agent answers the user's reply
to the infrastructure check.

Expected results:

- the agent reads the workspace and asks which tools it can reach before it
  asks any design question
- after the user says they have no remote MCP servers, the agent says which
  steps that affects and offers a version without those tools
- nothing is validated or published
"""

import re

from evals import assertions
from evals.assertions import Check
from evals.scenario import Outcome, Rule, RunScenario, assert_passed

PROMPT = """\
Use the agentic-workflows skill to build a Prefect workflow that reads our
new Zendesk tickets every morning, summarizes them, and posts the summary to
our #support Slack channel.
"""

NO_SERVERS = """\
We don't have any of those web addresses. We use Zendesk and Slack in the
browser, and nobody here hosts anything.
"""

USER = [
    Rule(
        label="no-remote-servers",
        pattern=r"reach|business tool|web address|remote MCP server|MCP",
        reply=NO_SERVERS,
        max_uses=1,
    ),
    Rule(label="end", reply=None),
]

# A question, ending in "?", about the schedule or the approvals. The agent
# may mention these as defaults it will propose later, which is fine.
DESIGN_QUESTIONS = r"[^.?!\n]*(what time|time zone|how often|approv)[^.?!\n]*\?"
LIMITS = {
    "Zendesk": r"zendesk",
    "Slack": r"slack",
    "a version without those tools": (
        r"without|by hand|manual|paste|copy|human (approval|review|step)"
        r"|leave [^.\n]* out"
    ),
}


def checks(outcome: Outcome) -> list[Check]:
    transcript = outcome.transcript
    calls = transcript.tool_calls
    answer = transcript.first_reply("no-remote-servers")
    first_turn_calls = [call for call in calls if call.turn == 1]
    first_question = transcript.final_message(1)
    follow_up = transcript.final_message(answer.turn + 1) if answer else ""
    return [
        Check(
            "the agent's first question is the infrastructure check",
            answer is not None and answer.turn == 1,
            "the first message didn't ask which tools the agent can reach",
        ),
        Check(
            "the first question asks no design question",
            not re.search(DESIGN_QUESTIONS, first_question, re.IGNORECASE),
            "the first message asks about the design",
        ),
        assertions.check_called(first_turn_calls, "list_secret_blocks"),
        assertions.check_called(first_turn_calls, "list_deployments"),
        assertions.check_text_mentions(
            "the agent explains the limit and offers a version without the tools",
            follow_up,
            LIMITS,
        ),
        assertions.check_never_called(calls, "validate_plan"),
        assertions.check_never_called(calls, "publish_plan"),
    ]


async def test_no_infrastructure(run_scenario: RunScenario) -> None:
    outcome = await run_scenario(PROMPT, USER)
    assert_passed(outcome, checks(outcome))
