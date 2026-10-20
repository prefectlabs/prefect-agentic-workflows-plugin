"""Ask for a model that only the user's own key can run.

The user asks for a one-agent workflow on Claude Fable 5.1. Prefect's models
don't include Fable, so a Prefect connection can't run it. The agent should
send the user the link that adds their own Anthropic or Vertex AI key, and
never ask for the key. The conversation ends when the agent sends the link.

Expected results:

- the agent reads the providers before it answers
- it sends the link that adds an Anthropic or Vertex AI connection
- it never adds a Prefect connection, and never asks the user for a
  connection ID or an API key
"""

from evals import assertions
from evals.assertions import Check
from evals.scenario import (
    FLOW_PREFIX,
    Outcome,
    Rule,
    Scenario,
    ScenarioInputs,
)

FLOW_NAME = "feedback-summary"

PROMPT = f"""\
Use the agentic-workflows skill to build a Prefect workflow named
{FLOW_PREFIX}{FLOW_NAME}. Each run takes one piece of customer feedback as
text, and an AI agent running Claude Fable 5.1 summarizes it in three bullet
points. It doesn't connect to any other tools.
"""

ADD_LINK = r"agentic-model-providers\?add=(anthropic|vertex)"

USER = [
    Rule(label="end", pattern=ADD_LINK, reply=None),
    Rule(
        label="reachable-systems",
        pattern=r"reach|business tool|web address|remote MCP server",
        reply="None. The workflow only uses the feedback I paste in.",
        max_uses=1,
    ),
    Rule(label="go-ahead", pattern=r"\?", reply="The defaults are fine.", max_uses=3),
]


def checks(outcome: Outcome) -> list[Check]:
    transcript = outcome.transcript
    calls = transcript.tool_calls
    messages = transcript.agent_text + transcript.turn_results
    return [
        assertions.check_called(calls, "list_providers"),
        assertions.check_text_mentions(
            "the agent sends the link to add an Anthropic or Vertex AI key",
            "\n".join(messages),
            {"the add link": ADD_LINK},
        ),
        assertions.check_never_called(calls, "create_prefect_connection"),
        assertions.check_never_asks_for_id_or_key(messages),
    ]


SCENARIO = Scenario(
    "own_key_model",
    ScenarioInputs(prompt=PROMPT, user=USER, max_turns=6),
    checks,
)
