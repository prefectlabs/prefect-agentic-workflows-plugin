"""Set up a model provider in a workspace that has none, and use it in the plan.

The sandbox has no provider connections. The user asks for a one-agent
workflow without naming a model. The agent asks which model to use with the
design summary, not with its first question. The user picks Haiku 5.5 on
Prefect's models, and the agent adds a Prefect connection and runs the agent
node on it. Provider connections belong to the whole workspace, so the case
runs alone, and it deletes every connection before and after it runs. The
user turns down the test run.

Expected results:

- the agent reads the workspace's provider connections
- its first message asks nothing about the model
- it adds a Prefect connection once, only after the user chooses Prefect's
  models
- it never asks the user for a connection ID or an API key
- every agent node of the published plan runs Haiku 5.5 on the new connection
- `publish_plan` is called only after `validate_plan` passes, and a version
  of the flow is saved
"""

import re
from typing import Any

from evals import assertions, graph
from evals.assertions import Check
from evals.record import ToolCall
from evals.sandbox import SandboxApi
from evals.scenario import (
    DECLINE_TEST_RUN,
    FLOW_PREFIX,
    Outcome,
    Rule,
    Scenario,
    ScenarioInputs,
)

FLOW_NAME = "feedback-summary"
MODEL_ID = "claude-haiku-5-5"

PROMPT = f"""\
Use the agentic-workflows skill to build a Prefect workflow named
{FLOW_PREFIX}{FLOW_NAME}. Each run takes one piece of customer feedback as
text, and an AI agent summarizes it in three bullet points. It doesn't
connect to any other tools.
"""

CHOOSE_PREFECT = """\
Use Prefect's models, with Haiku 5.5. The rest of the design is right, so go
ahead and build it.
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
        label="model-choice",
        before_tool="create_prefect_connection",
        pattern=r"\bmodel",
        reply=CHOOSE_PREFECT,
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

# A question, ending in "?", that mentions the model.
MODEL_QUESTION = r"[^.?!\n]*\bmodel\b[^.?!\n]*\?"


def clear_provider_connections(sandbox: SandboxApi, flow_prefix: str) -> None:
    sandbox.delete_provider_connections()


def created_connection_id(calls: list[ToolCall]) -> str | None:
    """Return the ID of the first connection `create_prefect_connection` added."""
    for call in assertions.calls_to(calls, "create_prefect_connection"):
        if not call.is_error and isinstance(call.result, dict):
            return str(call.result.get("id"))
    return None


def check_agent_nodes_use_connection(
    plan: graph.Plan, connection_id: str | None
) -> Check:
    expected = {"connection_id": connection_id, "id": MODEL_ID}
    models: dict[str, Any] = {
        node_id: graph.nodes(plan)[node_id].get("model")
        for node_id in assertions.nodes_of_kind(plan, "AgentNode")
    }
    passed = (
        connection_id is not None
        and bool(models)
        and all(
            isinstance(model, dict)
            and {key: model.get(key) for key in expected} == expected
            for model in models.values()
        )
    )
    return Check(
        f"every agent node runs {MODEL_ID} on the new connection",
        passed,
        "" if passed else f"found {models}; the new connection is {connection_id}",
    )


def checks(outcome: Outcome) -> list[Check]:
    transcript = outcome.transcript
    calls = transcript.tool_calls
    return [
        assertions.check_called(
            calls, "list_provider_connections", where=lambda call: not call.is_error
        ),
        Check(
            "the first message asks nothing about the model",
            not re.search(MODEL_QUESTION, transcript.final_message(1), re.IGNORECASE),
            "the first message asks about the model",
        ),
        assertions.check_called(calls, "create_prefect_connection", times=1),
        assertions.check_only_after_reply(
            "the Prefect connection is added only after the user chose it",
            assertions.calls_to(calls, "create_prefect_connection"),
            transcript.first_reply("model-choice"),
        ),
        assertions.check_never_asks_for_id_or_key(transcript.turn_results),
        check_agent_nodes_use_connection(
            outcome.published_plan(FLOW_NAME), created_connection_id(calls)
        ),
        assertions.check_publish_succeeded(calls),
        assertions.check_published_only_after_valid(calls),
        assertions.check_flow_saved(outcome.flows, FLOW_NAME),
    ]


SCENARIO = Scenario(
    "provider_onboarding",
    ScenarioInputs(
        prompt=PROMPT,
        user=USER,
        setup=clear_provider_connections,
        teardown=clear_provider_connections,
        runs_alone=True,
    ),
    checks,
)
