"""Build a support-triage workflow from a plain description.

The user describes the support workflow from the sprint brief: an agent sorts
an issue into a question or a suspected bug, answers questions only when the
reply meets the user's requirements, and otherwise holds the reply. It
connects to no tools. The user turns down the test run.

Expected results:

- the plan has a `result` output with one optional `node_output` field for
  each output that ends a run
- every `one_of` field reads outputs of one node that selects at most one
  output
- when `get_schema` lists version 0.3, the plan is version 0.3 and an agent
  node output carries expectations; otherwise the plan has no expectations,
  and the agent tells the user that the workspace can't check them yet
- `publish_plan` is called only after `validate_plan` passes on the same plan
- a version of the flow is saved
- an LLM judge reads the plan and finds that the ending outputs are named for
  outcomes and that the expectations restate the user's requirements
"""

import json
import re

from evals import assertions, graph
from evals.assertions import Check
from evals.record import ToolCall
from evals.scenario import (
    DECLINE_TEST_RUN,
    FLOW_PREFIX,
    INFRASTRUCTURE_QUESTION,
    Judgement,
    Outcome,
    Rule,
    Scenario,
    ScenarioInputs,
    section,
)

FLOW_NAME = "support-triage"

PROMPT = f"""\
Use the agentic-workflows skill to build a Prefect workflow named
{FLOW_PREFIX}{FLOW_NAME}. Each run takes one customer support issue as text.
An AI agent decides whether the issue is a routine question or a suspected
bug. For a question, an agent drafts a reply. A reply is ready only when it
answers the question the customer asked and makes no promise about dates or
refunds. When it isn't, hold the reply and give me the findings and the
question I need to decide. For a suspected bug, an agent writes a bug report
with steps to reproduce. It doesn't connect to Pylon, GitHub, or any other
tool yet, so the run returns the reply or the report instead of sending it.
"""

USER = [
    DECLINE_TEST_RUN,
    Rule(label="end", after_tool="publish_plan", reply=None),
    Rule(
        label="reachable-systems",
        before_tool="validate_plan",
        pattern=INFRASTRUCTURE_QUESTION,
        reply="None for now. The workflow only uses the issue text I paste in.",
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

# The agent says the workspace can't check the requirements yet.
CANNOT_CHECK = (
    r"(can't|cannot|can not|couldn't|could not|doesn't|does not|isn't|is not"
    r"|not yet|unable|not by|not using|didn't use|did not use|won't use)"
    r"[^.\n]*(check|verif|expectation|requirement)"
    r"|(expectation|requirement|check)[^.\n]*"
    r"(can't|cannot|not (yet )?(available|supported|enabled|checked|written)"
    r"|left out)"
)


def supports_expectations(calls: list[ToolCall]) -> bool:
    """Return whether any `get_schema` result lists schema version 0.3."""
    return any(
        isinstance(call.result, dict)
        and "0.3" in call.result.get("supported_schema_versions", [])
        for call in assertions.calls_to(calls, "get_schema")
    )


def outputs_with_expectations(plan: graph.Plan) -> list[str]:
    return [
        f"{node_id}.{output}"
        for node_id, node in graph.nodes(plan).items()
        for output, port in (node.get("outputs") or {}).items()
        if isinstance(port, dict) and port.get("expectations")
    ]


def check_expectations_follow_schema(outcome: Outcome) -> Check:
    """Check for expectations when `get_schema` lists 0.3, and otherwise for no
    expectations and a message that the workspace can't check them."""
    plan = outcome.plan
    found = outputs_with_expectations(plan)
    if supports_expectations(outcome.transcript.tool_calls):
        problems = []
        if plan.get("schema_version") != "0.3":
            problems.append(f"version {plan.get('schema_version')}")
        if not found:
            problems.append("no output has expectations")
    else:
        problems = [f"expectations on {found} without 0.3"] if found else []
        if not any(
            re.search(CANNOT_CHECK, text, re.IGNORECASE)
            for text in outcome.transcript.agent_text
        ):
            problems.append("no message says the requirements aren't checked")
    return Check(
        "expectations follow the schema versions get_schema lists",
        not problems,
        "; ".join(problems),
    )


def checks(outcome: Outcome) -> list[Check]:
    calls = outcome.transcript.tool_calls
    return [
        assertions.check_plan_outputs(outcome.plan, {"result"}),
        assertions.check_result_records_ending_outputs(outcome.plan),
        assertions.check_one_of_reads_one_node(outcome.plan),
        check_expectations_follow_schema(outcome),
        assertions.check_publish_succeeded(calls),
        assertions.check_published_only_after_valid(calls),
        assertions.check_flow_saved(outcome.flows, FLOW_NAME),
        assertions.check_validated_only_after_approval(
            calls, outcome.transcript.first_reply("design-approval")
        ),
    ]


OUTCOME_RUBRIC = """\
The plan is a support workflow that sorts an issue into a question or a
suspected bug, replies to questions, holds a reply that misses the user's
requirements, and writes a bug report for a suspected bug. Each node output
with no outgoing edge is named for the outcome it stands for, such as
`reply_ready`, `reply_held`, `bug_reported`, or `blocked`, and not for a step
or a data type, such as `output`, `done`, or `data`. The plan output `result`
has one field for each of those outputs. When the plan declares
`expectations`, each one restates a requirement from the request: the reply
answers the question the customer asked, or it makes no promise about dates
or refunds. A plan whose ending outputs have generic names, or whose
expectations add a requirement the user didn't state, does not pass.
"""


def plan_evidence(outcome: Outcome) -> str:
    plan = outcome.published_plan(FLOW_NAME)
    if not plan:
        return ""
    return section("Request", PROMPT) + section("Plan", json.dumps(plan, indent=2))


SCENARIO = Scenario(
    "support_triage",
    ScenarioInputs(prompt=PROMPT, user=USER),
    checks,
    judgements=[
        Judgement(
            "judge: ending outputs are named for outcomes",
            OUTCOME_RUBRIC,
            plan_evidence,
        )
    ],
)
