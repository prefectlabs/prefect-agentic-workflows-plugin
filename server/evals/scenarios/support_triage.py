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
  node output carries expectations; otherwise the plan has no expectations
- an LLM judge reads the agent's messages and finds that, without 0.3, it
  tells the user that Prefect won't check the requirements
- `publish_plan` is called only after `validate_plan` passes on the same plan
- a version of the flow is saved
- an LLM judge reads the plan and finds that the ending outputs are named for
  outcomes and that the expectations restate the user's requirements
"""

import json

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


def outputs_with_expectations(plan: graph.Plan) -> list[str]:
    return [
        f"{node_id}.{output}"
        for node_id, node in graph.nodes(plan).items()
        for output, port in (node.get("outputs") or {}).items()
        if isinstance(port, dict) and port.get("expectations")
    ]


def schema_versions(calls: list[ToolCall]) -> list[str]:
    """Return the schema versions the `get_schema` results list."""
    return sorted(
        {
            version
            for call in assertions.calls_to(calls, "get_schema")
            if isinstance(call.result, dict)
            for version in call.result.get("supported_schema_versions", [])
        }
    )


def check_expectations_follow_schema(outcome: Outcome) -> Check:
    """Check for expectations when `get_schema` lists 0.3, and for none otherwise."""
    plan = outcome.plan
    found = outputs_with_expectations(plan)
    if "0.3" in schema_versions(outcome.transcript.tool_calls):
        problems = []
        if plan.get("schema_version") != "0.3":
            problems.append(f"version {plan.get('schema_version')}")
        if not found:
            problems.append("no output has expectations")
    else:
        problems = [f"expectations on {found} without 0.3"] if found else []
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
or refunds. A plan whose ending outputs have generic names or name the data
they carry, such as `bug_report`, or whose expectations add a requirement the
user didn't state, does not pass. Judge only the outputs and expectations,
not the workflow's name.
"""


UNCHECKED_RUBRIC = """\
The evidence lists the schema versions the workspace supports and the agent's
messages to the user. Schema version 0.3 is what lets Prefect check the
user's reply rules as expectations. When 0.3 is not listed, the agent tells
the user, in any wording, that Prefect itself won't check those rules in this
workspace, for example that it left out expectations or that the rules are
checked by a workflow step instead of the platform. When 0.3 is listed, the
case passes. A report that presents the rules as checked by Prefect when 0.3
is not listed does not pass.
"""


def report_evidence(outcome: Outcome) -> str:
    transcript = outcome.transcript
    if not transcript.turn_results:
        return ""
    versions = ", ".join(schema_versions(transcript.tool_calls)) or "none"
    return section("SchemaVersions", versions) + section(
        "AgentMessages", "\n\n".join(transcript.turn_results)
    )


def plan_evidence(outcome: Outcome) -> str:
    plan = outcome.published_plan(FLOW_NAME)
    if not plan:
        return ""
    request = PROMPT.replace(FLOW_PREFIX, outcome.flow_prefix)
    return section("Request", request) + section("Plan", json.dumps(plan, indent=2))


SCENARIO = Scenario(
    "support_triage",
    ScenarioInputs(prompt=PROMPT, user=USER),
    checks,
    judgements=[
        Judgement(
            "judge: ending outputs are named for outcomes",
            OUTCOME_RUBRIC,
            plan_evidence,
        ),
        Judgement(
            "judge: the agent says Prefect won't check the requirements",
            UNCHECKED_RUBRIC,
            report_evidence,
        ),
    ],
)
