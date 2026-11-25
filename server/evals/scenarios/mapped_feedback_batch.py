"""Build a workflow that sorts every message of a list the run takes.

Each run takes a list of customer feedback messages, each with an ID. An agent
sorts each message and summarizes it, and one report covers them all. The work
for each message is the same and needs no other message, so the plan should map
one agent node over the list. It connects to no tools. The user turns down the
test run.

Expected results:

- an agent node maps the list, one item for each message
- the items are keyed by the message ID
- each node input that reads the mapped node's output takes an object, because
  it receives the results of every item keyed by item
- every output that ends a run requires a `summary`
- `publish_plan` is called only after the design approval and after
  `validate_plan` passes on the same plan
- a version of the flow is saved
- an LLM judge reads the plan and finds the mapped node written for one
  message, an output for a message it can't sort, and a report node that knows
  its input maps each message ID to that message's result
"""

import json
import re
from typing import Any

from evals import assertions, graph
from evals.assertions import Check
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

FLOW_NAME = "feedback-batch"

PROMPT = f"""\
Use the agentic-workflows skill to build a Prefect workflow named
{FLOW_PREFIX}{FLOW_NAME}. Each run takes a list of up to 50 customer feedback
messages, each with an ID and its text. An AI agent sorts each message into a
bug, a feature request, praise, or a complaint, and writes a one-sentence
summary of it. Then one report gives the count for each category and lists the
summaries under their category. It doesn't connect to any tool yet, so the run
returns the report.
"""

USER = [
    DECLINE_TEST_RUN,
    Rule(label="end", after_tool="publish_plan", reply=None),
    Rule(
        label="reachable-systems",
        before_tool="validate_plan",
        pattern=INFRASTRUCTURE_QUESTION,
        reply="None for now. The workflow only uses the messages I pass in.",
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

# A `map.key` path that ends on an ID field, such as `$.id` or `$.message_id`.
ID_KEY = re.compile(r"^\$(\.[A-Za-z0-9_-]+)*\.[A-Za-z0-9_-]*id$", re.IGNORECASE)


def node_map(node: dict[str, Any]) -> dict[str, Any] | None:
    """Return a node's `orchestration.map`, or None when the node isn't mapped."""
    orchestration = node.get("orchestration")
    if not isinstance(orchestration, dict):
        return None
    found = orchestration.get("map")
    return found if isinstance(found, dict) else None


def mapped_nodes(plan: graph.Plan) -> dict[str, dict[str, Any]]:
    """Return the `map` of each mapped agent node, by node ID."""
    return {
        node_id: found
        for node_id, node in graph.nodes(plan).items()
        if node.get("kind") == "AgentNode" and (found := node_map(node)) is not None
    }


def check_maps_the_list(plan: graph.Plan) -> Check:
    """Check that an agent node maps one of the inputs it declares."""
    nodes = graph.nodes(plan)
    mapped = {
        node_id: found.get("input")
        for node_id, found in mapped_nodes(plan).items()
        if found.get("input") in (nodes[node_id].get("inputs") or {})
    }
    return Check(
        "an agent node maps the list of messages",
        bool(mapped),
        "" if mapped else "no agent node sets orchestration.map on an input it has",
    )


def check_keyed_by_id(plan: graph.Plan) -> Check:
    """Check that each mapped node keys its items by an ID field."""
    keys = {node_id: found.get("key") for node_id, found in mapped_nodes(plan).items()}
    passed = bool(keys) and all(
        isinstance(key, str) and ID_KEY.match(key) for key in keys.values()
    )
    return Check(
        "the mapped node keys each item by its ID",
        passed,
        "" if passed else f"found map keys {keys}",
    )


def check_gathered_inputs_take_objects(plan: graph.Plan) -> Check:
    """Check that each input fed by a mapped node's output takes an object.

    A mapped node's output reaches later nodes as one object keyed by item, and
    `validate_plan` doesn't compare the schemas of node-to-node edges.
    """
    mapped = set(mapped_nodes(plan))
    nodes = graph.nodes(plan)
    wrong = []
    for edge in graph.edges(plan):
        source, target = graph.edge_source(edge), graph.edge_target(edge)
        if source.get("type") != "node_output" or source.get("node") not in mapped:
            continue
        port = ((nodes.get(target.get("node")) or {}).get("inputs") or {}).get(
            target.get("input")
        )
        schema = port.get("schema") if isinstance(port, dict) else None
        if not isinstance(schema, dict) or schema.get("type") != "object":
            wrong.append(f"{target.get('node')}.{target.get('input')}")
    return Check(
        "every input that reads the mapped node takes an object",
        not wrong,
        f"not an object schema: {wrong}" if wrong else "",
    )


def checks(outcome: Outcome) -> list[Check]:
    calls = outcome.transcript.tool_calls
    return [
        check_maps_the_list(outcome.plan),
        check_keyed_by_id(outcome.plan),
        check_gathered_inputs_take_objects(outcome.plan),
        assertions.check_ending_outputs_carry_summary(outcome.plan),
        assertions.check_publish_succeeded(calls),
        assertions.check_published_only_after_valid(calls),
        assertions.check_flow_saved(outcome.flows, FLOW_NAME),
        assertions.check_validated_only_after_approval(
            calls, outcome.transcript.first_reply("design-approval")
        ),
    ]


MAPPING_RUBRIC = """\
The plan sorts each customer feedback message of a list into a category,
summarizes it, and writes one report. An agent node has `orchestration.map`,
so it runs once for each message.

Judge three things. First, the mapped node's objective is written for one
message, the one in its mapped input, not for the whole list. Second, the
mapped node has an output for a message it can't sort, such as `blocked`, with
a reason, and its objective says when to select it. Third, the node that
writes the report reads the mapped node's output, and its objective says that
the input holds the result of each message keyed by the message's ID, or by
item. Ignore the names of nodes, outputs, and the workflow.
"""


def plan_evidence(outcome: Outcome) -> str:
    plan = outcome.published_plan(FLOW_NAME)
    if not plan:
        return ""
    request = PROMPT.replace(FLOW_PREFIX, outcome.flow_prefix)
    return section("Request", request) + section("Plan", json.dumps(plan, indent=2))


SCENARIO = Scenario(
    "mapped_feedback_batch",
    ScenarioInputs(prompt=PROMPT, user=USER),
    checks,
    judgements=[
        Judgement(
            "judge: the mapped node handles one message and the report reads all",
            MAPPING_RUBRIC,
            plan_evidence,
        ),
    ],
)
