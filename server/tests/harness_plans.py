"""Plans that the harness tests publish, run, and check."""

import copy
from typing import Any

TEXT = {"type": "string"}
DRAFT = {
    "type": "object",
    "properties": {"draft": TEXT},
    "required": ["draft"],
}
APPROVAL_FORM = {
    "type": "object",
    "title": "Publish this draft?",
    "properties": {
        "decision": {"type": "string", "enum": ["approved", "rejected"]},
        "notes": TEXT,
    },
    "required": ["decision"],
}


def port(schema: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": schema,
        "expects": {"cardinality": "exactly_one", "shape": "value"},
    }


def orchestration() -> dict[str, Any]:
    return {
        "output_selection": "exactly_one",
        "evaluate_when": "all_required_inputs_produced",
    }


def edge(
    edge_id: str, source: dict[str, Any], node: str, node_input: str
) -> dict[str, Any]:
    return {"id": edge_id, "from": source, "to": {"node": node, "input": node_input}}


def from_node(node: str, output: str) -> dict[str, Any]:
    return {"type": "node_output", "node": node, "output": output}


def approval_plan() -> dict[str, Any]:
    """Return a plan that drafts text, asks a person, and publishes on approval.

    The `approve` node selects `approved` or `rejected`. Only `approved` leads
    to `publish`. The plan output `result` is the published text.
    """
    return copy.deepcopy(
        {
            "kind": "ExecutionPlan",
            "schema_version": "0.2",
            "inputs": {"topic": {"schema": TEXT, "required": True}},
            "nodes": {
                "draft": {
                    "kind": "AgentNode",
                    "objective": "Write a short draft about the topic.",
                    "inputs": {"topic": port(TEXT)},
                    "outputs": {"drafted": {"schema": DRAFT}},
                    "orchestration": orchestration(),
                },
                "approve": {
                    "kind": "HumanInputNode",
                    "human_input": {"form_schema": APPROVAL_FORM},
                    "inputs": {"draft": port(DRAFT)},
                    "outputs": {
                        "approved": {"schema": {"type": "object"}},
                        "rejected": {"schema": {"type": "object"}},
                    },
                    "orchestration": orchestration(),
                },
                "publish": {
                    "kind": "AgentNode",
                    "objective": "Publish the approved draft.",
                    "inputs": {"draft": port(DRAFT), "approval": port({})},
                    "outputs": {"published": {"schema": DRAFT}},
                    "orchestration": orchestration(),
                },
            },
            "edges": [
                edge(
                    "topic-to-draft",
                    {"type": "plan_input", "input": "topic"},
                    "draft",
                    "topic",
                ),
                edge(
                    "draft-to-approve",
                    from_node("draft", "drafted"),
                    "approve",
                    "draft",
                ),
                edge(
                    "draft-to-publish",
                    from_node("draft", "drafted"),
                    "publish",
                    "draft",
                ),
                edge(
                    "approved-to-publish",
                    from_node("approve", "approved"),
                    "publish",
                    "approval",
                ),
            ],
            "outputs": {
                "result": {
                    "fields": {
                        "text": {
                            "schema": DRAFT,
                            "source": from_node("publish", "published"),
                        }
                    }
                }
            },
        }
    )


def cyclic_plan() -> dict[str, Any]:
    """Return the approval plan with an edge from `publish` back to `draft`."""
    plan = approval_plan()
    plan["nodes"]["draft"]["inputs"]["feedback"] = port({})
    plan["edges"].append(
        edge("publish-to-draft", from_node("publish", "published"), "draft", "feedback")
    )
    return plan
