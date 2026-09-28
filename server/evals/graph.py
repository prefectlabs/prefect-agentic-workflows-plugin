"""Read the graph of an execution-plan document.

The fake Cloud API uses these functions to validate plans and to run them, and
the assertion helpers use them to check the plan an agent wrote. They take the
plan as the JSON object in the plan file and never raise on a malformed plan:
a missing or wrong-typed part reads as empty.
"""

from typing import Any

Plan = dict[str, Any]


def nodes(plan: Plan) -> dict[str, dict[str, Any]]:
    """Return the plan's nodes by ID, leaving out any that aren't objects."""
    raw = plan.get("nodes")
    if not isinstance(raw, dict):
        return {}
    return {node_id: node for node_id, node in raw.items() if isinstance(node, dict)}


def edges(plan: Plan) -> list[dict[str, Any]]:
    """Return the plan's edges, leaving out any that aren't objects."""
    raw = plan.get("edges")
    if not isinstance(raw, list):
        return []
    return [edge for edge in raw if isinstance(edge, dict)]


def edge_source(edge: dict[str, Any]) -> dict[str, Any]:
    source = edge.get("from")
    return source if isinstance(source, dict) else {}


def edge_target(edge: dict[str, Any]) -> dict[str, Any]:
    target = edge.get("to")
    return target if isinstance(target, dict) else {}


def node_edges(plan: Plan) -> list[tuple[str, str, str]]:
    """Return each edge from a node output as (source node, output, target node).

    Edges from plan inputs are left out, because they can't be part of a cycle.
    """
    result = []
    for edge in edges(plan):
        source = edge_source(edge)
        if source.get("type") != "node_output":
            continue
        result.append(
            (
                str(source.get("node")),
                str(source.get("output")),
                str(edge_target(edge).get("node")),
            )
        )
    return result


def topological_order(plan: Plan) -> tuple[list[str], set[str]]:
    """Return the node IDs in dependency order, and the IDs left in a cycle.

    Nodes with no path between them keep the order they have in the plan.
    A node that is in a cycle, or downstream of one, is not in the order.
    """
    node_ids = list(nodes(plan))
    incoming = dict.fromkeys(node_ids, 0)
    outgoing: dict[str, list[str]] = {node_id: [] for node_id in node_ids}
    for source, _, target in node_edges(plan):
        if source in incoming and target in incoming:
            outgoing[source].append(target)
            incoming[target] += 1

    ready = [node_id for node_id in node_ids if incoming[node_id] == 0]
    order: list[str] = []
    while ready:
        node_id = ready.pop(0)
        order.append(node_id)
        for target in outgoing[node_id]:
            incoming[target] -= 1
            if incoming[target] == 0:
                ready.append(target)
    return order, set(node_ids) - set(order)


def find_cycle(plan: Plan) -> list[str]:
    """Return the sorted IDs of the nodes in or after a cycle, or [] for none."""
    _, left_over = topological_order(plan)
    return sorted(left_over)


def human_input_expiry_output(node: dict[str, Any]) -> str | None:
    """Return the output a human-input node selects when its deadline passes."""
    human_input = node.get("human_input")
    if not isinstance(human_input, dict):
        return None
    deadline = human_input.get("deadline")
    if not isinstance(deadline, dict):
        return None
    on_expiry = deadline.get("on_expiry")
    if not isinstance(on_expiry, dict):
        return None
    output = on_expiry.get("output")
    return output if isinstance(output, str) else None


def output_names(node: dict[str, Any]) -> list[str]:
    outputs = node.get("outputs")
    return list(outputs) if isinstance(outputs, dict) else []


def input_names(node: dict[str, Any]) -> list[str]:
    inputs = node.get("inputs")
    return list(inputs) if isinstance(inputs, dict) else []


def response_outputs(node: dict[str, Any]) -> list[str]:
    """Return the outputs a person's answer to a human-input node can select.

    These are all of the node's outputs except the one its deadline selects.
    """
    expiry = human_input_expiry_output(node)
    return [name for name in output_names(node) if name != expiry]
