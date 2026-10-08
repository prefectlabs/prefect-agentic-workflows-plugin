"""Checks a scenario runs on the plan the agent wrote, its tool calls, and its flows.

Each `check_*` function returns a `Check`, with a detail that says what was
found when the check fails, so the runner can list every result in its table
instead of stopping at the first failure. The other functions read facts from
a plan for checks that a scenario writes itself.
"""

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from evals import graph
from evals.record import FlowState, Reply, ToolCall

Plan = graph.Plan


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: str = ""


# Facts about a plan.


def node_kinds(plan: Plan) -> dict[str, str]:
    """Return each node's `kind`, by node ID."""
    return {
        node_id: str(node.get("kind")) for node_id, node in graph.nodes(plan).items()
    }


def nodes_of_kind(plan: Plan, kind: str) -> list[str]:
    return [node_id for node_id, found in node_kinds(plan).items() if found == kind]


def branches(plan: Plan, node_id: str) -> dict[str, set[str]]:
    """Return the nodes each output of a node leads to, by output name.

    An output that no edge starts from maps to an empty set.
    """
    node = graph.nodes(plan).get(node_id, {})
    result: dict[str, set[str]] = {name: set() for name in graph.output_names(node)}
    for source, output, target in graph.node_edges(plan):
        if source == node_id:
            result.setdefault(output, set()).add(target)
    return result


def plan_inputs(plan: Plan) -> set[str]:
    inputs = plan.get("inputs")
    return set(inputs) if isinstance(inputs, dict) else set()


def plan_outputs(plan: Plan) -> set[str]:
    outputs = plan.get("outputs")
    return set(outputs) if isinstance(outputs, dict) else set()


# Checks on a plan.


def check_no_cycle(plan: Plan) -> Check:
    cyclic = graph.find_cycle(plan)
    return Check(
        "plan has no cycle",
        not cyclic,
        f"nodes in or after a cycle: {', '.join(cyclic)}" if cyclic else "",
    )


def check_has_node_kind(plan: Plan, kind: str, *, at_least: int = 1) -> Check:
    found = nodes_of_kind(plan, kind)
    return Check(
        f"plan has at least {at_least} {kind}",
        len(found) >= at_least,
        f"found {len(found)}: {', '.join(found) or 'none'}",
    )


def check_branches(plan: Plan, node_id: str, expected: Mapping[str, set[str]]) -> Check:
    """Check that each named output of a node leads to exactly the expected nodes."""
    found = branches(plan, node_id)
    wrong = {
        output: sorted(found.get(output, set()))
        for output, targets in expected.items()
        if found.get(output, set()) != targets
    }
    return Check(
        f"branches from {node_id}",
        not wrong,
        f"found {found}" if wrong else "",
    )


def check_plan_inputs(plan: Plan, expected: set[str], *, exact: bool = False) -> Check:
    found = plan_inputs(plan)
    passed = found == expected if exact else expected <= found
    return Check(
        f"plan inputs include {sorted(expected)}",
        passed,
        "" if passed else f"found {sorted(found)}",
    )


def check_plan_outputs(plan: Plan, expected: set[str], *, exact: bool = False) -> Check:
    found = plan_outputs(plan)
    passed = found == expected if exact else expected <= found
    return Check(
        f"plan outputs include {sorted(expected)}",
        passed,
        "" if passed else f"found {sorted(found)}",
    )


# Checks on the tool calls.


def tool_names(calls: Iterable[ToolCall]) -> list[str]:
    return [call.name for call in calls]


def calls_to(calls: Iterable[ToolCall], name: str) -> list[ToolCall]:
    return [call for call in calls if call.name == name]


def check_called(
    calls: list[ToolCall],
    name: str,
    *,
    times: int | None = None,
    where: Callable[[ToolCall], bool] | None = None,
) -> Check:
    """Check that a tool was called, `times` times when it is set.

    `where` counts only the calls it returns true for, for example the calls
    with a given argument.
    """
    matching = [call for call in calls_to(calls, name) if where is None or where(call)]
    passed = len(matching) == times if times is not None else bool(matching)
    label = f"{name} called {times} times" if times is not None else f"{name} called"
    return Check(label, passed, "" if passed else f"called {len(matching)} times")


def check_publish_succeeded(calls: list[ToolCall]) -> Check:
    """Check that at least one `publish_plan` call saved a version."""
    saved = [
        call
        for call in calls_to(calls, "publish_plan")
        if not call.is_error
        and isinstance(call.result, dict)
        and call.result.get("published") is True
    ]
    tried = len(calls_to(calls, "publish_plan"))
    return Check(
        "publish_plan saved a version",
        bool(saved),
        "" if saved else f"{tried} publish_plan calls, none saved a version",
    )


def check_never_called(calls: list[ToolCall], name: str) -> Check:
    count = len(calls_to(calls, name))
    return Check(
        f"{name} never called",
        count == 0,
        f"called {count} times" if count else "",
    )


def activating_calls(calls: Iterable[ToolCall]) -> list[ToolCall]:
    """Return the calls that make a plan version active.

    These are `activate_plan_version`, and `publish_plan` unless its
    `activate` argument is false.
    """
    return [
        call
        for call in calls
        if call.name == "activate_plan_version"
        or (call.name == "publish_plan" and call.arguments.get("activate") is not False)
    ]


def check_only_after_reply(
    name: str, calls: Iterable[ToolCall], reply: Reply | None
) -> Check:
    """Check that every call came in an agent turn after the simulated user's reply.

    With no reply, the check passes only when there are no calls.
    """
    early = [call for call in calls if reply is None or call.turn <= reply.turn]
    if not early:
        return Check(name, True)
    when = "before the user answered" if reply else "and the user never answered"
    return Check(
        name,
        False,
        f"{', '.join(sorted({call.name for call in early}))} called in turn "
        f"{early[0].turn} {when}",
    )


def check_called_in_order(calls: list[ToolCall], names: list[str]) -> Check:
    """Check that the tools were called in this order, with any calls between them."""
    remaining = list(names)
    for call in calls:
        if remaining and call.name == remaining[0]:
            remaining.pop(0)
    return Check(
        f"called in order: {' then '.join(names)}",
        not remaining,
        f"never reached {remaining[0]}" if remaining else "",
    )


def check_published_only_after_valid(calls: list[ToolCall]) -> Check:
    """Check that each `publish_plan` follows a passing `validate_plan` of its plan.

    A plan that validated, then changed, doesn't count as validated.
    """
    validated: list[Any] = []
    for index, call in enumerate(calls):
        if call.name == "validate_plan":
            result = call.result if isinstance(call.result, dict) else {}
            if not call.is_error and result.get("valid") is True:
                validated.append(call.arguments.get("plan"))
        elif (
            call.name == "publish_plan" and call.arguments.get("plan") not in validated
        ):
            return Check(
                "publish_plan only after validate_plan passed",
                False,
                f"tool call {index + 1} published a plan that no passing "
                "validate_plan call checked",
            )
    return Check("publish_plan only after validate_plan passed", True)


def check_flows_named_with_prefix(calls: list[ToolCall], prefix: str) -> Check:
    """Check that every flow the agent created has a name that starts with `prefix`.

    The harness deletes only the flows with the prefix, so a flow without it
    stays in the sandbox after the session.
    """
    names = [
        str(call.arguments.get("name"))
        for call in calls_to(calls, "get_or_create_flow")
    ]
    wrong = sorted({name for name in names if not name.startswith(prefix)})
    return Check(
        f"every flow the agent created starts with {prefix!r}",
        not wrong,
        f"get_or_create_flow called with {wrong}" if wrong else "",
    )


# Checks on the flows in the sandbox after the conversation.


def check_flow_saved(flows: Mapping[str, FlowState], name: str) -> Check:
    """Check that the flow exists in the sandbox and has at least one plan version."""
    flow = flows.get(name)
    passed = flow is not None and bool(flow.version_ids)
    if flow is None:
        detail = f"no flow {name!r}; found {sorted(flows) or 'none'}"
    else:
        detail = "" if passed else f"the flow {name!r} has no plan version"
    return Check(f"the flow {name!r} has a saved plan version", passed, detail)


# Checks on what the agent said.


def check_text_mentions(
    name: str, text: str, patterns: Mapping[str, str], *, flags: int = re.IGNORECASE
) -> Check:
    """Check that the text matches every regular expression in `patterns`.

    `patterns` maps a short description of each expected part to its pattern.
    """
    missing = [
        part
        for part, pattern in patterns.items()
        if not re.search(pattern, text, flags)
    ]
    return Check(name, not missing, f"missing: {', '.join(missing)}" if missing else "")
