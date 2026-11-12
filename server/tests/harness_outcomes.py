"""Hand-written outcomes for the scenario check tests.

Shared by the scenario tests in `tests/eval_harness/`, so each one builds a
transcript and its flows the same way.
"""

from pathlib import Path
from typing import Any

from evals.record import FlowState, ToolCall, Transcript
from evals.scenario import Outcome

PREFIX = "eval-abc123-1-"
VALID = {"valid": True, "errors": []}


def outcome(
    transcript: Transcript,
    plan: dict[str, Any] | None = None,
    flows: list[FlowState] | None = None,
) -> Outcome:
    plans = {"workflow.plan.json": plan} if plan is not None else {}
    by_name = {flow.name.removeprefix(PREFIX): flow for flow in flows or []}
    return Outcome(transcript, plans, by_name, Path("."), PREFIX)


def saved_flow(name: str, plan: dict[str, Any]) -> FlowState:
    """Return a flow with one saved and active version of the plan."""
    return FlowState(
        id="flow-1",
        name=f"{PREFIX}{name}",
        active_version_id="version-1",
        active_plan=plan,
        version_ids=["version-1"],
    )


def published(plan: dict[str, Any], flow: str, turn: int = 1) -> list[ToolCall]:
    return [
        ToolCall("get_or_create_flow", {"name": f"{PREFIX}{flow}"}, turn=turn),
        ToolCall("validate_plan", {"plan": plan}, VALID, turn=turn),
        ToolCall(
            "publish_plan",
            {"plan": plan},
            {"published": True, "activated": True},
            turn=turn,
        ),
    ]


def created(connection_id: str, name: str, url: str, **arguments: Any) -> ToolCall:
    """Return a `create_mcp_connection` call for a client that can't open links."""
    return ToolCall(
        "create_mcp_connection",
        {"name": name, "url": url, **arguments},
        {
            "setup": "client_cannot_open_links",
            "connection": {
                "id": connection_id,
                "name": name,
                "setup_url": f"https://app.prefect.cloud/connections/{connection_id}/setup",
            },
        },
        turn=1,
    )


def connection_plan(*servers: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": "ExecutionPlan",
        "nodes": {
            "digest": {
                "kind": "AgentNode",
                "mcp": {"mcpServers": {f"server{n}": s for n, s in enumerate(servers)}},
            }
        },
    }
