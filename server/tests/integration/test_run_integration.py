"""Integration tests for the run tools against a real Cloud workspace.

Each test publishes a one-node plan to its own flow, starts a run, and watches
it with `get_run` until it finishes.
"""

from time import monotonic
from typing import Any

from fastmcp import Client

FLOW_NAME_PREFIX = "prefect-agentic-workflows-integration-run"
FINISHED = {"completed", "failed", "cancelled"}
WATCH_LIMIT_SECONDS = 300

ORCHESTRATION = {
    "output_selection": "exactly_one",
    "evaluate_when": "all_required_inputs_produced",
}

AGENT_PLAN: dict[str, Any] = {
    "kind": "ExecutionPlan",
    "schema_version": "0.1",
    "nodes": {
        "reply": {
            "kind": "AgentNode",
            "objective": "Reply with the word done and select the `done` output.",
            "outputs": {"done": {"schema": True}},
            "orchestration": ORCHESTRATION,
        }
    },
    "edges": [],
}

HUMAN_INPUT_PLAN: dict[str, Any] = {
    "kind": "ExecutionPlan",
    "schema_version": "0.1",
    "nodes": {
        "approve": {
            "kind": "HumanInputNode",
            "human_input": {"deadline": {"after": "PT10M"}},
            "outputs": {"answer": {"schema": {"type": "object"}}},
            "orchestration": ORCHESTRATION,
        }
    },
    "edges": [],
}


async def publish(mcp_client: Client[Any], name: str, plan: dict[str, Any]) -> str:
    """Publish and activate `plan` on the flow called `name`. Return the flow ID."""
    flow = await mcp_client.call_tool("get_or_create_flow", {"name": name})
    assert flow.structured_content is not None
    flow_id = flow.structured_content["flow"]["id"]

    published = await mcp_client.call_tool(
        "publish_plan", {"flow_id": flow_id, "plan": plan}
    )
    assert published.structured_content is not None
    assert published.structured_content["activated"], published.structured_content
    return flow_id


async def start(mcp_client: Client[Any], flow_id: str) -> str:
    started = await mcp_client.call_tool("start_run", {"flow_id": flow_id})
    assert started.structured_content is not None
    assert started.structured_content["created"] is True
    return started.structured_content["flow_run_id"]


async def watch(
    mcp_client: Client[Any], flow_run_id: str, until: set[str]
) -> dict[str, Any]:
    """Call `get_run` with a wait until the run's status is in `until`."""
    deadline = monotonic() + WATCH_LIMIT_SECONDS
    while True:
        result = await mcp_client.call_tool(
            "get_run", {"flow_run_id": flow_run_id, "wait_seconds": 30}
        )
        run = result.structured_content
        assert run is not None
        assert run["wait"]["seconds_waited"] <= 31
        if run["status"] in until:
            return run
        assert monotonic() < deadline, f"run did not reach {until}: {run}"


async def test_a_one_node_agent_plan_runs_to_completion(mcp_client: Client[Any]):
    flow_id = await publish(mcp_client, f"{FLOW_NAME_PREFIX}-agent", AGENT_PLAN)
    flow_run_id = await start(mcp_client, flow_id)

    run = await watch(mcp_client, flow_run_id, FINISHED)

    assert run["status"] == "completed", run
    (node,) = run["nodes"]
    assert node["node"] == "reply"
    assert node["status"] == "completed"


async def test_a_human_input_plan_finishes_after_the_form_is_answered(
    mcp_client: Client[Any],
):
    flow_id = await publish(
        mcp_client, f"{FLOW_NAME_PREFIX}-human-input", HUMAN_INPUT_PLAN
    )
    flow_run_id = await start(mcp_client, flow_id)

    waiting = await watch(
        mcp_client, flow_run_id, {"awaiting_external_progress", *FINISHED}
    )
    (node,) = waiting["nodes"]
    assert node["wait"]["kind"] == "human_input", waiting
    activation_id = node["wait"]["activation_id"]

    submitted = await mcp_client.call_tool(
        "submit_human_input",
        {
            "flow_run_id": flow_run_id,
            "activation_id": activation_id,
            "response": {"approved": True},
        },
    )
    assert submitted.structured_content is not None
    assert submitted.structured_content["submitted"] is True

    run = await watch(mcp_client, flow_run_id, FINISHED)
    assert run["status"] == "completed", run

    output = await mcp_client.call_tool(
        "get_run_output",
        {
            "flow_run_id": flow_run_id,
            "output_name": "answer",
            "activation_id": activation_id,
        },
    )
    assert output.structured_content is not None
    assert output.structured_content["available"] is True
    assert output.structured_content["value"] == {"approved": True}
