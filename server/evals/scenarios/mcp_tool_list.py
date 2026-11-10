"""Limit an agent node to the read-only tools of an MCP connection.

The user wants a workflow that answers a question from the Prefect docs. The
docs MCP server needs no sign-in, so its connection works as soon as it is
created. The server lists two read-only tools and one that sends feedback.
The user says the workflow only reads, so the agent node must get only the
read-only tools. The user turns down the test run.

The connection's name in the prompt includes the run's prefix, so each run's
connection name is unique in the sandbox, and teardown deletes it.

Expected results:

- the connection has the name the user gave
- the agent reads the connection's tools, from `check_mcp_connection` or
  `get_mcp_connection`, before it validates a plan
- the published plan references the connection with
  `{"type": "connection", ...}` and a `tools` list that names only read-only
  tools of the server
- `publish_plan` is called only after `validate_plan` passes, and only after
  the design approval
"""

from typing import Any

from evals import assertions, graph
from evals.assertions import Check
from evals.record import ToolCall
from evals.scenario import (
    DECLINE_TEST_RUN,
    FLOW_PREFIX,
    INFRASTRUCTURE_QUESTION,
    Outcome,
    Rule,
    Scenario,
    ScenarioInputs,
)

FLOW_NAME = "docs-answer"
CONNECTION_NAME = "prefect-docs"
DOCS_URL = "https://docs.prefect.io/mcp"
# The server's tools whose `readOnlyHint` is true. It also lists
# `submit_feedback`, which is not read-only.
READ_ONLY_TOOLS = {"search_prefect", "query_docs_filesystem_prefect"}

PROMPT = f"""\
Use the agentic-workflows skill to build a Prefect workflow named
{FLOW_PREFIX}{FLOW_NAME}. Each run takes a question about Prefect and answers
it from the Prefect docs, with a link to each page it used.

The docs MCP server is {DOCS_URL}. It needs no sign-in. Name its connection
`{FLOW_PREFIX}{CONNECTION_NAME}`. That is the only tool the workflow needs.

The workflow only reads the docs. Its agent must not be able to send feedback
or change anything, so limit it to the server's read-only tools.
"""

USER = [
    DECLINE_TEST_RUN,
    Rule(label="end", after_tool="publish_plan", reply=None),
    Rule(
        label="reachable-systems",
        before_tool="create_mcp_connection",
        pattern=INFRASTRUCTURE_QUESTION,
        reply="Only the docs server from my first message.",
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


def listed_tools(call: ToolCall) -> list[Any] | None:
    """Return the tools in the connection a call returned, or None without a list."""
    if call.is_error or not isinstance(call.result, dict):
        return None
    # `create_mcp_connection` returns the connection under `connection`.
    connection = call.result.get("connection", call.result)
    check = connection.get("last_check") if isinstance(connection, dict) else None
    tools = check.get("tools") if isinstance(check, dict) else None
    return tools if isinstance(tools, list) else None


def created_connection_ids(calls: list[ToolCall]) -> set[str]:
    return {
        call.result["connection"]["id"]
        for call in assertions.calls_to(calls, "create_mcp_connection")
        if not call.is_error
        and isinstance(call.result, dict)
        and isinstance(call.result.get("connection"), dict)
    }


def check_connection_named(calls: list[ToolCall], name: str) -> Check:
    names = [
        str(call.arguments.get("name", ""))
        for call in assertions.calls_to(calls, "create_mcp_connection")
        if not call.is_error
    ]
    passed = name in names
    return Check(
        "the connection has the name the user gave",
        passed,
        "" if passed else f"names: {names}",
    )


def check_tools_read_before_validate(calls: list[ToolCall]) -> Check:
    first_validate = next(
        (index for index, call in enumerate(calls) if call.name == "validate_plan"),
        len(calls),
    )
    passed = any(listed_tools(call) for call in calls[:first_validate])
    return Check(
        "the agent reads the connection's tools before it validates a plan",
        passed,
        "" if passed else f"calls: {assertions.tool_names(calls)}",
    )


def check_limited_to_read_only_tools(outcome: Outcome) -> Check:
    created = created_connection_ids(outcome.transcript.tool_calls)
    servers = [
        server
        for node in graph.nodes(outcome.published_plan(FLOW_NAME)).values()
        for server in ((node.get("mcp") or {}).get("mcpServers") or {}).values()
        if isinstance(server, dict)
    ]
    docs = [
        server
        for server in servers
        if server.get("type") == "connection" and server.get("connection_id") in created
    ]
    tool_lists = [server.get("tools") for server in docs]
    passed = bool(docs) and all(
        isinstance(tools, list) and tools and set(tools) <= READ_ONLY_TOOLS
        for tools in tool_lists
    )
    return Check(
        "the plan limits the docs connection to read-only tools",
        passed,
        "" if passed else f"servers: {servers}, created connections: {sorted(created)}",
    )


def checks(outcome: Outcome) -> list[Check]:
    calls = outcome.transcript.tool_calls
    return [
        check_connection_named(calls, f"{outcome.flow_prefix}{CONNECTION_NAME}"),
        check_tools_read_before_validate(calls),
        check_limited_to_read_only_tools(outcome),
        assertions.check_publish_succeeded(calls),
        assertions.check_published_only_after_valid(calls),
        assertions.check_validated_only_after_approval(
            calls, outcome.transcript.first_reply("design-approval")
        ),
        assertions.check_flow_saved(outcome.flows, FLOW_NAME),
    ]


SCENARIO = Scenario(
    "mcp_tool_list",
    ScenarioInputs(prompt=PROMPT, user=USER),
    checks,
)
