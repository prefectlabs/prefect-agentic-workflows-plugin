"""Set up one OAuth and one API-key MCP connection, and reference both in a plan.

The user wants a daily digest of billing issues from Linear, with each
customer looked up in Stripe. Linear's MCP server signs in with OAuth. The
user wants Stripe to use a restricted API key, although its server also
offers OAuth. The user leaves both setup pages for later, so the version
publishes with warnings. The user turns down the test run.

The account names in the prompt include the run's prefix, so each run's
connection names are unique in the sandbox, and teardown deletes them.

Expected results:

- the agent calls `discover_mcp_server` for each server before it creates
  that server's connection
- the Linear connection uses `oauth`, and the Stripe connection uses an
  `Authorization` header with the `Bearer ` prefix and no `oauth`
- each connection is named after its provider and the user's account
- the agent never asks the user to type the API key
- the agent sends the user each connection's setup link, unless the client
  opened the setup page
- the published plan references both connections with
  `{"type": "connection", ...}` and has no inline server for them
- `publish_plan` is called only after `validate_plan` passes, and only after
  the design approval
"""

import re
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

FLOW_NAME = "billing-issue-digest"
ACCOUNT = f"{FLOW_PREFIX}acme"
LINEAR_URL = "https://mcp.linear.app/mcp"
STRIPE_URL = "https://mcp.stripe.com"

PROMPT = f"""\
Use the agentic-workflows skill to build a Prefect workflow named
{FLOW_PREFIX}{FLOW_NAME}. Each run reads the Linear issues labeled `billing`
from the last day, looks up each issue's customer in Stripe, and returns a
short digest. It only reads; it changes nothing in Linear or Stripe.

Linear's MCP server is {LINEAR_URL}, and I sign in with my Linear account,
`{ACCOUNT}`. Stripe's MCP server is {STRIPE_URL}. For Stripe, use a
restricted API key, not a Stripe sign-in. My Stripe account is also
`{ACCOUNT}`. Those are the only tools the workflow needs.
"""

SETUP_LATER = "I'll finish the setup pages later."

USER = [
    DECLINE_TEST_RUN,
    Rule(label="end", after_tool="publish_plan", reply=None),
    # A request to paste a credential into the conversation. The skill never
    # asks for one; a check fails when this rule is used.
    Rule(
        label="secret-asked",
        pattern=(
            r"(paste|share|send|give|provide|type)[^.?!\n]*"
            r"(api key|restricted key|token|secret|password)[^.?!\n]*\?"
        ),
        reply="Here it is: rk_test_placeholder",
        max_uses=1,
    ),
    Rule(
        label="reachable-systems",
        before_tool="create_mcp_connection",
        pattern=INFRASTRUCTURE_QUESTION,
        reply="Only the Linear and Stripe servers from my first message.",
        max_uses=1,
    ),
    Rule(
        label="design-approval",
        before_tool="validate_plan",
        pattern=r"confirm|approv|right|build|go ahead|proceed",
        reply=f"Yes, build it. {SETUP_LATER}",
    ),
    Rule(
        label="setup-later",
        after_tool="create_mcp_connection",
        before_tool="validate_plan",
        reply=f"{SETUP_LATER} Go on.",
        max_uses=2,
    ),
    Rule(label="go-ahead", pattern=r"\?", reply="The defaults are fine.", max_uses=3),
]


def for_server(call: ToolCall, url: str) -> bool:
    """Return whether a call's `url` argument is the server at `url`."""
    return str(call.arguments.get("url", "")).rstrip("/") == url


def creates(calls: list[ToolCall], url: str) -> list[ToolCall]:
    """Return the `create_mcp_connection` calls for the server at `url`."""
    return [
        call
        for call in assertions.calls_to(calls, "create_mcp_connection")
        if for_server(call, url)
    ]


def created_connection(call: ToolCall) -> dict[str, Any] | None:
    """Return the connection a `create_mcp_connection` call saved, or None."""
    if call.is_error or not isinstance(call.result, dict):
        return None
    connection = call.result.get("connection")
    return connection if isinstance(connection, dict) else None


def check_discovered_before_create(calls: list[ToolCall], url: str) -> Check:
    def first(name: str) -> int | None:
        return next(
            (
                index
                for index, call in enumerate(calls)
                if call.name == name and for_server(call, url)
            ),
            None,
        )

    discovered, created = first("discover_mcp_server"), first("create_mcp_connection")
    passed = discovered is not None and created is not None and discovered < created
    return Check(
        f"discover_mcp_server called for {url} before its connection is created",
        passed,
        "" if passed else f"calls: {assertions.tool_names(calls)}",
    )


def check_linear_uses_oauth(calls: list[ToolCall]) -> Check:
    found = creates(calls, LINEAR_URL)
    passed = bool(found) and all(
        call.arguments.get("oauth") is not None
        and not any(
            str(header.get("name", "")).lower() == "authorization"
            for header in call.arguments.get("headers") or []
        )
        for call in found
    )
    return Check(
        "the Linear connection uses oauth",
        passed,
        "" if passed else f"arguments: {[call.arguments for call in found]}",
    )


def check_stripe_uses_a_bearer_header(calls: list[ToolCall]) -> Check:
    found = creates(calls, STRIPE_URL)
    passed = bool(found) and all(
        call.arguments.get("oauth") is None
        and any(
            str(header.get("name", "")).lower() == "authorization"
            and header.get("value_prefix") == "Bearer "
            for header in call.arguments.get("headers") or []
        )
        for call in found
    )
    return Check(
        "the Stripe connection uses an Authorization header with the Bearer prefix",
        passed,
        "" if passed else f"arguments: {[call.arguments for call in found]}",
    )


def check_named_after_provider_and_account(
    calls: list[ToolCall], account: str
) -> Check:
    names = [
        str(call.arguments.get("name", ""))
        for call in assertions.calls_to(calls, "create_mcp_connection")
    ]
    passed = any(
        "linear" in name.lower() and account in name for name in names
    ) and any("stripe" in name.lower() and account in name for name in names)
    return Check(
        "each connection is named after its provider and the account",
        passed,
        "" if passed else f"names: {names}",
    )


def check_setup_links_sent(outcome: Outcome) -> Check:
    text = "\n".join(outcome.transcript.agent_text)
    created = [
        (call, connection)
        for call in assertions.calls_to(
            outcome.transcript.tool_calls, "create_mcp_connection"
        )
        if (connection := created_connection(call)) is not None
    ]
    missing = [
        connection.get("name", "?")
        for call, connection in created
        if call.result.get("setup") not in ("opened", "not_needed")
        and connection.get("setup_url", "") not in text
    ]
    passed = bool(created) and not missing
    return Check(
        "the agent sends each setup link the client didn't open",
        passed,
        "" if passed else f"no link sent for {missing or 'any connection'}",
    )


def check_plan_references_both_connections(outcome: Outcome) -> Check:
    plan = outcome.published_plan(FLOW_NAME)
    servers = [
        server
        for node in graph.nodes(plan).values()
        for server in ((node.get("mcp") or {}).get("mcpServers") or {}).values()
    ]
    referenced = {
        server.get("connection_id")
        for server in servers
        if server.get("type") == "connection"
    }
    created = {
        connection["id"]
        for call in assertions.calls_to(
            outcome.transcript.tool_calls, "create_mcp_connection"
        )
        if (connection := created_connection(call)) is not None
    }
    inline = [
        server.get("url")
        for server in servers
        if server.get("type") == "http"
        and re.search(r"linear\.app|stripe\.com", str(server.get("url", "")))
    ]
    passed = len(created) == 2 and created <= referenced and not inline
    return Check(
        "the plan references both connections and no inline server for them",
        passed,
        ""
        if passed
        else f"created {sorted(created)}, referenced {sorted(map(str, referenced))}, "
        f"inline {inline}",
    )


def checks(outcome: Outcome) -> list[Check]:
    calls = outcome.transcript.tool_calls
    account = f"{outcome.flow_prefix}acme"
    return [
        check_discovered_before_create(calls, LINEAR_URL),
        check_discovered_before_create(calls, STRIPE_URL),
        check_linear_uses_oauth(calls),
        check_stripe_uses_a_bearer_header(calls),
        check_named_after_provider_and_account(calls, account),
        Check(
            "the agent never asks for the API key",
            not outcome.transcript.replies_labeled("secret-asked"),
            "the agent asked the user to type a credential",
        ),
        check_setup_links_sent(outcome),
        check_plan_references_both_connections(outcome),
        assertions.check_publish_succeeded(calls),
        assertions.check_published_only_after_valid(calls),
        assertions.check_validated_only_after_approval(
            calls, outcome.transcript.first_reply("design-approval")
        ),
        assertions.check_flow_saved(outcome.flows, FLOW_NAME),
    ]


SCENARIO = Scenario(
    "mcp_connection_setup",
    ScenarioInputs(prompt=PROMPT, user=USER, max_turns=14),
    checks,
)
