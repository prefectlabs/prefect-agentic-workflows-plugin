"""Set up a BigQuery connection that acts as a service account, and use it in a plan.

The user wants a daily summary of a BigQuery query, run as their analytics
service account rather than their own Google sign-in. Google's BigQuery MCP
server lists its tools without credentials, so discovery reports no auth; the
user's instruction decides the method. The user leaves the setup page, where
the key file is pasted, for later. The user turns down the test run.

Expected results:

- the agent calls `discover_mcp_server` for the server before it creates the
  connection
- the connection uses `service_identity` with `type` `google_service_account`,
  no `oauth`, and no `Authorization` header
- the connection is named after BigQuery and the user's account
- the agent never asks the user to paste the key file
- the agent sends the user the setup link, unless the client opened the page
- the published plan references the connection and has no inline server for it
- `publish_plan` is called only after `validate_plan` passes, and only after
  the design approval
"""

from typing import Any

from evals import assertions
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
from evals.scenarios.mcp_connection_setup import (
    check_discovered_before_create,
    check_plan_references_connections,
    check_setup_links_sent,
    creates,
    headers,
)

FLOW_NAME = "revenue-summary"
ACCOUNT = f"{FLOW_PREFIX}acme-analytics"
BIGQUERY_URL = "https://bigquery.googleapis.com/mcp"

PROMPT = f"""\
Use the agentic-workflows skill to build a Prefect workflow named
{FLOW_PREFIX}{FLOW_NAME}. Each run queries yesterday's revenue by region from
the `billing.revenue` table in BigQuery and returns a short summary. It only
reads.

BigQuery's MCP server is {BIGQUERY_URL}. Run it as our analytics service
account, `bq-agent@{ACCOUNT}.iam.gserviceaccount.com`, not as me. The Google
Cloud project is `{ACCOUNT}`. That is the only tool the workflow needs.
"""

SETUP_LATER = "I'll paste the key on the setup page later."

USER = [
    DECLINE_TEST_RUN,
    Rule(label="end", after_tool="publish_plan", reply=None),
    # A request to paste the key file into the conversation. The skill never
    # asks for one; a check fails when this rule is used.
    Rule(
        label="secret-asked",
        pattern=(
            r"(paste|share|send|give|provide|type|upload|what(?:'s| is| are))"
            r"[^.?!\n]*(key file|service account key|private key|json key|"
            r"credentials? file)[^.?!\n]*\?"
        ),
        reply='Here it is: {"type": "service_account", "private_key": "placeholder"}',
        max_uses=1,
    ),
    Rule(
        label="reachable-systems",
        before_tool="create_mcp_connection",
        pattern=INFRASTRUCTURE_QUESTION,
        reply="Only the BigQuery server from my first message.",
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
        max_uses=1,
    ),
    Rule(label="go-ahead", pattern=r"\?", reply="The defaults are fine.", max_uses=3),
]


def uses_google_identity(call: ToolCall) -> bool:
    """Whether a create call's `service_identity` is a Google service account."""
    identity: Any = call.arguments.get("service_identity")
    return (
        isinstance(identity, dict) and identity.get("type") == "google_service_account"
    )


def check_bigquery_uses_a_service_identity(calls: list[ToolCall]) -> Check:
    found = creates(calls, BIGQUERY_URL)
    passed = bool(found) and all(
        uses_google_identity(call)
        and call.arguments.get("oauth") is None
        and not any(
            str(header.get("name", "")).lower() == "authorization"
            for header in headers(call)
        )
        for call in found
    )
    return Check(
        "the BigQuery connection uses a Google service account identity",
        passed,
        "" if passed else f"arguments: {[call.arguments for call in found]}",
    )


def check_named_after_bigquery_and_account(
    calls: list[ToolCall], account: str
) -> Check:
    names = [
        str(call.arguments.get("name", "")) for call in creates(calls, BIGQUERY_URL)
    ]
    passed = any("bigquery" in name.lower() and account in name for name in names)
    return Check(
        "the connection is named after BigQuery and the account",
        passed,
        "" if passed else f"names: {names}",
    )


def checks(outcome: Outcome) -> list[Check]:
    calls = outcome.transcript.tool_calls
    account = f"{outcome.flow_prefix}acme-analytics"
    return [
        check_discovered_before_create(calls, BIGQUERY_URL),
        check_bigquery_uses_a_service_identity(calls),
        check_named_after_bigquery_and_account(calls, account),
        Check(
            "the agent never asks for the key file",
            not outcome.transcript.replies_labeled("secret-asked"),
            "the agent asked the user to paste a credential",
        ),
        check_setup_links_sent(outcome),
        check_plan_references_connections(
            outcome,
            FLOW_NAME,
            1,
            r"googleapis\.com",
            "the plan references the connection and no inline server for it",
        ),
        assertions.check_publish_succeeded(calls),
        assertions.check_published_only_after_valid(calls),
        assertions.check_validated_only_after_approval(
            calls, outcome.transcript.first_reply("design-approval")
        ),
        assertions.check_flow_saved(outcome.flows, FLOW_NAME),
    ]


SCENARIO = Scenario(
    "mcp_connection_service_identity",
    ScenarioInputs(prompt=PROMPT, user=USER, max_turns=14),
    checks,
)
