"""Tests for the checks of the service identity scenario.

The checks run on a hand-written outcome that passes, and on broken ones that
should fail exactly the listed checks.
"""

import pytest
from harness_outcomes import (
    PREFIX,
    connection_plan,
    created,
    outcome,
    published,
    saved_flow,
)

from evals.record import Reply, ToolCall, Transcript
from evals.scenario import next_rule
from evals.scenarios import mcp_connection_service_identity as scenario

BIGQUERY_ID = "0199c3a2-0000-7000-8000-000000000003"
ACCOUNT = f"{PREFIX}acme-analytics"


def bigquery(broken: bool):
    call = created(
        BIGQUERY_ID,
        f"BigQuery ({ACCOUNT})",
        scenario.BIGQUERY_URL,
        service_identity={"type": "google_service_account"},
    )
    plan = connection_plan({"type": "connection", "connection_id": BIGQUERY_ID})
    discovers = [
        ToolCall("discover_mcp_server", {"url": scenario.BIGQUERY_URL}, turn=1)
    ]
    links = [call.result["connection"]["setup_url"]]
    replies = [Reply(1, "design-approval", "")]
    if broken:
        discovers = []
        call.arguments.pop("service_identity")
        call.arguments["oauth"] = {}
        call.arguments["name"] = f"Google ({ACCOUNT})"
        call.is_error = True
        plan = connection_plan({"type": "http", "url": scenario.BIGQUERY_URL})
        replies.append(Reply(1, "secret-asked", ""))
    calls = [*discovers, call, *published(plan, scenario.FLOW_NAME, turn=2)]
    transcript = Transcript(
        tool_calls=calls,
        agent_text=[f"Finish setup at {link}" for link in links],
        replies=replies,
    )
    return outcome(transcript, plan, [saved_flow(scenario.FLOW_NAME, plan)])


BROKEN_CHECKS = [
    "discover_mcp_server called for https://bigquery.googleapis.com/mcp before "
    "its connection is created",
    "the BigQuery connection uses a Google service account identity",
    "the connection is named after BigQuery and the account",
    "the agent never asks for the key file",
    "the agent sends each setup link the client didn't open",
    "the plan references the connection and no inline server for it",
]


@pytest.mark.parametrize("broken", [False, True], ids=["expected", "broken"])
def test_scenario_checks(broken: bool):
    failed = [
        check.name for check in scenario.checks(bigquery(broken)) if not check.passed
    ]

    assert failed == (BROKEN_CHECKS if broken else [])


def test_an_authorization_header_beside_the_identity_fails_the_method_check():
    result = bigquery(broken=False)
    call = next(
        call
        for call in result.transcript.tool_calls
        if call.name == "create_mcp_connection"
    )
    call.arguments["headers"] = [{"name": "Authorization", "value_prefix": "Bearer "}]

    failed = [check.name for check in scenario.checks(result) if not check.passed]

    assert failed == ["the BigQuery connection uses a Google service account identity"]


@pytest.mark.parametrize(
    "message",
    [
        "Can you paste the service account key file here?",
        "What is the private key for the service account?",
        "Please share the JSON key so I can add it?",
    ],
)
def test_the_simulated_user_hears_a_request_for_the_key(message: str):
    rule = next_rule(scenario.USER, message, Transcript())

    assert rule is not None
    assert rule.label == "secret-asked"


def test_the_simulated_user_does_not_mistake_setup_talk_for_a_request():
    rule = next_rule(
        scenario.USER,
        "You will paste the key file on the setup page in Prefect Cloud. Go on?",
        Transcript(),
    )

    assert rule is None or rule.label != "secret-asked"
