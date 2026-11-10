"""Tests for a connection that acts as a service identity. No tool carries its key."""

from typing import Any

import pytest
import respx
from fastmcp import Client
from support import error_text, request_body
from test_mcp_connection_tools import (
    CONNECTION_PATH,
    CONNECTIONS_PATH,
    SETUP_URL,
    connection_response,
    connection_result,
)

BIGQUERY_URL = "https://bigquery.googleapis.com/mcp"
SERVICE_ACCOUNT = "bq-agent@acme-analytics.iam.gserviceaccount.com"


def identity_oauth(has_key: bool) -> dict[str, Any]:
    """The `oauth` view Cloud returns for a service identity, with or without a key."""
    return {
        "bearer_token_source": "access_token",
        "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
        "issuer": None,
        "supports_dcr": None,
        "client": None,
        "subject": {"display": SERVICE_ACCOUNT} if has_key else None,
        "authorized_at": "2026-11-06T17:01:00Z" if has_key else None,
        "service_identity": {
            "type": "google_service_account",
            "has_key": has_key,
            "client_email": SERVICE_ACCOUNT if has_key else None,
            "project_id": "acme-analytics" if has_key else None,
            "key_saved_at": "2026-11-06T17:00:00Z" if has_key else None,
        },
    }


def identity_response(state: str, has_key: bool) -> dict[str, Any]:
    return connection_response(
        state,
        name="BigQuery (acme-analytics)",
        url=BIGQUERY_URL,
        auth_method="service_identity",
        oauth=identity_oauth(has_key),
        headers=[],
    )


def identity_result(state: str, has_key: bool) -> dict[str, Any]:
    return connection_result(
        state,
        name="BigQuery (acme-analytics)",
        url=BIGQUERY_URL,
        auth_method="service_identity",
        oauth={
            "bearer_token_source": "access_token",
            "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
            "issuer": None,
            "supports_dcr": None,
            "service_identity": {
                "type": "google_service_account",
                "has_key": has_key,
                "client_email": SERVICE_ACCOUNT if has_key else None,
                "project_id": "acme-analytics" if has_key else None,
            },
        },
        headers=[],
    )


CREATE_ARGUMENTS = {
    "name": "BigQuery (acme-analytics)",
    "url": BIGQUERY_URL,
    "service_identity": {"type": "google_service_account"},
}


async def test_create_mcp_connection_with_an_identity_sends_its_type_and_opens_setup(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post(CONNECTIONS_PATH).respond(
        201, json=identity_response("authorization_needed", has_key=False)
    )

    result = await mcp_client.call_tool("create_mcp_connection", CREATE_ARGUMENTS)

    assert request_body(route.calls.last.request) == {
        "name": "BigQuery (acme-analytics)",
        "url": BIGQUERY_URL,
        "oauth": None,
        "headers": [],
        "service_identity": {"type": "google_service_account"},
    }
    assert result.structured_content == {
        "setup": "client_cannot_open_links",
        "connection": identity_result("authorization_needed", has_key=False),
    }
    assert result.structured_content["connection"]["setup_url"] == SETUP_URL


async def test_create_mcp_connection_sends_the_scopes_the_agent_names(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post(CONNECTIONS_PATH).respond(
        201, json=identity_response("authorization_needed", has_key=False)
    )

    await mcp_client.call_tool(
        "create_mcp_connection",
        {
            **CREATE_ARGUMENTS,
            "service_identity": {
                "type": "google_service_account",
                "scopes": ["https://www.googleapis.com/auth/bigquery"],
            },
        },
    )

    assert request_body(route.calls.last.request)["service_identity"] == {
        "type": "google_service_account",
        "scopes": ["https://www.googleapis.com/auth/bigquery"],
    }


async def test_create_mcp_connection_leaves_the_field_out_without_an_identity(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post(CONNECTIONS_PATH).respond(201, json=connection_response())

    await mcp_client.call_tool(
        "create_mcp_connection",
        {"name": "Linear (eddie)", "url": "https://mcp.linear.app/mcp", "oauth": {}},
    )

    assert "service_identity" not in request_body(route.calls.last.request)


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        ({"oauth": {}}, "not both"),
        (
            {"headers": [{"name": "Authorization", "value_prefix": "Bearer "}]},
            "Authorization",
        ),
    ],
    ids=["oauth", "authorization header"],
)
async def test_create_mcp_connection_refuses_a_second_credential_beside_the_identity(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    extra: dict[str, Any],
    message: str,
):
    route = cloud_api.post(CONNECTIONS_PATH).respond(201, json=connection_response())

    result = await mcp_client.call_tool(
        "create_mcp_connection", {**CREATE_ARGUMENTS, **extra}, raise_on_error=False
    )

    assert message in error_text(result)
    assert not route.called


async def test_create_mcp_connection_rejects_a_key_in_the_identity(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post(CONNECTIONS_PATH).respond(201, json=connection_response())

    result = await mcp_client.call_tool(
        "create_mcp_connection",
        {
            **CREATE_ARGUMENTS,
            "service_identity": {
                "type": "google_service_account",
                "key": '{"type": "service_account"}',
            },
        },
        raise_on_error=False,
    )

    assert result.is_error
    assert not route.called


async def test_get_mcp_connection_names_the_account_a_service_identity_runs_as(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(CONNECTION_PATH).respond(
        200, json=identity_response("authorized", has_key=True)
    )

    result = await mcp_client.call_tool(
        "get_mcp_connection", {"connection_id": CONNECTION_PATH.rsplit("/", 1)[1]}
    )

    assert result.structured_content == identity_result("authorized", has_key=True)
    assert "key_saved_at" not in str(result.structured_content)
