"""Tests for the MCP connection tools, which never take or return a secret."""

from typing import Any

import httpx
import pytest
import respx
from fastmcp import Client
from support import (
    ACCOUNT_ID,
    FLOW_ID,
    PLAN,
    VERSIONS_PATH,
    WORKSPACE_ID,
    browser_user,
    error_text,
    request_body,
    version_response,
    version_summary,
)

from prefect_agentic_workflows_mcp.server import build_server

CONNECTIONS_PATH = "/agentic/mcp-connections"
CONNECTION_ID = "0199c3a2-0000-7000-8000-000000000003"
CONNECTION_PATH = f"{CONNECTIONS_PATH}/{CONNECTION_ID}"
CREATOR_ID = "eeeeeeee-0000-0000-0000-000000000001"
SETUP_URL = (
    f"https://app.prefect.cloud/account/{ACCOUNT_ID}/workspace/{WORKSPACE_ID}"
    f"/agentic-workflows/connections/{CONNECTION_ID}/setup"
)
LINEAR_URL = "https://mcp.linear.app/mcp"

# Field names that would carry a secret into or out of a tool call.
SECRET_FIELDS = {
    "value",
    "secret",
    "client_secret",
    "token",
    "access_token",
    "refresh_token",
    "id_token",
    "password",
    "api_key",
    "ciphertext",
}


def connection_response(
    state: str = "authorization_needed", **overrides: Any
) -> dict[str, Any]:
    """Return an OAuth connection in the shape Cloud's MCP connection routes return.

    It includes fields the tools leave out, such as the OAuth client and the
    header ids, so the tests show they never reach the agent.
    """
    return {
        "id": CONNECTION_ID,
        "name": "Linear (eddie)",
        "url": LINEAR_URL,
        "auth_method": "oauth",
        "oauth": {
            "bearer_token_source": "access_token",
            "scopes": ["read"],
            "issuer": "https://mcp.linear.app",
            "supports_dcr": True,
            "client": {
                "source": "dynamic",
                "client_id": "abc123",
                "has_client_secret": False,
                "token_endpoint_auth_method": "none",
            },
            "subject": None,
            "authorized_at": None,
        },
        "headers": [
            {
                "id": "0199c3a2-0000-7000-8000-0000000000a1",
                "name": "X-Tenant",
                "value_prefix": "",
                "has_value": False,
                "value_saved_at": None,
            }
        ],
        "status": {
            "state": state,
            "reason": None,
            "since": None,
            "access_expires_at": None,
            "retry_after": None,
        },
        "last_check": None,
        "setup_url": SETUP_URL,
        "created": "2026-10-27T12:00:00Z",
        "updated": "2026-10-27T12:00:00Z",
        "created_by_actor_id": CREATOR_ID,
        **overrides,
    }


def connection_result(state: str = "authorization_needed", **overrides: Any):
    """Return a connection as the tools return it."""
    return {
        "id": CONNECTION_ID,
        "name": "Linear (eddie)",
        "url": LINEAR_URL,
        "auth_method": "oauth",
        "status": {"state": state, "reason": None},
        "headers": [{"name": "X-Tenant", "value_prefix": "", "has_value": False}],
        "oauth": {
            "bearer_token_source": "access_token",
            "scopes": ["read"],
            "issuer": "https://mcp.linear.app",
            "supports_dcr": True,
        },
        "last_check": None,
        "setup_url": SETUP_URL,
        "created_by_actor_id": CREATOR_ID,
        **overrides,
    }


CREATE_ARGUMENTS = {
    "name": "Linear (eddie)",
    "url": LINEAR_URL,
    "oauth": {"scopes": ["read"]},
    "headers": [{"name": "X-Tenant"}],
}


async def test_discover_mcp_server_returns_how_the_server_signs_in(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post(f"{CONNECTIONS_PATH}/discover").respond(
        200,
        json={
            "requires_auth": True,
            "oauth": {
                "issuer": "https://github.com/login/oauth",
                "supports_dcr": False,
                "scopes_supported": ["repo"],
                "supports_id_token": False,
            },
        },
    )

    result = await mcp_client.call_tool(
        "discover_mcp_server", {"url": "https://api.githubcopilot.com/mcp/"}
    )

    assert request_body(route.calls.last.request) == {
        "url": "https://api.githubcopilot.com/mcp/"
    }
    assert result.structured_content == {
        "requires_auth": True,
        "oauth": {
            "issuer": "https://github.com/login/oauth",
            "supports_dcr": False,
            "scopes_supported": ["repo"],
            "supports_id_token": False,
        },
    }


async def test_list_mcp_connections_returns_connections_without_client_details(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(CONNECTIONS_PATH).respond(200, json=[connection_response()])

    result = await mcp_client.call_tool("list_mcp_connections", {})

    assert result.structured_content == {"mcp_connections": [connection_result()]}


async def test_get_mcp_connection_returns_its_status_check_and_setup_link(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    check = {
        "outcome": "succeeded",
        "checked_at": "2026-10-27T12:05:00Z",
        "tool_count": 12,
        "http_status": 200,
        "credential_revision": 2,
    }
    cloud_api.get(CONNECTION_PATH).respond(
        200, json=connection_response("authorized", last_check=check)
    )

    result = await mcp_client.call_tool(
        "get_mcp_connection", {"connection_id": CONNECTION_ID}
    )

    assert result.structured_content == connection_result(
        "authorized",
        last_check={
            "outcome": "succeeded",
            "checked_at": "2026-10-27T12:05:00Z",
            "tool_count": 12,
        },
    )


async def test_create_mcp_connection_skips_setup_for_a_server_without_auth(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    public = connection_response(
        "not_verified", auth_method="none", oauth=None, headers=[]
    )
    route = cloud_api.post(CONNECTIONS_PATH).respond(201, json=public)

    result = await mcp_client.call_tool(
        "create_mcp_connection", {"name": "Linear (eddie)", "url": LINEAR_URL}
    )

    assert request_body(route.calls.last.request) == {
        "name": "Linear (eddie)",
        "url": LINEAR_URL,
        "oauth": None,
        "headers": [],
    }
    assert result.structured_content == {
        "setup": "not_needed",
        "connection": connection_result(
            "not_verified", auth_method="none", oauth=None, headers=[]
        ),
    }


async def test_create_mcp_connection_returns_the_link_to_a_client_that_cant_open_it(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post(CONNECTIONS_PATH).respond(201, json=connection_response())

    result = await mcp_client.call_tool("create_mcp_connection", CREATE_ARGUMENTS)

    assert request_body(route.calls.last.request) == {
        "name": "Linear (eddie)",
        "url": LINEAR_URL,
        "oauth": {"bearer_token_source": "access_token", "scopes": ["read"]},
        "headers": [{"name": "X-Tenant", "value_prefix": ""}],
    }
    assert result.structured_content == {
        "setup": "client_cannot_open_links",
        "connection": connection_result(),
    }


@pytest.mark.parametrize("mode", ["auto", "legacy"])
async def test_create_mcp_connection_opens_setup_and_returns_the_connection_after(
    cloud_api: respx.MockRouter, mode: str
):
    create = cloud_api.post(CONNECTIONS_PATH).respond(201, json=connection_response())
    cloud_api.get(CONNECTION_PATH).respond(200, json=connection_response("authorized"))
    opened_urls: list[str] = []

    async with Client(
        build_server(),
        mode=mode,
        elicitation_handler=browser_user("accept", opened_urls),
    ) as client:
        result = await client.call_tool("create_mcp_connection", CREATE_ARGUMENTS)

    assert create.call_count == 1
    assert opened_urls == [SETUP_URL]
    assert result.structured_content == {
        "setup": "opened",
        "connection": connection_result("authorized"),
    }


@pytest.mark.parametrize("mode", ["auto", "legacy"])
async def test_create_mcp_connection_reports_a_declined_setup_page(
    cloud_api: respx.MockRouter, mode: str
):
    create = cloud_api.post(CONNECTIONS_PATH).respond(201, json=connection_response())
    cloud_api.get(CONNECTION_PATH).respond(200, json=connection_response())

    async with Client(
        build_server(), mode=mode, elicitation_handler=browser_user("decline", [])
    ) as client:
        result = await client.call_tool("create_mcp_connection", CREATE_ARGUMENTS)

    assert create.call_count == 1
    assert result.structured_content == {
        "setup": "declined",
        "connection": connection_result(),
    }


async def test_create_mcp_connection_rejects_a_header_value(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post(CONNECTIONS_PATH)

    result = await mcp_client.call_tool(
        "create_mcp_connection",
        {
            "name": "Billing",
            "url": "https://billing.example.com/mcp",
            "headers": [{"name": "X-API-Key", "value": "sk_live_123"}],
        },
        raise_on_error=False,
    )

    assert result.is_error
    assert not route.called


async def test_no_tool_input_has_a_field_for_a_secret(mcp_client: Client[Any]):
    def field_names(schema: Any) -> set[str]:
        if isinstance(schema, list):
            return set().union(*(field_names(item) for item in schema))
        if not isinstance(schema, dict):
            return set()
        names = set(schema.get("properties", {}))
        return names.union(*(field_names(value) for value in schema.values()))

    for tool in await mcp_client.list_tools():
        assert not field_names(tool.input_schema) & SECRET_FIELDS, tool.name


async def test_check_mcp_connection_returns_the_saved_check(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    check = {
        "outcome": "authentication_failed",
        "checked_at": "2026-10-27T12:05:00Z",
        "http_status": 401,
        "credential_revision": 1,
    }
    cloud_api.post(f"{CONNECTION_PATH}/check").respond(
        200,
        json=connection_response(
            "reauthorization_required",
            status={
                "state": "reauthorization_required",
                "reason": "credential_rejected",
            },
            last_check=check,
        ),
    )

    result = await mcp_client.call_tool(
        "check_mcp_connection", {"connection_id": CONNECTION_ID}
    )

    assert result.structured_content == connection_result(
        status={"state": "reauthorization_required", "reason": "credential_rejected"},
        last_check={
            "outcome": "authentication_failed",
            "checked_at": "2026-10-27T12:05:00Z",
            "tool_count": None,
        },
    )


async def test_check_mcp_connection_sends_the_user_to_unfinished_setup(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(f"{CONNECTION_PATH}/check").respond(
        409,
        json={
            "detail": {
                "code": "mcp_connection_unavailable",
                "state": "authorization_needed",
            }
        },
    )

    result = await mcp_client.call_tool(
        "check_mcp_connection", {"connection_id": CONNECTION_ID}, raise_on_error=False
    )

    assert error_text(result) == (
        "The connection can't be checked because its setup isn't finished "
        "(state: authorization_needed). Send the user to the connection's "
        "`setup_url` from `get_mcp_connection`, then check it again."
    )


TOOL_CALLS: list[tuple[str, dict[str, Any], str, str]] = [
    (
        "discover_mcp_server",
        {"url": LINEAR_URL},
        "POST",
        f"{CONNECTIONS_PATH}/discover",
    ),
    ("list_mcp_connections", {}, "GET", CONNECTIONS_PATH),
    ("get_mcp_connection", {"connection_id": CONNECTION_ID}, "GET", CONNECTION_PATH),
    ("create_mcp_connection", CREATE_ARGUMENTS, "POST", CONNECTIONS_PATH),
    (
        "check_mcp_connection",
        {"connection_id": CONNECTION_ID},
        "POST",
        f"{CONNECTION_PATH}/check",
    ),
]


@pytest.mark.parametrize(
    ("tool", "arguments", "method", "path"),
    TOOL_CALLS,
    ids=[call[0] for call in TOOL_CALLS],
)
async def test_every_tool_says_when_mcp_connections_are_not_enabled(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    tool: str,
    arguments: dict[str, Any],
    method: str,
    path: str,
):
    cloud_api.route(method=method, path__regex=f"{path}$").respond(
        404, json={"detail": "Not found"}
    )

    result = await mcp_client.call_tool(tool, arguments, raise_on_error=False)

    assert "MCP connections are not enabled for this account" in error_text(result)


async def test_a_missing_connection_is_reported_as_missing(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get(CONNECTION_PATH).respond(
        404, json={"detail": "MCP connection not found."}
    )

    result = await mcp_client.call_tool(
        "get_mcp_connection", {"connection_id": CONNECTION_ID}, raise_on_error=False
    )

    message = error_text(result)
    assert "HTTP 404" in message
    assert "MCP connection not found." in message
    assert "not enabled" not in message


async def test_create_mcp_connection_explains_a_workspace_without_a_bucket(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(CONNECTIONS_PATH).respond(
        409,
        json={"detail": "Workspace object storage bucket has not been provisioned."},
    )

    result = await mcp_client.call_tool(
        "create_mcp_connection", CREATE_ARGUMENTS, raise_on_error=False
    )

    assert "no object storage bucket" in error_text(result)


async def test_publish_plan_names_a_connection_the_user_cannot_reference(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    denied = {
        "code": "mcp_connection_access_denied",
        "phase": "semantic",
        "path": ["nodes", "triage", "mcp", "mcpServers", "issues", "connection_id"],
        "message": (
            f"You do not have access to reference MCP connection {CONNECTION_ID}. "
            "Ask a workspace owner for access."
        ),
    }
    cloud_api.post("/execution-plans/validate").respond(
        200, json={"valid": False, "errors": [denied]}
    )
    versions = cloud_api.post(VERSIONS_PATH)

    result = await mcp_client.call_tool(
        "publish_plan", {"flow_id": FLOW_ID, "plan": PLAN}
    )

    assert result.structured_content is not None
    assert result.structured_content["published"] is False
    assert result.structured_content["errors"] == [denied]
    assert not versions.called


async def test_publish_plan_returns_the_warning_for_a_connection_not_set_up(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    warning = {
        "code": "mcp_connection_not_authorized",
        "node_id": "triage",
        "server_alias": "issues",
        "connection_id": CONNECTION_ID,
        "state": "authorization_needed",
        "setup_url": SETUP_URL,
    }
    cloud_api.post("/execution-plans/validate").respond(
        200, json={"valid": True, "errors": []}
    )
    cloud_api.post(VERSIONS_PATH).respond(
        201, json={**version_response(), "warnings": [warning]}
    )

    result = await mcp_client.call_tool(
        "publish_plan", {"flow_id": FLOW_ID, "plan": PLAN, "activate": False}
    )

    assert result.structured_content is not None
    assert result.structured_content["version"] == {
        **version_summary(),
        "warnings": [warning],
    }


async def test_check_mcp_connection_reports_access_denied_with_its_message(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post(f"{CONNECTION_PATH}/check").mock(
        return_value=httpx.Response(
            403,
            json={
                "detail": {
                    "code": "mcp_connection_access_denied",
                    "connection_id": CONNECTION_ID,
                    "message": (
                        "You do not have access to this MCP connection. Ask a "
                        "workspace owner for access."
                    ),
                }
            },
        )
    )

    result = await mcp_client.call_tool(
        "check_mcp_connection", {"connection_id": CONNECTION_ID}, raise_on_error=False
    )

    message = error_text(result)
    assert "mcp_connection_access_denied" in message
    assert "Ask a workspace owner for access." in message
