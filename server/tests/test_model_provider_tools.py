"""Tests for the model provider tools, which reshape what Cloud returns."""

from typing import Any, Literal

import httpx
import pytest
import respx
from fastmcp import Client
from fastmcp.client.elicitation import ElicitResult
from support import ACCOUNT_ID, WORKSPACE_ID, error_text, request_body

from prefect_agentic_workflows_mcp.server import build_server

APP_URL = f"https://app.prefect.cloud/account/{ACCOUNT_ID}/workspace/{WORKSPACE_ID}"
PREFECT_CONNECTION_ID = "dddddddd-0000-0000-0000-000000000001"
VERTEX_CONNECTION_ID = "dddddddd-0000-0000-0000-000000000002"
ANTHROPIC_CONNECTION_ID = "dddddddd-0000-0000-0000-000000000003"
ADD_ANTHROPIC_URL = f"{APP_URL}/agentic-model-providers?add=anthropic"


def connection(
    connection_id: str, name: str, provider: dict[str, Any]
) -> dict[str, Any]:
    """Return a connection in the shape `/agentic/provider-connections` returns."""
    return {
        "id": connection_id,
        "name": name,
        "provider": provider,
        "created": "2026-10-19T12:00:00Z",
        "updated": "2026-10-19T12:00:00Z",
    }


async def test_list_provider_connections_returns_only_ids_names_and_types(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get("/agentic/provider-connections").respond(
        200,
        json=[
            connection(PREFECT_CONNECTION_ID, "prefect", {"type": "prefect"}),
            connection(
                VERTEX_CONNECTION_ID,
                "team-vertex",
                {"type": "vertex", "project_id": "acme", "region": "us-east5"},
            ),
        ],
    )

    result = await mcp_client.call_tool("list_provider_connections", {})

    assert result.structured_content == {
        "provider_connections": [
            {
                "id": PREFECT_CONNECTION_ID,
                "name": "prefect",
                "provider": {"type": "prefect"},
            },
            {
                "id": VERTEX_CONNECTION_ID,
                "name": "team-vertex",
                "provider": {"type": "vertex"},
            },
        ]
    }


async def test_list_providers_returns_each_providers_models_and_add_link(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get("/agentic/providers").respond(
        200,
        json=[
            {"type": "anthropic", "models": [{"id": "claude-opus-5-5"}]},
            {"type": "prefect", "models": [{"id": "claude-haiku-5-5"}]},
        ],
    )

    result = await mcp_client.call_tool("list_providers", {})

    assert result.structured_content == {
        "providers": [
            {
                "type": "anthropic",
                "models": [{"id": "claude-opus-5-5"}],
                "add_url": f"{APP_URL}/agentic-model-providers?add=anthropic",
            },
            {
                "type": "prefect",
                "models": [{"id": "claude-haiku-5-5"}],
                "add_url": f"{APP_URL}/agentic-model-providers?add=prefect",
            },
        ]
    }


async def test_create_prefect_connection_sends_a_prefect_provider(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post("/agentic/provider-connections").respond(
        201, json=connection(PREFECT_CONNECTION_ID, "prefect", {"type": "prefect"})
    )

    result = await mcp_client.call_tool("create_prefect_connection", {})

    assert request_body(route.calls.last.request) == {
        "name": "prefect",
        "provider": {"type": "prefect"},
    }
    assert result.structured_content == {
        "id": PREFECT_CONNECTION_ID,
        "name": "prefect",
        "provider": {"type": "prefect"},
    }


async def test_create_prefect_connection_relays_a_taken_name(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post("/agentic/provider-connections").respond(
        409, json={"detail": "The workspace already has a connection named 'prefect'."}
    )

    result = await mcp_client.call_tool(
        "create_prefect_connection", {}, raise_on_error=False
    )

    assert "already has a connection named 'prefect'" in error_text(result)


def user_adds_anthropic_connection(cloud_api: respx.MockRouter) -> None:
    """Mock a workspace that gains an Anthropic connection between two reads."""
    existing = connection(PREFECT_CONNECTION_ID, "prefect", {"type": "prefect"})
    added = connection(ANTHROPIC_CONNECTION_ID, "my-anthropic", {"type": "anthropic"})
    cloud_api.get("/agentic/providers").respond(
        200, json=[{"type": "anthropic", "models": [{"id": "claude-haiku-5-5"}]}]
    )
    cloud_api.get("/agentic/provider-connections").mock(
        side_effect=[
            httpx.Response(200, json=[existing]),
            httpx.Response(200, json=[existing, added]),
        ]
    )


def browser_user(
    action: Literal["accept", "decline", "cancel"], opened_urls: list[str]
) -> Any:
    """Return an elicitation handler that records each URL and answers `action`."""

    async def handle(message: str, response_type: Any, params: Any, context: Any):
        opened_urls.append(params.url)
        return ElicitResult(action=action)

    return handle


@pytest.mark.parametrize("mode", ["auto", "legacy"])
async def test_connect_model_provider_returns_the_connection_the_user_added(
    cloud_api: respx.MockRouter, mode: str
):
    user_adds_anthropic_connection(cloud_api)
    opened_urls: list[str] = []

    async with Client(
        build_server(),
        mode=mode,
        elicitation_handler=browser_user("accept", opened_urls),
    ) as client:
        result = await client.call_tool(
            "connect_model_provider", {"provider": "anthropic"}
        )

    assert opened_urls == [ADD_ANTHROPIC_URL]
    assert result.structured_content == {
        "status": "connected",
        "connections": [
            {
                "id": ANTHROPIC_CONNECTION_ID,
                "name": "my-anthropic",
                "provider": {"type": "anthropic"},
            }
        ],
        "add_url": ADD_ANTHROPIC_URL,
        "known_connection_ids": [PREFECT_CONNECTION_ID],
    }


async def test_connect_model_provider_reports_a_declined_link(
    cloud_api: respx.MockRouter,
):
    user_adds_anthropic_connection(cloud_api)

    async with Client(
        build_server(), elicitation_handler=browser_user("decline", [])
    ) as client:
        result = await client.call_tool(
            "connect_model_provider", {"provider": "anthropic"}
        )

    assert result.structured_content == {
        "status": "declined",
        "connections": [],
        "add_url": ADD_ANTHROPIC_URL,
        "known_connection_ids": [PREFECT_CONNECTION_ID],
    }


async def test_connect_model_provider_reports_when_no_connection_was_added(
    cloud_api: respx.MockRouter,
):
    cloud_api.get("/agentic/providers").respond(
        200, json=[{"type": "anthropic", "models": [{"id": "claude-haiku-5-5"}]}]
    )
    cloud_api.get("/agentic/provider-connections").respond(200, json=[])

    async with Client(
        build_server(), elicitation_handler=browser_user("accept", [])
    ) as client:
        result = await client.call_tool(
            "connect_model_provider", {"provider": "anthropic"}
        )

    assert result.structured_content == {
        "status": "no_new_connection",
        "connections": [],
        "add_url": ADD_ANTHROPIC_URL,
        "known_connection_ids": [],
    }


async def test_connect_model_provider_returns_the_link_to_a_client_that_cant_open_it(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    user_adds_anthropic_connection(cloud_api)

    result = await mcp_client.call_tool(
        "connect_model_provider", {"provider": "anthropic"}
    )

    assert result.structured_content == {
        "status": "client_cannot_open_links",
        "connections": [],
        "add_url": ADD_ANTHROPIC_URL,
        "known_connection_ids": [PREFECT_CONNECTION_ID],
    }


async def test_connect_model_provider_rejects_an_unknown_provider(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get("/agentic/providers").respond(
        200, json=[{"type": "anthropic", "models": [{"id": "claude-haiku-5-5"}]}]
    )

    result = await mcp_client.call_tool(
        "connect_model_provider", {"provider": "claude"}, raise_on_error=False
    )

    assert error_text(result) == (
        "Prefect Cloud has no model provider `claude`. Use one of anthropic."
    )
