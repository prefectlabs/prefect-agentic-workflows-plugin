"""Tests for the model provider tools, which reshape what Cloud returns."""

from typing import Any

import respx
from fastmcp import Client
from support import error_text, request_body

PREFECT_CONNECTION_ID = "dddddddd-0000-0000-0000-000000000001"
VERTEX_CONNECTION_ID = "dddddddd-0000-0000-0000-000000000002"


def connection(
    connection_id: str, name: str, provider: dict[str, Any], **overrides: Any
) -> dict[str, Any]:
    """Return a connection in the shape `/agentic/provider-connections` returns."""
    return {
        "id": connection_id,
        "name": name,
        "provider": provider,
        "created": "2026-10-19T12:00:00Z",
        "updated": "2026-10-19T12:00:00Z",
        **overrides,
    }


async def test_list_provider_connections_returns_only_ids_names_types_and_models(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get("/agentic/provider-connections").respond(
        200,
        json=[
            connection(
                PREFECT_CONNECTION_ID,
                "prefect",
                {"type": "prefect"},
                models=[{"id": "claude-haiku-5-5"}],
            ),
            connection(
                VERTEX_CONNECTION_ID,
                "team-vertex",
                {"type": "vertex", "project_id": "acme", "region": "us-east5"},
                models=None,
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
                "models": [{"id": "claude-haiku-5-5"}],
            },
            {
                "id": VERTEX_CONNECTION_ID,
                "name": "team-vertex",
                "provider": {"type": "vertex"},
                "models": None,
            },
        ]
    }


async def test_list_provider_connections_reads_a_connection_without_models_as_unlimited(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.get("/agentic/provider-connections").respond(
        200, json=[connection(PREFECT_CONNECTION_ID, "prefect", {"type": "prefect"})]
    )

    result = await mcp_client.call_tool("list_provider_connections", {})

    assert result.structured_content is not None
    assert result.structured_content["provider_connections"][0]["models"] is None


async def test_list_providers_returns_each_providers_model_ids(
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
            {"type": "anthropic", "models": [{"id": "claude-opus-5-5"}]},
            {"type": "prefect", "models": [{"id": "claude-haiku-5-5"}]},
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
        "models": None,
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
