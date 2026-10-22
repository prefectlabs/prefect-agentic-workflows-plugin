"""OAuth authentication and workspace isolation over the HTTP transport."""

import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import Any

import httpx
import httpx2
import pytest
import respx
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.server.auth.providers.jwt import JWTVerifier, RSAKeyPair
from fastmcp.utilities.tests import run_server_async
from joserfc.jwk import RSAKey
from support import (
    ACCOUNT_ID,
    ACTIVATION_ID,
    FLOW_ID,
    FLOW_RUN_ID,
    PLAN,
    SCHEDULE_ID,
    VERSION_ID,
    WORKSPACE_ID,
    error_text,
    schema_response,
)

from prefect_agentic_workflows_mcp import cloud_oauth
from prefect_agentic_workflows_mcp.cloud_oauth import CloudOAuthSettings
from prefect_agentic_workflows_mcp.server import build_server

CLOUD_ORIGIN = "https://api.prefect.dev"
MCP_ORIGIN = "https://agentic.example.com"
RESOURCE_URL = f"{MCP_ORIGIN}/mcp"
GRANT_URL = f"{CLOUD_ORIGIN}/auth/mcp/oauth/grant/workspaces"
WORKSPACE_URL = f"{CLOUD_ORIGIN}/api/accounts/{ACCOUNT_ID}/workspaces/{WORKSPACE_ID}"
OTHER_WORKSPACE_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
OTHER_WORKSPACE_URL = (
    f"{CLOUD_ORIGIN}/api/accounts/{ACCOUNT_ID}/workspaces/{OTHER_WORKSPACE_ID}"
)
WORKSPACE = {
    "account_id": str(ACCOUNT_ID),
    "workspace_id": str(WORKSPACE_ID),
    "account_handle": "test-account",
    "workspace_handle": "test-workspace",
}
OTHER_WORKSPACE = {
    **WORKSPACE,
    "workspace_id": OTHER_WORKSPACE_ID,
    "workspace_handle": "other-workspace",
}


@pytest.fixture(scope="module")
def signing_key() -> RSAKeyPair:
    return RSAKeyPair.generate()


def access_token(key: RSAKeyPair, **overrides: Any) -> str:
    arguments: dict[str, Any] = {
        "issuer": CLOUD_ORIGIN,
        "audience": RESOURCE_URL,
        "scopes": ["prefect-cloud:workspaces"],
        "kid": "cloud-key",
        **overrides,
    }
    return key.create_token(**arguments)


@pytest.fixture
def cloud_api() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False) as router:
        router.route(host="127.0.0.1").pass_through()
        router.get(GRANT_URL, name="grant").respond(200, json=[WORKSPACE])
        for url in [WORKSPACE_URL, OTHER_WORKSPACE_URL]:
            router.get(f"{url}/execution-plans/schema").respond(
                200, json=schema_response()
            )
            router.post(f"{url}/deployments/filter").respond(200, json=[])
            router.post(f"{url}/flows/filter").respond(200, json=[])
        yield router


@pytest.fixture
async def server_url(
    monkeypatch: pytest.MonkeyPatch,
    signing_key: RSAKeyPair,
    cloud_api: respx.MockRouter,
) -> AsyncIterator[str]:
    monkeypatch.setenv("PREFECT_MCP_CLOUD_ENABLED", "true")
    monkeypatch.setenv("PREFECT_MCP_CLOUD_API_BASE_URL", CLOUD_ORIGIN)
    monkeypatch.setenv("PREFECT_MCP_CLOUD_PUBLIC_BASE_URL", MCP_ORIGIN)
    public_key = RSAKey.import_key(signing_key.public_key).as_dict(private=False)
    public_key["kid"] = "cloud-key"

    def jwks(request: httpx2.Request) -> httpx2.Response:
        assert str(request.url) == f"{CLOUD_ORIGIN}/auth/mcp/oauth/jwks.json"
        return httpx2.Response(200, json={"keys": [public_key]})

    async with httpx2.AsyncClient(transport=httpx2.MockTransport(jwks)) as jwks_client:

        def verifier(**kwargs: Any) -> JWTVerifier:
            return JWTVerifier(**kwargs, http_client=jwks_client)

        monkeypatch.setattr(cloud_oauth, "JWTVerifier", verifier)
        async with run_server_async(build_server()) as url:
            yield url


def connect(url: str, token: str, **headers: str) -> Client[Any]:
    return Client(
        StreamableHttpTransport(
            url, headers={"Authorization": f"Bearer {token}", **headers}
        )
    )


async def test_oauth_uses_the_granted_workspace_and_token(
    server_url: str, signing_key: RSAKeyPair, cloud_api: respx.MockRouter
):
    token = access_token(signing_key)
    async with connect(
        server_url,
        token,
        **{
            "X-Prefect-Api-Url": OTHER_WORKSPACE_URL,
            "X-Prefect-Api-Key": "pnu_other_key",
        },
    ) as client:
        workspace = await client.call_tool("get_workspace", {})
        await client.call_tool("list_deployments", {})
    assert workspace.structured_content == {
        "api_url": WORKSPACE_URL,
        "app_url": (
            f"https://app.prefect.dev/account/{ACCOUNT_ID}/workspace/{WORKSPACE_ID}"
        ),
    }
    requests = [
        call.request for call in cloud_api.calls if call.request.url.host != "127.0.0.1"
    ]
    assert requests
    assert all(r.headers["Authorization"] == f"Bearer {token}" for r in requests)
    assert all(r.url.host == "api.prefect.dev" for r in requests)


async def test_get_workspace_resolves_its_grant_once(
    server_url: str, signing_key: RSAKeyPair, cloud_api: respx.MockRouter
):
    grant = cloud_api.routes["grant"].mock(
        side_effect=[
            httpx.Response(200, json=[WORKSPACE]),
            httpx.Response(200, json=[OTHER_WORKSPACE]),
        ]
    )
    async with connect(server_url, access_token(signing_key)) as client:
        result = await client.call_tool("get_workspace", {})

    assert grant.call_count == 1
    assert result.structured_content == {
        "api_url": WORKSPACE_URL,
        "app_url": (
            f"https://app.prefect.dev/account/{ACCOUNT_ID}/workspace/{WORKSPACE_ID}"
        ),
    }


@pytest.mark.parametrize(
    "overrides",
    [
        {"issuer": "https://untrusted.example.com"},
        {"audience": "https://other-server.example.com/mcp"},
        {"expires_in_seconds": -120},
        {"scopes": ["unrelated-scope"]},
    ],
    ids=["issuer", "audience", "expired", "scope"],
)
async def test_invalid_tokens_never_use_the_profile(
    server_url: str,
    signing_key: RSAKeyPair,
    cloud_api: respx.MockRouter,
    overrides: dict[str, Any],
):
    async with httpx.AsyncClient() as client:
        response = await client.post(
            server_url,
            headers={
                "Authorization": f"Bearer {access_token(signing_key, **overrides)}"
            },
        )
    assert response.status_code in {401, 403}
    assert not cloud_api.routes["grant"].called


@pytest.mark.parametrize("token", [None, "not-a-jwt"])
async def test_missing_or_malformed_tokens_cannot_use_paired_headers(
    server_url: str, cloud_api: respx.MockRouter, token: str | None
):
    headers = {"X-Prefect-Api-Url": WORKSPACE_URL, "X-Prefect-Api-Key": "pnu_key"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    async with httpx.AsyncClient() as client:
        response = await client.post(server_url, headers=headers)
    assert response.status_code == 401
    assert not cloud_api.routes["grant"].called


async def test_a_token_from_another_signing_key_is_rejected(server_url: str):
    async with httpx.AsyncClient() as client:
        response = await client.post(
            server_url,
            headers={"Authorization": f"Bearer {access_token(RSAKeyPair.generate())}"},
        )
    assert response.status_code == 401


async def test_multiple_workspaces_require_an_explicit_choice(
    server_url: str, signing_key: RSAKeyPair, cloud_api: respx.MockRouter
):
    cloud_api.routes["grant"].respond(200, json=[WORKSPACE, OTHER_WORKSPACE])
    async with connect(server_url, access_token(signing_key)) as client:
        result = await client.call_tool("get_workspace", {}, raise_on_error=False)
        assert "workspace_id" in error_text(result)
        assert "test-account/test-workspace" in error_text(result)
        assert OTHER_WORKSPACE_ID in error_text(result)
        workspace = await client.call_tool(
            "get_workspace", {"workspace_id": OTHER_WORKSPACE_ID}
        )
        await client.call_tool("list_deployments", {"workspace_id": OTHER_WORKSPACE_ID})
    assert workspace.structured_content is not None
    assert workspace.structured_content["api_url"] == OTHER_WORKSPACE_URL
    workspace_calls = [
        c for c in cloud_api.calls if "/api/accounts/" in c.request.url.path
    ]
    assert workspace_calls
    assert all(OTHER_WORKSPACE_ID in str(c.request.url) for c in workspace_calls)


async def test_a_workspace_outside_the_grant_is_rejected(
    server_url: str, signing_key: RSAKeyPair, cloud_api: respx.MockRouter
):
    async with connect(server_url, access_token(signing_key)) as client:
        result = await client.call_tool(
            "list_deployments",
            {"workspace_id": OTHER_WORKSPACE_ID},
            raise_on_error=False,
        )
    assert "not included" in error_text(result)
    assert not any("/api/accounts/" in c.request.url.path for c in cloud_api.calls)


@pytest.mark.parametrize("status", [401, 403, 503])
async def test_grant_errors_do_not_use_profile_credentials(
    server_url: str, signing_key: RSAKeyPair, cloud_api: respx.MockRouter, status: int
):
    token = access_token(signing_key)
    cloud_api.routes["grant"].respond(status, json={"detail": "Grant unavailable"})
    async with connect(server_url, token) as client:
        result = await client.call_tool("list_deployments", {}, raise_on_error=False)
    assert result.is_error
    assert token not in error_text(result)
    assert "prefect cloud login" not in error_text(result)
    assert not any("/api/accounts/" in c.request.url.path for c in cloud_api.calls)


async def test_empty_grants_require_reauthorization(
    server_url: str, signing_key: RSAKeyPair, cloud_api: respx.MockRouter
):
    cloud_api.routes["grant"].respond(200, json=[])
    async with connect(server_url, access_token(signing_key)) as client:
        result = await client.call_tool("get_workspace", {}, raise_on_error=False)
    assert "Reconnect" in error_text(result)


async def test_concurrent_users_do_not_share_workspace_credentials(
    server_url: str, signing_key: RSAKeyPair, cloud_api: respx.MockRouter
):
    first_token = access_token(signing_key, subject="first-user")
    second_token = access_token(signing_key, subject="second-user")

    def grant(request: httpx.Request) -> httpx.Response:
        workspace = (
            WORKSPACE
            if request.headers["Authorization"] == f"Bearer {first_token}"
            else OTHER_WORKSPACE
        )
        return httpx.Response(200, json=[workspace])

    cloud_api.routes["grant"].mock(side_effect=grant)
    async with (
        connect(server_url, first_token) as first,
        connect(server_url, second_token) as second,
    ):
        first_result, second_result = await asyncio.gather(
            first.call_tool("get_workspace", {}), second.call_tool("get_workspace", {})
        )
        await asyncio.gather(
            first.call_tool("list_deployments", {}),
            second.call_tool("list_deployments", {}),
        )
    assert first_result.structured_content is not None
    assert second_result.structured_content is not None
    assert first_result.structured_content["api_url"] == WORKSPACE_URL
    assert second_result.structured_content["api_url"] == OTHER_WORKSPACE_URL
    for call in cloud_api.calls:
        if "/api/accounts/" in call.request.url.path:
            expected = (
                second_token
                if OTHER_WORKSPACE_ID in str(call.request.url)
                else first_token
            )
            assert call.request.headers["Authorization"] == f"Bearer {expected}"


@pytest.mark.parametrize(
    "origin", ["https://api.prefect.cloud", "https://api.stg.prefect.dev", CLOUD_ORIGIN]
)
def test_oauth_settings_derive_endpoints_for_each_environment(origin: str):
    settings = CloudOAuthSettings(
        enabled=True, api_base_url=origin, public_base_url=MCP_ORIGIN
    )
    provider = cloud_oauth.build_auth_provider(settings)
    assert provider is not None
    assert str(provider.base_url).rstrip("/") == MCP_ORIGIN


def test_oauth_requires_a_public_server_url():
    with pytest.raises(ValueError, match="public_base_url"):
        CloudOAuthSettings(enabled=True)


TOOL_ARGUMENTS: dict[str, dict[str, Any]] = {
    "get_workspace": {},
    "get_schema": {},
    "validate_plan": {"plan": PLAN},
    "get_flow": {"name": "test-flow"},
    "get_or_create_flow": {"name": "test-flow"},
    "list_deployments": {},
    "get_deployment": {"deployment_id": FLOW_ID},
    "publish_plan": {"flow_id": FLOW_ID, "plan": PLAN},
    "get_plan": {"flow_id": FLOW_ID},
    "list_plan_versions": {"flow_id": FLOW_ID},
    "activate_plan_version": {"flow_id": FLOW_ID, "version_id": VERSION_ID},
    "start_run": {"flow_id": FLOW_ID},
    "get_run": {"flow_run_id": FLOW_RUN_ID},
    "get_run_output": {"flow_run_id": FLOW_RUN_ID, "output_name": "summary"},
    "submit_human_input": {
        "flow_run_id": FLOW_RUN_ID,
        "activation_id": ACTIVATION_ID,
        "response": {"approved": True},
    },
    "create_schedule": {
        "flow_id": FLOW_ID,
        "name": "daily",
        "schedule": {"type": "cron", "cron": "0 0 * * *", "timezone": "UTC"},
    },
    "list_schedules": {"flow_id": FLOW_ID},
    "get_schedule": {"flow_id": FLOW_ID, "schedule_id": SCHEDULE_ID},
    "update_schedule": {
        "flow_id": FLOW_ID,
        "schedule_id": SCHEDULE_ID,
        "active": False,
    },
    "delete_schedule": {"flow_id": FLOW_ID, "schedule_id": SCHEDULE_ID},
    "list_secret_blocks": {},
    "list_providers": {},
    "list_provider_connections": {},
    "connect_model_provider": {"provider": "anthropic"},
}


@pytest.mark.parametrize("tool", TOOL_ARGUMENTS)
async def test_every_tool_rejects_workspaces_outside_the_grant(
    server_url: str, signing_key: RSAKeyPair, cloud_api: respx.MockRouter, tool: str
):
    async with connect(server_url, access_token(signing_key)) as client:
        result = await client.call_tool(
            tool,
            {**TOOL_ARGUMENTS[tool], "workspace_id": OTHER_WORKSPACE_ID},
            raise_on_error=False,
        )
    assert "not included" in error_text(result)
    assert not any("/api/accounts/" in c.request.url.path for c in cloud_api.calls)


async def test_workspace_isolation_covers_every_registered_tool(
    server_url: str, signing_key: RSAKeyPair
):
    async with connect(server_url, access_token(signing_key)) as client:
        assert {t.name for t in await client.list_tools()} == set(TOOL_ARGUMENTS)


async def test_revoked_grants_are_not_cached_between_tool_calls(
    server_url: str, signing_key: RSAKeyPair, cloud_api: respx.MockRouter
):
    async with connect(server_url, access_token(signing_key)) as client:
        await client.call_tool("get_workspace", {})
        cloud_api.routes["grant"].respond(401)
        result = await client.call_tool("get_workspace", {}, raise_on_error=False)
    assert result.is_error
    assert "Reconnect" in error_text(result)


@pytest.mark.parametrize("body", [{}, [{"workspace_id": "not-a-uuid"}]])
async def test_malformed_grants_cannot_produce_workspace_requests(
    server_url: str, signing_key: RSAKeyPair, cloud_api: respx.MockRouter, body: Any
):
    cloud_api.routes["grant"].respond(200, json=body)
    async with connect(server_url, access_token(signing_key)) as client:
        result = await client.call_tool("list_deployments", {}, raise_on_error=False)
    assert "invalid workspace grant" in error_text(result)
    assert not any("/api/accounts/" in c.request.url.path for c in cloud_api.calls)


async def test_protected_resource_metadata_matches_token_audience(server_url: str):
    url = httpx.URL(server_url).copy_with(
        path="/.well-known/oauth-protected-resource/mcp"
    )
    async with httpx.AsyncClient() as client:
        response = await client.get(str(url))
    assert response.status_code == 200
    metadata = response.json()
    assert metadata["resource"] == RESOURCE_URL
    assert metadata["authorization_servers"] == [CLOUD_ORIGIN]
    assert metadata["scopes_supported"] == ["prefect-cloud:workspaces"]


@pytest.mark.parametrize("suffix", ["", "/", "/mcp", "/mcp/"])
def test_public_urls_resolve_to_one_mcp_path(suffix: str):
    settings = CloudOAuthSettings(enabled=True, public_base_url=MCP_ORIGIN + suffix)
    assert settings.resource_url == RESOURCE_URL


@pytest.mark.parametrize(
    "field,value",
    [
        ("api_base_url", "http://api.prefect.dev"),
        ("auth_base_url", "https://api.prefect.dev/auth"),
        ("public_base_url", "https://user:password@example.com"),
        ("public_base_url", "https://example.com?token=secret"),
        ("api_base_url", "https://api.prefect.dev/api"),
    ],
)
def test_oauth_settings_reject_unsafe_or_ambiguous_urls(field: str, value: str):
    settings: dict[str, Any] = {field: value}
    with pytest.raises(ValueError):
        CloudOAuthSettings(**settings)


@pytest.mark.parametrize("status", [401, 403])
@pytest.mark.parametrize("stage", ["preflight", "request"])
async def test_workspace_access_errors_give_oauth_instructions(
    server_url: str,
    signing_key: RSAKeyPair,
    cloud_api: respx.MockRouter,
    status: int,
    stage: str,
):
    if stage == "preflight":
        cloud_api.get(f"{WORKSPACE_URL}/execution-plans/schema").respond(status)
    else:
        cloud_api.post(f"{WORKSPACE_URL}/deployments/filter").respond(status)
    async with connect(server_url, access_token(signing_key)) as client:
        result = await client.call_tool("list_deployments", {}, raise_on_error=False)
    assert "reconnect" in error_text(result).lower()
    assert "prefect cloud login" not in error_text(result)
    assert "API key" not in error_text(result)


async def test_grant_network_failures_do_not_fall_back_to_the_profile(
    server_url: str, signing_key: RSAKeyPair, cloud_api: respx.MockRouter
):
    cloud_api.routes["grant"].mock(side_effect=httpx.ReadTimeout("grant timed out"))
    async with connect(server_url, access_token(signing_key)) as client:
        result = await client.call_tool("get_workspace", {}, raise_on_error=False)
    assert "Could not reach Prefect Cloud" in error_text(result)
    assert not any("/api/accounts/" in c.request.url.path for c in cloud_api.calls)


async def test_provider_links_use_the_selected_oauth_workspace(
    server_url: str,
    signing_key: RSAKeyPair,
    cloud_api: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("PREFECT_UI_URL", "https://profile.example.com")
    cloud_api.routes["grant"].respond(200, json=[WORKSPACE, OTHER_WORKSPACE])
    cloud_api.get(f"{OTHER_WORKSPACE_URL}/agentic/providers").respond(
        200,
        json=[{"type": "anthropic", "models": []}, {"type": "prefect", "models": []}],
    )
    cloud_api.get(f"{OTHER_WORKSPACE_URL}/agentic/provider-connections").respond(
        200, json=[]
    )
    async with connect(server_url, access_token(signing_key)) as client:
        providers = await client.call_tool(
            "list_providers", {"workspace_id": OTHER_WORKSPACE_ID}
        )
        connection = await client.call_tool(
            "connect_model_provider",
            {"provider": "anthropic", "workspace_id": OTHER_WORKSPACE_ID},
        )
    add_url = (
        f"https://app.prefect.dev/account/{ACCOUNT_ID}/workspace/"
        f"{OTHER_WORKSPACE_ID}/agentic-model-providers?add=anthropic"
    )
    assert providers.structured_content is not None
    assert providers.structured_content["providers"][0]["add_url"] == add_url
    assert providers.structured_content["providers"][1]["add_url"] is None
    assert connection.structured_content is not None
    assert connection.structured_content["add_url"] == add_url
