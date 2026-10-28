"""Prefect Cloud token validation and consented workspace resolution."""

from uuid import UUID

import httpx
from fastmcp.exceptions import ToolError
from fastmcp.server.auth import RemoteAuthProvider
from fastmcp.server.auth.providers.jwt import JWTVerifier
from fastmcp.server.dependencies import get_access_token
from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


class CloudOAuthSettings(BaseSettings):
    """Configure the trusted Prefect Cloud origins and the public MCP URL."""

    model_config = SettingsConfigDict(env_prefix="PREFECT_MCP_CLOUD_", extra="ignore")

    enabled: bool = False
    api_base_url: str = "https://api.prefect.cloud"
    auth_base_url: str | None = None
    public_base_url: str | None = None

    @field_validator("api_base_url", "auth_base_url", "public_base_url")
    @classmethod
    def validate_base_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        url = httpx.URL(value)
        if (
            not url.is_absolute_url
            or url.scheme != "https"
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "OAuth base URLs must use HTTPS without credentials, "
                "queries, or fragments."
            )
        return str(url).rstrip("/")

    @model_validator(mode="after")
    def validate_oauth_origins(self) -> "CloudOAuthSettings":
        if self.enabled and self.public_base_url is None:
            raise ValueError("public_base_url is required when OAuth is enabled.")
        for origin in [self.api_base_url, self.auth_base_url]:
            if origin is not None and httpx.URL(origin).path not in {"", "/"}:
                raise ValueError(
                    "OAuth API and auth base URLs must be origins without a path."
                )
        return self

    @property
    def auth_origin(self) -> str:
        return self.auth_base_url or self.api_base_url

    @property
    def resource_url(self) -> str:
        if self.public_base_url is None:
            raise ValueError("public_base_url is required when OAuth is enabled.")
        return f"{self.public_base_url.removesuffix('/mcp')}/mcp"


class WorkspaceGrant(BaseModel):
    """A workspace included in a user's Prefect Cloud consent grant."""

    account_id: UUID
    workspace_id: UUID
    account_handle: str | None = None
    workspace_handle: str | None = None

    @property
    def handle_name(self) -> str | None:
        if self.account_handle and self.workspace_handle:
            return f"{self.account_handle}/{self.workspace_handle}"
        return None

    @property
    def display_name(self) -> str:
        return self.handle_name or str(self.workspace_id)


def build_auth_provider(settings: CloudOAuthSettings) -> RemoteAuthProvider | None:
    """Return an OAuth provider when Prefect Cloud OAuth is enabled."""
    if not settings.enabled:
        return None
    base_url = settings.resource_url.removesuffix("/mcp")
    verifier = JWTVerifier(
        jwks_uri=f"{settings.auth_origin}/auth/mcp/oauth/jwks.json",
        issuer=settings.auth_origin,
        audience=settings.resource_url,
        algorithm="RS256",
        required_scopes=["prefect-cloud:workspaces"],
        base_url=base_url,
    )
    # OAuth issuer identifiers must match exactly, including trailing slashes.
    issuer_url = TypeAdapter(
        AnyHttpUrl, config=ConfigDict(url_preserve_empty_path=True)
    ).validate_python(settings.auth_origin)
    return RemoteAuthProvider(
        token_verifier=verifier,
        authorization_servers=[issuer_url],
        base_url=base_url,
        scopes_supported=["prefect-cloud:workspaces"],
    )


async def resolve_authorized_workspace(
    settings: CloudOAuthSettings, workspace_id: UUID | None
) -> WorkspaceGrant:
    """Return a consented workspace, requiring a choice when several exist."""
    token = get_access_token()
    if token is None:
        raise ToolError(
            "A validated Prefect Cloud OAuth token is required. "
            "Reconnect to this server."
        )
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(
                f"{settings.auth_origin}/auth/mcp/oauth/grant/workspaces",
                headers={"Authorization": f"Bearer {token.token}"},
            )
    except httpx.HTTPError as exc:
        raise ToolError(
            "Could not reach Prefect Cloud to read the OAuth grant. Try again."
        ) from exc
    if response.status_code in {401, 403}:
        raise ToolError(
            "Prefect Cloud rejected access to the OAuth grant. "
            "Reconnect and authorize your workspaces again."
        )
    if response.is_error:
        raise ToolError(
            f"Could not read the Prefect Cloud OAuth grant "
            f"(HTTP {response.status_code}). Try again."
        )
    try:
        workspaces = TypeAdapter(list[WorkspaceGrant]).validate_json(response.content)
    except ValidationError as exc:
        raise ToolError(
            "Prefect Cloud returned an invalid workspace grant. "
            "Reconnect and authorize your workspaces again."
        ) from exc
    if workspace_id is not None:
        for workspace in workspaces:
            if workspace.workspace_id == workspace_id:
                return workspace
        raise ToolError(
            "This workspace is not included in the OAuth consent grant. "
            "Choose a consented workspace or reconnect to authorize it."
        )
    if len(workspaces) == 1:
        return workspaces[0]
    if not workspaces:
        raise ToolError(
            "The OAuth grant includes no workspaces. "
            "Reconnect and select at least one workspace."
        )
    choices = ", ".join(f"{w.display_name} ({w.workspace_id})" for w in workspaces)
    raise ToolError(
        "workspace_id is required because the OAuth grant includes several "
        f"workspaces: {choices}. Ask which workspace to use, then pass its "
        "workspace_id to each tool."
    )
