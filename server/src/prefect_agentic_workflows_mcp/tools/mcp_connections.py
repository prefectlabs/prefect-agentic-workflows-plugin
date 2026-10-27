"""Tools that save MCP servers as connections and check that they work.

An agent node references a connection by ID instead of embedding a server URL
and its credentials. No tool takes or returns a secret: the user signs in, or
enters header values, on the connection's setup page in Prefect Cloud.
"""

from typing import Annotated, Any, Literal
from uuid import UUID

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from mcp.types import InputRequiredResult
from pydantic import BaseModel, ConfigDict, Field

from prefect_agentic_workflows_mcp.parameters import WorkspaceId
from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.tools._url_elicitation import (
    ask_client_to_open_url,
    client_can_open_url,
    client_opened_url,
)
from prefect_agentic_workflows_mcp.workspace_api import (
    HttpMethod,
    WorkspaceApi,
    coded_error_detail,
    read_json,
    response_detail,
)

MCP_CONNECTIONS_PATH = "/agentic/mcp-connections"
SETUP_INPUT_KEY = "set_up_connection"

# Cloud answers every MCP connection route with this 404 detail when the
# account doesn't have the feature. A missing connection has its own detail.
FEATURE_OFF_DETAIL = "Not found"
FEATURE_OFF_MESSAGE = (
    "Prefect Cloud returned 404 for the MCP connection API, so MCP "
    "connections are not enabled for this account. Ask your Prefect contact to "
    "turn them on. Until then, an agent node can reach an MCP server with an "
    "inline `http` server whose credential headers reference Secret blocks "
    "from `list_secret_blocks`."
)
SETUP_NOT_FINISHED_MESSAGE = (
    "The connection can't be checked because its setup isn't finished (state: "
    "{state}). Send the user to the connection's `setup_url` from "
    "`get_mcp_connection`, then check it again."
)

ConnectionId = Annotated[
    UUID,
    Field(
        description=(
            "ID of an MCP connection, from `list_mcp_connections` or "
            "`create_mcp_connection`."
        )
    ),
]


class McpOAuthSettings(BaseModel):
    """The OAuth choices for a connection. Prefect Cloud discovers the rest."""

    model_config = ConfigDict(extra="forbid")

    bearer_token_source: Literal["access_token", "id_token"] = Field(
        "access_token",
        description=(
            "The token the connection sends as `Authorization: Bearer`. Use "
            "`id_token` only when the server's documentation asks for an "
            "OpenID Connect ID token; it needs `openid` in `scopes`."
        ),
    )
    scopes: list[str] | None = Field(
        None,
        description=(
            "OAuth scopes to request. Omit to request the scopes from "
            "`discover_mcp_server`."
        ),
    )


class McpHeaderSpec(BaseModel):
    """A header the connection sends. The user enters its value on the setup page."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(
        description="Header name, such as `Authorization` or `X-API-Key`."
    )
    value_prefix: str = Field(
        "",
        description=(
            "Fixed text before the value the user enters, such as `Bearer ` "
            "with its trailing space."
        ),
    )


class DiscoveredOAuth(BaseModel):
    issuer: str
    supports_dcr: bool
    scopes_supported: list[str] | None
    supports_id_token: bool


class DiscoveredMcpServer(BaseModel):
    requires_auth: bool
    oauth: DiscoveredOAuth | None


class McpConnectionStatus(BaseModel):
    state: str
    reason: str | None = None


class McpConnectionHeader(BaseModel):
    name: str
    value_prefix: str
    has_value: bool


class McpConnectionOAuth(BaseModel):
    bearer_token_source: str
    scopes: list[str]
    issuer: str | None
    supports_dcr: bool | None


class McpConnectionCheck(BaseModel):
    outcome: str
    checked_at: str
    tool_count: int | None = None


class McpConnection(BaseModel):
    id: UUID
    name: str
    url: str
    auth_method: str
    status: McpConnectionStatus
    headers: list[McpConnectionHeader]
    oauth: McpConnectionOAuth | None
    last_check: McpConnectionCheck | None
    setup_url: str
    created_by_actor_id: str


class McpConnectionList(BaseModel):
    mcp_connections: list[McpConnection]


SetupOutcome = Literal["opened", "declined", "not_needed", "client_cannot_open_links"]


class McpConnectionSetup(BaseModel):
    setup: SetupOutcome
    connection: McpConnection


class SetupRequest(BaseModel):
    """What the first round of `create_mcp_connection` hands to the second."""

    connection_id: UUID


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the MCP connection tools to `mcp`."""

    async def call(
        method: HttpMethod,
        path: str = "",
        *,
        json: Any = None,
        workspace_id: WorkspaceId = None,
    ) -> Any:
        full_path = f"{MCP_CONNECTIONS_PATH}{path}"
        response = await api.request(
            method, full_path, json=json, workspace_id=workspace_id
        )
        if (
            response.status_code == 404
            and response_detail(response) == FEATURE_OFF_DETAIL
        ):
            raise ToolError(FEATURE_OFF_MESSAGE)
        detail = coded_error_detail(response)
        if detail is not None and detail["code"] == "mcp_connection_unavailable":
            raise ToolError(SETUP_NOT_FINISHED_MESSAGE.format(state=detail["state"]))
        return read_json(response, method, full_path)

    async def read_connection(
        connection_id: UUID, workspace_id: WorkspaceId = None
    ) -> McpConnection:
        return McpConnection.model_validate(
            await call("GET", f"/{connection_id}", workspace_id=workspace_id)
        )

    @tool(mcp, read_only=True)
    async def discover_mcp_server(
        url: Annotated[
            str,
            Field(description="The MCP server's `https://` URL."),
        ],
        workspace_id: WorkspaceId = None,
    ) -> DiscoveredMcpServer:
        """Find out how an MCP server signs in, before you create a connection to it.

        Saves nothing. Choose the connection's settings from the answer:

        - `requires_auth` is false: the server lists its tools without
          credentials. Create the connection with no `oauth` and no `headers`.
        - `oauth` is set: the server uses OAuth. Create the connection with
          `oauth`. When `oauth.supports_dcr` is false, Prefect Cloud can't
          register itself with the provider, so tell the user they'll need
          the client ID of an OAuth app they registered at the provider.
        - `requires_auth` is true and `oauth` is null: the server needs a
          fixed credential, such as an API key. Create the connection with
          `headers`, using the header name from the provider's documentation.
        """
        return DiscoveredMcpServer.model_validate(
            await call(
                "POST", "/discover", json={"url": url}, workspace_id=workspace_id
            )
        )

    @tool(mcp, read_only=True)
    async def list_mcp_connections(
        workspace_id: WorkspaceId = None,
    ) -> McpConnectionList:
        """List the workspace's MCP connections and whether each one works.

        An agent node uses a connection with
        `{"type": "connection", "connection_id": "<id>"}` in its
        `mcp.mcpServers` map. Reuse a connection for the same server and
        account instead of creating another. Never ask the user for a
        connection ID.

        `status.state` is one of:

        - `authorized` or `not_verified`: the connection can run.
          `not_verified` means it hasn't passed `check_mcp_connection` yet.
        - `refresh_delayed`: the provider is failing for now and Prefect Cloud
          retries. Runs fail once the token expires.
        - `authorization_needed` or `reauthorization_required`: runs fail
          until the user finishes setup or signs in again at `setup_url`.

        `created_by_actor_id` is the actor who created the connection.
        """
        return McpConnectionList(
            mcp_connections=[
                McpConnection.model_validate(connection)
                for connection in await call("GET", workspace_id=workspace_id)
            ]
        )

    @tool(mcp, read_only=True)
    async def get_mcp_connection(
        connection_id: ConnectionId, workspace_id: WorkspaceId = None
    ) -> McpConnection:
        """Read one MCP connection, with its status and setup link.

        `list_mcp_connections` describes each `status.state`. When the user
        has to act, send them `setup_url`; the page asks them to sign in to
        Prefect Cloud, and the link grants nothing by itself.
        """
        return await read_connection(connection_id, workspace_id=workspace_id)

    @tool(mcp, read_only=False)
    async def create_mcp_connection(
        name: Annotated[
            str,
            Field(
                description=(
                    'The provider and the account, such as "GitHub (eddie-park)". '
                    "Names are unique in the workspace."
                )
            ),
        ],
        url: Annotated[str, Field(description="The MCP server's `https://` URL.")],
        ctx: Context,
        oauth: Annotated[
            McpOAuthSettings | None,
            Field(description="Set it to sign in with OAuth, even as `{}`."),
        ] = None,
        headers: Annotated[
            list[McpHeaderSpec] | None,
            Field(description="Headers whose values the user enters."),
        ] = None,
        workspace_id: WorkspaceId = None,
    ) -> McpConnectionSetup | InputRequiredResult:
        """Save an MCP server as a connection, and have the user finish its setup.

        Call `discover_mcp_server` first and choose `oauth` or `headers` from
        its answer. Neither holds a secret: the user signs in, or enters each
        header value, on the connection's setup page in Prefect Cloud, outside
        this conversation. Never ask the user for a token, key, or password.
        When you don't know the account to name the connection after, ask.

        When the connection needs setup, the tool opens its setup page for the
        user and returns the connection after they come back. `setup` says
        what happened:

        - `opened`: read `connection.status` to see whether they finished.
        - `declined`: the user closed the page.
        - `not_needed`: the server needs no credential.
        - `client_cannot_open_links`: send the user `connection.setup_url`.

        Then call `check_mcp_connection`. Creating a connection needs
        permission to manage MCP connections, which the Owner and Developer
        roles have.
        """
        if ctx.request_state is None:
            connection = McpConnection.model_validate(
                await call(
                    "POST",
                    json={
                        "name": name,
                        "url": url,
                        "oauth": oauth.model_dump() if oauth is not None else None,
                        "headers": [header.model_dump() for header in headers or []],
                    },
                    workspace_id=workspace_id,
                )
            )
            if connection.status.state != "authorization_needed":
                return McpConnectionSetup(setup="not_needed", connection=connection)
            if not client_can_open_url(ctx):
                return McpConnectionSetup(
                    setup="client_cannot_open_links", connection=connection
                )
            opened = await ask_client_to_open_url(
                ctx,
                key=SETUP_INPUT_KEY,
                message=(
                    f"Finish setting up {connection.name} in Prefect Cloud, "
                    "then come back."
                ),
                url=connection.setup_url,
                request_state=SetupRequest(
                    connection_id=connection.id
                ).model_dump_json(),
            )
            if isinstance(opened, InputRequiredResult):
                return opened
            connection_id = connection.id
        else:
            connection_id = SetupRequest.model_validate_json(
                ctx.request_state
            ).connection_id
            opened = client_opened_url(ctx, SETUP_INPUT_KEY)

        return McpConnectionSetup(
            setup="opened" if opened else "declined",
            connection=await read_connection(connection_id, workspace_id=workspace_id),
        )

    @tool(mcp, read_only=False)
    async def check_mcp_connection(
        connection_id: ConnectionId, workspace_id: WorkspaceId = None
    ) -> McpConnection:
        """Check an MCP connection by listing the server's tools with its credentials.

        Saves the result as `last_check` and returns the connection.
        `last_check.outcome` is one of:

        - `succeeded`: the server accepted the credentials and listed
          `tool_count` tools. It doesn't show that every tool call is allowed.
        - `authentication_failed`: the server rejected the credentials. Send
          the user to `setup_url` to sign in again or replace the header value.
        - `unavailable` or `invalid_configuration`: the server didn't answer
          as an MCP server. Tell the user, and check again later.
        """
        return McpConnection.model_validate(
            await call("POST", f"/{connection_id}/check", workspace_id=workspace_id)
        )
