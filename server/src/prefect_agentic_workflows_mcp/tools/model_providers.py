"""Tools that find and add the model providers an agent node's `model` uses."""

from typing import Any, Literal

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from mcp.types import InputRequiredResult
from pydantic import BaseModel, TypeAdapter

from prefect_agentic_workflows_mcp.parameters import WorkspaceId
from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.tools._url_elicitation import (
    ask_client_to_open_url,
    client_can_open_url,
    client_opened_url,
)
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi

PROVIDER_CONNECTIONS_PATH = "/agentic/provider-connections"
PROVIDERS_PATH = "/agentic/providers"


class Model(BaseModel):
    id: str


class ConnectionProvider(BaseModel):
    type: str


class ProviderConnection(BaseModel):
    id: str
    name: str
    provider: ConnectionProvider


class ProviderConnectionList(BaseModel):
    provider_connections: list[ProviderConnection]


class CatalogProvider(BaseModel):
    type: str
    models: list[Model]


class Provider(CatalogProvider):
    add_url: str | None


class ProviderList(BaseModel):
    providers: list[Provider]


ConnectStatus = Literal[
    "connected", "declined", "no_new_connection", "client_cannot_open_links"
]


class ConnectOutcome(BaseModel):
    status: ConnectStatus
    connections: list[ProviderConnection]
    add_url: str
    known_connection_ids: list[str]


class ConnectRequest(BaseModel):
    """What the first round of `connect_model_provider` hands to the second."""

    provider: str
    add_url: str
    known_connection_ids: list[str]

    def outcome(
        self, status: ConnectStatus, connections: list[ProviderConnection]
    ) -> ConnectOutcome:
        return ConnectOutcome(
            status=status,
            connections=connections,
            add_url=self.add_url,
            known_connection_ids=self.known_connection_ids,
        )


CONNECT_INPUT_KEY = "add_connection"


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the model provider tools to `mcp`."""

    async def list_connections(
        workspace_id: WorkspaceId = None,
    ) -> list[ProviderConnection]:
        return TypeAdapter(list[ProviderConnection]).validate_python(
            await api.call("GET", PROVIDER_CONNECTIONS_PATH, workspace_id=workspace_id)
        )

    def add_url(provider_type: str, ui_url: str) -> str:
        return f"{ui_url}/agentic-model-providers?add={provider_type}"

    @tool(mcp, read_only=True)
    async def list_provider_connections(
        workspace_id: WorkspaceId = None,
    ) -> ProviderConnectionList:
        """List the workspace's model provider connections.

        Returns each connection's `id`, `name`, and `provider` type. An agent
        node sets its model with
        `"model": {"connection_id": "<id>", "id": "<model id>"}`, using a model
        its provider offers in `list_providers`. Never ask the user for a
        connection ID.
        """
        return ProviderConnectionList(
            provider_connections=await list_connections(workspace_id=workspace_id)
        )

    @tool(mcp, read_only=True)
    async def list_providers(workspace_id: WorkspaceId = None) -> ProviderList:
        """List the provider types a connection can use and their known models.

        Use it to turn a model name the user gives, such as "Haiku 5.5", into
        its model id, and to tell whether a provider the workspace has no
        connection for offers a model. A `prefect` connection runs only the
        models listed under `prefect`. `add_url` is the Prefect Cloud page
        where the user adds a connection for that provider; `prefect` has none,
        because every workspace already has its connection.
        """
        catalog = TypeAdapter(list[CatalogProvider]).validate_python(
            await api.call("GET", PROVIDERS_PATH, workspace_id=workspace_id)
        )
        ui_url = await api.current_ui_url(workspace_id=workspace_id)
        return ProviderList(
            providers=[
                Provider(
                    type=provider.type,
                    models=provider.models,
                    add_url=None
                    if provider.type == "prefect"
                    else add_url(provider.type, ui_url),
                )
                for provider in catalog
            ]
        )

    async def new_connect_request(
        provider: str, workspace_id: WorkspaceId = None
    ) -> ConnectRequest:
        if provider == "prefect":
            raise ToolError(
                "The workspace already has a `prefect` connection; find it with "
                "`list_provider_connections`."
            )
        catalog = TypeAdapter(list[CatalogProvider]).validate_python(
            await api.call("GET", PROVIDERS_PATH, workspace_id=workspace_id)
        )
        provider_types = [entry.type for entry in catalog]
        if provider not in provider_types:
            raise ToolError(
                f"Prefect Cloud has no model provider `{provider}`. Use one of "
                f"{', '.join(provider_types)}."
            )
        return ConnectRequest(
            provider=provider,
            add_url=add_url(
                provider, await api.current_ui_url(workspace_id=workspace_id)
            ),
            known_connection_ids=[
                connection.id
                for connection in await list_connections(workspace_id=workspace_id)
            ],
        )

    @tool(mcp, read_only=False)
    async def connect_model_provider(
        provider: str, ctx: Context, workspace_id: WorkspaceId = None
    ) -> ConnectOutcome | InputRequiredResult:
        """Have the user add a connection to their own provider account.

        The user enters its credentials on the provider's Prefect Cloud page,
        outside this conversation. When several connections come back, ask which to
        use. On `client_cannot_open_links`, send the user `add_url`; the new
        connection is the one not in `known_connection_ids`.
        """
        if ctx.request_state is None:
            request = await new_connect_request(provider, workspace_id=workspace_id)
            if not client_can_open_url(ctx):
                return request.outcome("client_cannot_open_links", [])
            opened = await ask_client_to_open_url(
                ctx,
                key=CONNECT_INPUT_KEY,
                message=(
                    f"Add your {provider} connection in Prefect Cloud, then come back."
                ),
                url=request.add_url,
                request_state=request.model_dump_json(),
            )
            if isinstance(opened, InputRequiredResult):
                return opened
        else:
            request = ConnectRequest.model_validate_json(ctx.request_state)
            opened = client_opened_url(ctx, CONNECT_INPUT_KEY)

        if not opened:
            return request.outcome("declined", [])
        new = [
            connection
            for connection in await list_connections(workspace_id=workspace_id)
            if connection.provider.type == request.provider
            and connection.id not in request.known_connection_ids
        ]
        if not new:
            return request.outcome("no_new_connection", [])
        return request.outcome("connected", new)
