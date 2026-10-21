"""Tools that find and add the model providers an agent node's `model` uses."""

import uuid
from typing import Any, Literal

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from mcp.types import (
    ElicitRequest,
    ElicitRequestURLParams,
    ElicitResult,
    InputRequiredResult,
)
from mcp_types.version import MODERN_PROTOCOL_VERSIONS
from pydantic import BaseModel, TypeAdapter

from prefect_agentic_workflows_mcp.tools import tool
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
    add_url: str


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

    async def list_connections() -> list[ProviderConnection]:
        return TypeAdapter(list[ProviderConnection]).validate_python(
            await api.call("GET", PROVIDER_CONNECTIONS_PATH)
        )

    def add_url(provider_type: str) -> str:
        return f"{api.current_ui_url()}/agentic-model-providers?add={provider_type}"

    @tool(mcp, read_only=True)
    async def list_provider_connections() -> ProviderConnectionList:
        """List the workspace's model provider connections.

        Returns each connection's `id`, `name`, and `provider` type. An agent
        node runs a specific model with
        `"model": {"connection_id": "<id>", "id": "<model id>"}`, using a model
        its provider offers in `list_providers`. Never ask the user for a
        connection ID.
        """
        return ProviderConnectionList(provider_connections=await list_connections())

    @tool(mcp, read_only=True)
    async def list_providers() -> ProviderList:
        """List the provider types a connection can use and their known models.

        Use it to turn a model name the user gives, such as "Haiku 5.5", into
        its model id, and to tell whether a provider the workspace has no
        connection for offers a model. A `prefect` connection runs only the
        models listed under `prefect`. `add_url` is the Prefect Cloud page
        where the user adds a connection for that provider.
        """
        catalog = TypeAdapter(list[CatalogProvider]).validate_python(
            await api.call("GET", PROVIDERS_PATH)
        )
        return ProviderList(
            providers=[
                Provider(
                    type=provider.type,
                    models=provider.models,
                    add_url=add_url(provider.type),
                )
                for provider in catalog
            ]
        )

    async def new_connect_request(provider: str) -> ConnectRequest:
        catalog = TypeAdapter(list[CatalogProvider]).validate_python(
            await api.call("GET", PROVIDERS_PATH)
        )
        provider_types = [entry.type for entry in catalog]
        if provider not in provider_types:
            raise ToolError(
                f"Prefect Cloud has no model provider `{provider}`. Use one of "
                f"{', '.join(provider_types)}."
            )
        return ConnectRequest(
            provider=provider,
            add_url=add_url(provider),
            known_connection_ids=[
                connection.id for connection in await list_connections()
            ],
        )

    @tool(mcp, read_only=False)
    async def connect_model_provider(
        provider: str, ctx: Context
    ) -> ConnectOutcome | InputRequiredResult:
        """Have the user add a connection to their own provider account.

        The user enters its credentials on the provider's Prefect Cloud page,
        outside this conversation. When several connections come back, ask which to
        use. On `client_cannot_open_links`, send the user `add_url`; the new
        connection is the one not in `known_connection_ids`.
        """
        if ctx.request_state is None:
            request = await new_connect_request(provider)
            capabilities = ctx.session.client_capabilities
            if (
                capabilities is None
                or capabilities.elicitation is None
                or capabilities.elicitation.url is None
            ):
                return request.outcome("client_cannot_open_links", [])
            message = (
                f"Add your {provider} connection in Prefect Cloud, then come back."
            )
            elicitation_id = str(uuid.uuid4())
            # The 2026-07-28 protocol has no server-to-client requests mid-call:
            # the tool returns the ask, and the client calls it again with the
            # answer and the request state.
            if ctx.session.protocol_version in MODERN_PROTOCOL_VERSIONS:
                return InputRequiredResult(
                    input_requests={
                        CONNECT_INPUT_KEY: ElicitRequest(
                            params=ElicitRequestURLParams(
                                message=message,
                                url=request.add_url,
                                elicitation_id=elicitation_id,
                            )
                        )
                    },
                    request_state=request.model_dump_json(),
                )
            answer = await ctx.session.elicit_url(
                message=message,
                url=request.add_url,
                elicitation_id=elicitation_id,
                related_request_id=ctx.request_id,
            )
        else:
            request = ConnectRequest.model_validate_json(ctx.request_state)
            answer = (ctx.input_responses or {}).get(CONNECT_INPUT_KEY)

        if not isinstance(answer, ElicitResult) or answer.action != "accept":
            return request.outcome("declined", [])
        new = [
            connection
            for connection in await list_connections()
            if connection.provider.type == request.provider
            and connection.id not in request.known_connection_ids
        ]
        if not new:
            return request.outcome("no_new_connection", [])
        return request.outcome("connected", new)
