"""Tools that find and add the model providers an agent node's `model` uses."""

from typing import Any

from fastmcp import FastMCP
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
    models: list[Model] | None = None


class ProviderConnectionList(BaseModel):
    provider_connections: list[ProviderConnection]


class Provider(BaseModel):
    type: str
    models: list[Model]


class ProviderList(BaseModel):
    providers: list[Provider]


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the model provider tools to `mcp`."""

    @tool(mcp, read_only=True)
    async def list_provider_connections() -> ProviderConnectionList:
        """List the workspace's model provider connections.

        Returns each connection's `id`, `name`, `provider` type, and `models`.
        An agent node runs a specific model with
        `"model": {"connection_id": "<id>", "id": "<model id>"}`. `models`
        lists the only models a connection runs; null means it runs the
        models its provider offers, which `list_providers` lists. Never ask the
        user for a connection ID.
        """
        connections = TypeAdapter(list[ProviderConnection]).validate_python(
            await api.call("GET", PROVIDER_CONNECTIONS_PATH)
        )
        return ProviderConnectionList(provider_connections=connections)

    @tool(mcp, read_only=True)
    async def list_providers() -> ProviderList:
        """List the provider types a connection can use and their known models.

        Use it to turn a model name the user gives, such as "Haiku 5.5", into
        its model id, and to tell whether a provider the workspace has no
        connection for offers a model. A `prefect` connection runs only the
        models listed under `prefect`.
        """
        providers = TypeAdapter(list[Provider]).validate_python(
            await api.call("GET", PROVIDERS_PATH)
        )
        return ProviderList(providers=providers)

    @tool(mcp, read_only=False)
    async def create_prefect_connection(name: str = "prefect") -> ProviderConnection:
        """Add a connection that runs models on Prefect Cloud's credentials.

        It needs no API key. Call it only after the user chooses Prefect's
        models. A connection with the user's own key is added in the Prefect
        Cloud UI, never with a tool: link the user to
        `<app_url>/agentic-model-providers?add=<anthropic|openai|vertex>`,
        with `app_url` from `get_workspace`, and call
        `list_provider_connections` again when they're done.
        """
        return ProviderConnection.model_validate(
            await api.call(
                "POST",
                PROVIDER_CONNECTIONS_PATH,
                json={"name": name, "provider": {"type": "prefect"}},
            )
        )
