"""A tool that lists the Secret blocks a plan can reference."""

from typing import Any

from fastmcp import FastMCP
from pydantic import BaseModel

from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi

SECRET_BLOCK_TYPE_SLUG = "secret"
SECRET_BLOCK_TYPE_NAME = "Secret"
PAGE_SIZE = 200


class SecretBlock(BaseModel):
    name: str
    id: str


class SecretBlockList(BaseModel):
    secret_blocks: list[SecretBlock]


def is_secret_block(document: dict[str, Any]) -> bool:
    """Return whether a block document from the API is of type `Secret`."""
    block_type = document.get("block_type") or {}
    if "slug" in block_type:
        return block_type["slug"] == SECRET_BLOCK_TYPE_SLUG
    return document.get("block_type_name") == SECRET_BLOCK_TYPE_NAME


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the Secret block tool to `mcp`."""

    @tool(mcp, read_only=True)
    async def list_secret_blocks() -> SecretBlockList:
        """List the Secret blocks in the workspace by name and ID.

        Returns names and IDs only, never secret values. Reference a block in
        a plan with `{"$ref": {"block_document_id": "<id>"}}`, for example
        for a sensitive MCP header or query value. If the block you need is
        missing, give the user the command or UI steps to create it. Never
        ask the user for the secret value.
        """
        blocks: list[SecretBlock] = []
        offset = 0
        while True:
            page = await api.call(
                "POST",
                "/block_documents/filter",
                json={
                    "block_types": {"slug": {"any_": [SECRET_BLOCK_TYPE_SLUG]}},
                    "include_secrets": False,
                    "sort": "NAME_ASC",
                    "offset": offset,
                    "limit": PAGE_SIZE,
                },
            )
            documents: list[dict[str, Any]] = page or []
            # Copy only the name and ID. The filter already asks for Secret
            # blocks without their values, and this keeps both promises even
            # if the API sends back something else.
            blocks.extend(
                SecretBlock(name=str(document["name"]), id=str(document["id"]))
                for document in documents
                if is_secret_block(document)
            )
            if len(documents) < PAGE_SIZE:
                return SecretBlockList(secret_blocks=blocks)
            offset += PAGE_SIZE
