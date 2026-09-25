"""Tests for the `list_secret_blocks` tool."""

import json
from typing import Any

import respx
from fastmcp import Client

from support import error_text

SECRET_TYPE_ID = "33333333-3333-3333-3333-333333333333"
OTHER_TYPE_ID = "44444444-4444-4444-4444-444444444444"


def block_document(
    block_id: str,
    name: str,
    *,
    type_name: str = "Secret",
    type_slug: str = "secret",
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a block document in the shape `/block_documents/filter` returns."""
    type_id = SECRET_TYPE_ID if type_slug == "secret" else OTHER_TYPE_ID
    return {
        "id": block_id,
        "created": "2026-09-01T00:00:00Z",
        "updated": "2026-09-01T00:00:00Z",
        "name": name,
        "data": data if data is not None else {"value": "********"},
        "block_schema_id": "55555555-5555-5555-5555-555555555555",
        "block_type_id": type_id,
        "block_type_name": type_name,
        "block_type": {
            "id": type_id,
            "name": type_name,
            "slug": type_slug,
        },
        "block_document_references": {},
        "is_anonymous": False,
    }


SLACK_TOKEN_ID = "aaaaaaaa-0000-0000-0000-000000000001"
GITHUB_TOKEN_ID = "aaaaaaaa-0000-0000-0000-000000000002"


async def test_list_secret_blocks_returns_names_and_ids(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    route = cloud_api.post("/block_documents/filter").respond(
        200,
        json=[
            block_document(GITHUB_TOKEN_ID, "github-token"),
            block_document(SLACK_TOKEN_ID, "slack-token"),
        ],
    )

    result = await mcp_client.call_tool("list_secret_blocks", {})

    assert result.structured_content == {
        "secret_blocks": [
            {"name": "github-token", "id": GITHUB_TOKEN_ID},
            {"name": "slack-token", "id": SLACK_TOKEN_ID},
        ]
    }
    body = json.loads(route.calls.last.request.content)
    assert body["block_types"] == {"slug": {"any_": ["secret"]}}
    assert body["include_secrets"] is False


async def test_list_secret_blocks_never_returns_a_secret_value(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post("/block_documents/filter").respond(
        200,
        json=[
            block_document(
                SLACK_TOKEN_ID,
                "slack-token",
                data={"value": "xoxb-real-secret-value"},
            )
        ],
    )

    result = await mcp_client.call_tool("list_secret_blocks", {})

    assert result.structured_content == {
        "secret_blocks": [{"name": "slack-token", "id": SLACK_TOKEN_ID}]
    }
    for content in result.content:
        assert "xoxb-real-secret-value" not in content.model_dump_json()


async def test_list_secret_blocks_excludes_other_block_types(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post("/block_documents/filter").respond(
        200,
        json=[
            block_document(SLACK_TOKEN_ID, "slack-token"),
            block_document(
                GITHUB_TOKEN_ID,
                "github-credentials",
                type_name="GitHub Credentials",
                type_slug="github-credentials",
            ),
        ],
    )

    result = await mcp_client.call_tool("list_secret_blocks", {})

    assert result.structured_content == {
        "secret_blocks": [{"name": "slack-token", "id": SLACK_TOKEN_ID}]
    }


async def test_list_secret_blocks_reads_every_page(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    first_page = [
        block_document(f"bbbbbbbb-0000-0000-0000-{index:012d}", f"secret-{index:03d}")
        for index in range(200)
    ]
    last_page = [block_document(SLACK_TOKEN_ID, "slack-token")]
    route = cloud_api.post("/block_documents/filter").mock(
        side_effect=[
            respx.MockResponse(200, json=first_page),
            respx.MockResponse(200, json=last_page),
        ]
    )

    result = await mcp_client.call_tool("list_secret_blocks", {})

    assert result.structured_content is not None
    blocks = result.structured_content["secret_blocks"]
    assert len(blocks) == 201
    assert blocks[-1] == {"name": "slack-token", "id": SLACK_TOKEN_ID}
    offsets = [json.loads(call.request.content)["offset"] for call in route.calls]
    assert offsets == [0, 200]


async def test_list_secret_blocks_returns_an_empty_list_when_there_are_none(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post("/block_documents/filter").respond(200, json=[])

    result = await mcp_client.call_tool("list_secret_blocks", {})

    assert result.structured_content == {"secret_blocks": []}


async def test_list_secret_blocks_reports_a_failed_request(
    mcp_client: Client[Any], cloud_api: respx.MockRouter
):
    cloud_api.post("/block_documents/filter").respond(
        403, json={"detail": "Missing scope see_secret_blocks"}
    )

    result = await mcp_client.call_tool(
        "list_secret_blocks", {}, raise_on_error=False
    )

    message = error_text(result)
    assert "403" in message
    assert "Missing scope see_secret_blocks" in message


async def test_list_secret_blocks_is_read_only(mcp_client: Client[Any]):
    tools = {tool.name: tool for tool in await mcp_client.list_tools()}

    annotations = tools["list_secret_blocks"].annotations
    assert annotations is not None
    assert annotations.readOnlyHint is True
    assert annotations.destructiveHint is False
