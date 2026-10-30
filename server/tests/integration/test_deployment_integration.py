"""Integration tests for `list_deployments` against a real Cloud workspace."""

from typing import Any

import pytest
from fastmcp import Client


async def test_list_deployments_returns_ids_and_names(mcp_client: Client[Any]):
    result = await mcp_client.call_tool("list_deployments", {})

    body = result.structured_content
    assert body is not None
    assert len(body["deployments"]) <= 50
    for deployment in body["deployments"]:
        assert set(deployment) == {"id", "name", "flow_name", "description"}


async def test_list_deployments_finds_a_deployment_by_name(mcp_client: Client[Any]):
    listed = await mcp_client.call_tool("list_deployments", {})
    assert listed.structured_content is not None
    # Cloud matches `name` with SQL LIKE, so `_` and `%` match other names too.
    candidates = [
        deployment
        for deployment in listed.structured_content["deployments"]
        if not {"_", "%"} & set(deployment["name"])
    ]
    if not candidates:
        pytest.skip("the workspace has no deployment to search for")
    wanted = candidates[0]

    result = await mcp_client.call_tool(
        "list_deployments", {"name": wanted["name"].upper()}
    )

    assert result.structured_content is not None
    found = result.structured_content["deployments"]
    assert wanted["id"] in {deployment["id"] for deployment in found}
    for deployment in found:
        searched = f"{deployment['name']} {deployment['flow_name']}".lower()
        assert wanted["name"].lower() in searched
