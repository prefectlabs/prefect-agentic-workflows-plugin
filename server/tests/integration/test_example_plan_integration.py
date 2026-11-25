"""Integration tests that validate the skill's example plans against Cloud.

The examples are in `skills/agentic-workflows/references/examples/`. Each one
must pass `validate_plan` against the live schema, so a schema change that
breaks an example fails this suite. The mocked suite checks that the
examples exist and are linked from the skill.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from fastmcp import Client

EXAMPLES_DIR = (
    Path(__file__).resolve().parents[3]
    / "skills"
    / "agentic-workflows"
    / "references"
    / "examples"
)
EXAMPLES = sorted(EXAMPLES_DIR.glob("*.plan.json"))
# The examples use this ID for the model's provider connection, which the
# skill tells the agent to replace before publishing.
PLACEHOLDER_ID = "00000000-0000-0000-0000-000000000000"


async def prefect_connection_id(client: Client[Any]) -> str:
    """Return the ID of the workspace's `prefect` provider connection."""
    result = await client.call_tool("list_provider_connections", {})
    assert result.structured_content is not None
    connections = result.structured_content["provider_connections"]
    return next(each["id"] for each in connections if each["name"] == "prefect")


@pytest.mark.parametrize("path", EXAMPLES, ids=[path.name for path in EXAMPLES])
async def test_example_plan_passes_validate_plan(mcp_client: Client[Any], path: Path):
    # Validation rejects a model connection that the workspace doesn't have,
    # so the placeholder becomes a real one.
    connection_id = await prefect_connection_id(mcp_client)
    plan = json.loads(
        path.read_text().replace(
            f'"connection_id": "{PLACEHOLDER_ID}"',
            f'"connection_id": "{connection_id}"',
        )
    )

    result = await mcp_client.call_tool("validate_plan", {"plan": plan})

    body = result.structured_content
    assert body is not None
    assert body["errors"] == []
    assert body["valid"] is True
