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


@pytest.mark.parametrize("path", EXAMPLES, ids=[path.name for path in EXAMPLES])
async def test_example_plan_passes_validate_plan(mcp_client: Client[Any], path: Path):
    plan = json.loads(path.read_text())

    result = await mcp_client.call_tool("validate_plan", {"plan": plan})

    body = result.structured_content
    assert body is not None
    assert body["errors"] == []
    assert body["valid"] is True

