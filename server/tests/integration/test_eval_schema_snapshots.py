"""Check the eval harness's schema copies against the live Cloud schema.

The fake Cloud in `evals/` validates plans against copies of the schema in
`evals/schemas/`. When Cloud changes a schema, these tests fail so the copies
don't go out of date without anyone noticing.
"""

import json
from typing import Any

import pytest
from fastmcp import Client

from evals.fake_cloud import SCHEMAS_DIR

SNAPSHOT_VERSIONS = sorted(path.stem for path in SCHEMAS_DIR.glob("*.json"))


@pytest.mark.parametrize("version", SNAPSHOT_VERSIONS)
async def test_schema_copy_matches_cloud(mcp_client: Client[Any], version: str):
    result = await mcp_client.call_tool("get_schema", {"version": version})

    body = result.structured_content
    assert body is not None
    snapshot = json.loads((SCHEMAS_DIR / f"{version}.json").read_text())
    assert body["schema"] == snapshot, (
        f"evals/schemas/{version}.json differs from Cloud's schema. Replace it "
        "with the `schema` field that `get_schema` returns for this version."
    )


async def test_every_supported_version_has_a_schema_copy(mcp_client: Client[Any]):
    result = await mcp_client.call_tool("get_schema", {})

    body = result.structured_content
    assert body is not None
    missing = sorted(set(body["supported_schema_versions"]) - set(SNAPSHOT_VERSIONS))
    assert not missing, (
        f"Cloud supports schema versions with no copy in evals/schemas/: {missing}"
    )
