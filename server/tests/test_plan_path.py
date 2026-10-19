"""Tests for passing a plan file's path instead of the plan document."""

import json
from pathlib import Path
from typing import Any

import pytest
import respx
from fastmcp import Client
from support import PLAN, error_text, request_body

VALID = {"valid": True, "errors": []}


async def test_validate_plan_reads_the_plan_from_its_file(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, tmp_path: Path
):
    plan_file = tmp_path / "daily.plan.json"
    plan_file.write_text(json.dumps(PLAN))
    route = cloud_api.post("/execution-plans/validate").respond(200, json=VALID)

    result = await mcp_client.call_tool("validate_plan", {"plan_path": str(plan_file)})

    assert result.structured_content == VALID
    assert request_body(route.calls.last.request) == {"plan": PLAN}


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ({}, "exactly one of"),
        ({"plan": PLAN, "plan_path": "/tmp/x.plan.json"}, "exactly one of"),
        ({"plan_path": "daily.plan.json"}, "absolute path"),
        ({"plan_path": "/nonexistent/daily.plan.json"}, "not a regular file"),
    ],
    ids=["neither", "both", "relative", "missing"],
)
async def test_validate_plan_rejects_a_bad_plan_argument(
    mcp_client: Client[Any],
    cloud_api: respx.MockRouter,
    arguments: dict[str, Any],
    message: str,
):
    route = cloud_api.post("/execution-plans/validate").respond(200, json=VALID)

    result = await mcp_client.call_tool(
        "validate_plan", arguments, raise_on_error=False
    )

    assert message in error_text(result)
    assert not route.called


async def test_validate_plan_rejects_a_file_that_isnt_json(
    mcp_client: Client[Any], tmp_path: Path
):
    plan_file = tmp_path / "daily.plan.json"
    plan_file.write_text("{not json")

    result = await mcp_client.call_tool(
        "validate_plan", {"plan_path": str(plan_file)}, raise_on_error=False
    )

    assert "not valid JSON" in error_text(result)


async def test_validate_plan_reads_a_utf8_plan_file(
    mcp_client: Client[Any], cloud_api: respx.MockRouter, tmp_path: Path
):
    plan = {**PLAN, "description": "Résumé of the café's day"}
    plan_file = tmp_path / "daily.plan.json"
    plan_file.write_bytes(json.dumps(plan, ensure_ascii=False).encode("utf-8"))
    route = cloud_api.post("/execution-plans/validate").respond(200, json=VALID)

    await mcp_client.call_tool("validate_plan", {"plan_path": str(plan_file)})

    assert request_body(route.calls.last.request) == {"plan": plan}


async def test_validate_plan_rejects_a_directory(
    mcp_client: Client[Any], tmp_path: Path
):
    result = await mcp_client.call_tool(
        "validate_plan", {"plan_path": str(tmp_path)}, raise_on_error=False
    )

    assert "not a regular file" in error_text(result)
