"""Constants, payloads, and helpers shared by the mocked tests."""

from typing import Any

from fastmcp.client.client import CallToolResult
from mcp.types import TextContent

ACCOUNT_ID = "11111111-1111-1111-1111-111111111111"
WORKSPACE_ID = "22222222-2222-2222-2222-222222222222"
WORKSPACE_API_URL = (
    f"https://api.prefect.cloud/api/accounts/{ACCOUNT_ID}/workspaces/{WORKSPACE_ID}"
)
API_KEY = "pnu_test_key"


def schema_response(version: str = "0.1") -> dict[str, Any]:
    """Return a `GET /execution-plans/schema` body in the shape Cloud returns."""
    return {
        "schema_version": version,
        "schema": {
            "type": "object",
            "properties": {"schema_version": {"const": version}},
            "required": ["schema_version", "kind", "nodes"],
        },
        "supported_schema_versions": [version],
        "current_schema_version": version,
        "is_current": True,
        "is_deprecated": False,
        "document_shape_only": True,
        "validation_guidance": (
            "This JSON Schema describes document shape only. Call "
            "POST /execution-plans/validate before publishing a draft because "
            "semantic validation may still reject schema-valid documents."
        ),
    }


def error_text(result: CallToolResult) -> str:
    """Return the message of a tool call that failed."""
    assert result.is_error, f"expected an error, got {result.structured_content!r}"
    content = result.content[0]
    assert isinstance(content, TextContent)
    return content.text
