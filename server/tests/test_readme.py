"""Checks that the repository README matches the server it documents.

The README at the repo root holds the quickstart, the install commands, and the
tool list. These tests fail when it names a tool the server doesn't register,
leaves out a tool the server does register, or runs a console script that the
package doesn't define. They also fail when the quickstart stops coming first,
or stops matching the example plan it builds.
"""

import json
import re
from pathlib import Path
from typing import Any

from fastmcp import Client

REPO_ROOT = Path(__file__).resolve().parents[2]
README = REPO_ROOT / "README.md"
PYPROJECT = REPO_ROOT / "server" / "pyproject.toml"
FEEDBACK_REPLY_EXAMPLE = (
    REPO_ROOT
    / "skills"
    / "agentic-workflows"
    / "references"
    / "examples"
    / "customer-feedback-reply.plan.json"
)
INSTALL_SOURCE = (
    "git+https://github.com/prefectlabs/prefect-agentic-workflows-plugin"
    "#subdirectory=server"
)

CONSOLE_SCRIPT = re.compile(r"^\[project\.scripts\]\n([a-z0-9-]+) =", re.MULTILINE)
TOOL_NAME = re.compile(
    r"`((?:get|list|create|update|delete|validate|publish|start|submit|activate)"
    r"_[a-z_]+)`"
)


async def test_readme_lists_every_registered_tool_and_no_others(
    mcp_client: Client[Any],
):
    registered = {tool.name for tool in await mcp_client.list_tools()}

    assert set(TOOL_NAME.findall(README.read_text())) == registered


def test_readme_install_commands_run_the_package_console_script():
    match = CONSOLE_SCRIPT.search(PYPROJECT.read_text())
    assert match is not None
    script = match.group(1)
    text = README.read_text()

    assert f'uvx --from "{INSTALL_SOURCE}" {script}' in text
    assert f'"{INSTALL_SOURCE}",\n        "{script}"' in text


def readme_section(heading: str) -> str:
    """Return the text of one `## ` section of the README."""
    text = README.read_text()
    start = text.index(f"## {heading}\n")
    end = text.find("\n## ", start + 1)
    return text[start : end if end != -1 else None]


def test_readme_puts_the_quickstart_before_the_install_reference():
    headings = re.findall(r"^## (.+)$", README.read_text(), re.MULTILINE)

    assert headings[:3] == [
        "Quickstart: reply to customer feedback",
        "Connect it to your tools",
        "Install reference",
    ]


def test_quickstart_builds_the_example_plan_with_no_outside_setup():
    quickstart = readme_section("Quickstart: reply to customer feedback")
    plan = json.loads(FEEDBACK_REPLY_EXAMPLE.read_text())

    assert "customer feedback reply example" in quickstart
    assert "workflows/customer-feedback-reply.plan.json" in quickstart
    for name in [*plan["inputs"], *plan["outputs"]]:
        assert f"`{name}`" in quickstart, name
    for node in plan["nodes"].values():
        assert f"**{node['label']}**" in quickstart, node["label"]
    assert "Secret block" not in quickstart
    assert "deployment" not in quickstart.lower()


def test_connect_section_explains_both_parts_and_links_the_vendor_docs():
    connect = readme_section("Connect it to your tools")

    assert "**A remote MCP server.**" in connect
    assert "**A Secret block.**" in connect
    assert "(https://docs.slack.dev/ai/mcp-server)" in connect
