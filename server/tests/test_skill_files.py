"""Checks that the agentic-workflows skill matches the server it drives.

The skill is Markdown in `skills/` at the repo root. These tests read it as
text and fail when it names a tool the server doesn't register or links to a
file that doesn't exist.
"""

import re
from pathlib import Path
from typing import Any

from fastmcp import Client

SKILL_DIR = Path(__file__).resolve().parents[2] / "skills" / "agentic-workflows"

# A backticked name that starts with one of these verbs is read as a tool name.
TOOL_NAME = re.compile(
    r"`((?:get|list|create|update|delete|validate|publish|start|submit|activate)"
    r"_[a-z_]+)`"
)
MARKDOWN_LINK = re.compile(r"\]\(([^)#]+)\)")


def skill_files() -> list[Path]:
    files = sorted(SKILL_DIR.rglob("*.md"))
    assert files, f"no skill files under {SKILL_DIR}"
    return files


async def test_skill_names_only_registered_tools(mcp_client: Client[Any]):
    registered = {tool.name for tool in await mcp_client.list_tools()}

    unknown = {
        f"{path.relative_to(SKILL_DIR)}: {name}"
        for path in skill_files()
        for name in TOOL_NAME.findall(path.read_text())
        if name not in registered
    }

    assert not unknown


def test_skill_links_point_at_existing_files():
    missing = [
        f"{path.relative_to(SKILL_DIR)}: {target}"
        for path in skill_files()
        for target in MARKDOWN_LINK.findall(path.read_text())
        if "://" not in target and not (path.parent / target).exists()
    ]

    assert not missing


def test_platform_limits_say_when_they_were_last_verified():
    text = (SKILL_DIR / "references" / "platform-limits.md").read_text()

    assert re.search(r"^Last verified: \d{4}-\d{2}-\d{2}", text, re.MULTILINE)
