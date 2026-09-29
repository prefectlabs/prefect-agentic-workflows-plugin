"""Checks that the repository README matches the server it documents.

The README at the repo root holds the quickstart, the install commands, and the
tool list. These tests fail when it names a tool the server doesn't register,
leaves out a tool the server does register, or runs a console script that the
package doesn't define.
"""

import re
from pathlib import Path
from typing import Any

from fastmcp import Client

REPO_ROOT = Path(__file__).resolve().parents[2]
README = REPO_ROOT / "README.md"
PYPROJECT = REPO_ROOT / "server" / "pyproject.toml"
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
