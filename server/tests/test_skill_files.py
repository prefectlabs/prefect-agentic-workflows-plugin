"""Checks that the agentic-workflows skill matches the server it drives.

The skill is Markdown in `skills/` at the repo root. These tests read it as
text and fail when it names a tool the server doesn't register or links to a
file that doesn't exist. They also check the example plans offline. The
integration suite validates the example plans against Cloud.
"""

import json
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


def test_skill_routes_conversions_to_the_conversion_guide():
    text = (SKILL_DIR / "SKILL.md").read_text()

    assert "(references/conversion.md)" in text


def test_conversion_guide_proposes_a_substitute_for_each_unsupported_part():
    text = (SKILL_DIR / "references" / "conversion.md").read_text()

    missing = [
        phrase
        for phrase in [
            "remote MCP server",
            "a Deployment node passes on only the child run's ID and state",
            "Streamable-HTTP MCP server",
            "A fixed number of steps",
            "human checkpoint",
            "not yet supported",
            "60 seconds",
        ]
        if phrase not in text
    ]

    assert not missing


def test_conversion_guide_handles_every_input_form():
    text = (SKILL_DIR / "references" / "conversion.md").read_text()

    missing = [
        form
        for form in [
            "A path to a `SKILL.md` file",
            "A path to a directory",
            "A skill name",
            "Pasted text",
        ]
        if form not in text
    ]

    assert not missing


def test_conversion_guide_follows_symlinked_skill_directories():
    # `npx skills add` installs a skill as a symlink, and `find` doesn't
    # follow a symlinked starting path without -L.
    text = (SKILL_DIR / "references" / "conversion.md").read_text()

    assert re.findall(r"\bfind (?!-L )[.~<]", text) == []


def test_conversion_guide_treats_an_edit_round_as_a_loop():
    text = (SKILL_DIR / "references" / "conversion.md").read_text()
    approval_row = next(
        line for line in text.splitlines() if line.startswith("| A human approval")
    )

    assert "loop" in approval_row


def test_skill_carries_the_step_map_into_the_summary_and_report():
    text = (SKILL_DIR / "SKILL.md").read_text()
    summary = next(line for line in text.splitlines() if "**Summary.**" in line)
    report = next(line for line in text.splitlines() if "**Report.**" in line)

    assert "step map" in summary
    assert "step map" in report


def example_plans() -> list[Path]:
    examples = sorted((SKILL_DIR / "references" / "examples").glob("*.plan.json"))
    assert examples, "the skill has no example plans"
    return examples


def test_example_plans_are_linked_from_the_examples_reference():
    text = (SKILL_DIR / "references" / "example-plans.md").read_text()
    linked = set(MARKDOWN_LINK.findall(text))

    unlinked = [
        path.name for path in example_plans() if f"examples/{path.name}" not in linked
    ]

    assert not unlinked


def test_example_plans_are_plan_documents_without_a_layout():
    for path in example_plans():
        plan = json.loads(path.read_text())

        assert plan["kind"] == "ExecutionPlan", path.name
        assert "layout" not in plan, path.name
