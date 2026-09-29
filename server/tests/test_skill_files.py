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

from evals import fake_cloud, graph

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


def test_example_plans_pass_the_offline_graph_checks():
    for path in example_plans():
        plan = json.loads(path.read_text())

        assert fake_cloud.validate(plan) == [], path.name


def test_feedback_reply_example_needs_no_tools_and_revises_a_rejected_draft():
    plan = json.loads(
        (
            SKILL_DIR / "references" / "examples" / "customer-feedback-reply.plan.json"
        ).read_text()
    )
    nodes = graph.nodes(plan)

    # The README quickstart runs this plan with no MCP server, Secret block,
    # or deployment.
    assert {node["kind"] for node in nodes.values()} == {"AgentNode", "HumanInputNode"}
    assert not any("mcp" in node for node in nodes.values())
    assert "$ref" not in json.dumps(plan)

    assert graph.find_cycle(plan) == []
    assert ("review_reply", "rejected", "finish_reply") in graph.node_edges(plan)
    reply_sources = plan["outputs"]["reply"]["fields"]["reply"]["source"]["one_of"]
    assert {"node": "finish_reply", "output": "revised_reply"} in [
        {"node": source["node"], "output": source["output"]} for source in reply_sources
    ]
    assert set(plan["inputs"]) == {"feedback"}
    assert set(plan["outputs"]) == {"reply", "category"}
