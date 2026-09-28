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


def skill_section(heading: str) -> str:
    """Return the text of one `## ` section of `SKILL.md`."""
    text = (SKILL_DIR / "SKILL.md").read_text()
    start = text.index(f"## {heading}\n")
    end = text.find("\n## ", start + 1)
    return text[start : end if end != -1 else None]


def skill_line(marker: str) -> str:
    """Return the first line of `SKILL.md` that contains `marker`."""
    lines = (SKILL_DIR / "SKILL.md").read_text().splitlines()
    return next(line for line in lines if marker in line)


def test_every_tool_that_runs_activates_or_schedules_has_one_approval_point():
    points = [
        line
        for line in skill_section("Approval points").splitlines()
        if re.match(r"\d\. \*\*", line)
    ]
    assert len(points) == 4
    design, external_effects, promotion, recurring_runs = points

    def points_naming(tool: str) -> list[str]:
        return [point for point in points if f"`{tool}`" in point]

    assert points_naming("start_run") == [external_effects]
    for tool in ["create_schedule", "update_schedule", "delete_schedule"]:
        assert points_naming(tool) == [recurring_runs], tool
    # Activation splits by the flow's state, so each call still has one point:
    # design when the flow has no active version, promotion when it has one.
    for tool in ["publish_plan", "activate_plan_version"]:
        assert points_naming(tool) == [design, promotion], tool
    assert "no active version" in design
    assert "already has an active version" in promotion
    assert "`list_schedules`" in promotion


def test_a_conversion_has_one_design_approval():
    summary = skill_line("**Summary.**")
    conversion = (SKILL_DIR / "references" / "conversion.md").read_text()

    assert "conversion report" in summary
    assert "design approval" in summary
    assert "Stop for the user's decisions" not in conversion
    assert "one design approval" in conversion


def test_checklist_separates_run_inputs_from_fixed_instructions():
    row = skill_line("| Inputs:")

    assert "Hardcode" not in row
    assert "plan input" in row
    assert "objective" in row


def test_starting_a_run_keeps_one_idempotency_key_per_run():
    text = skill_section("Starting a run")

    assert "idempotency key" in text
    assert "same key" in text
    assert "`created` false" in text


def test_skill_says_a_run_uses_the_active_version():
    text = skill_section("Starting a run")
    test_run = skill_line("**Test run.**")
    report = skill_line("**Report.**")

    assert "A run always uses the flow's active version." in text
    assert "active version" in test_run
    assert "execution_plan_version_id" in report
    assert "`list_plan_versions`" in report


def test_input_values_come_before_the_external_effects_approval():
    steps = skill_section("Starting a run")

    assert steps.index("Ask for any input values") < steps.index(
        "external-effects approval"
    )


def test_a_failed_activation_retry_says_whether_it_needs_approval():
    publish = skill_line("**Publish.**")

    assert "activation_error" in publish
    assert "same approval" in publish


def test_every_entry_path_runs_the_infrastructure_check_first():
    entry_paths = skill_section("Pick the entry path")
    conversion = (SKILL_DIR / "references" / "conversion.md").read_text()

    assert "(references/infrastructure-check.md)" in entry_paths
    for marker in ["**The user hands you", "**The user describes", "**The user gives"]:
        line = skill_line(marker)
        assert "infrastructure check" in line, marker
    describes = skill_line("**The user describes")
    assert describes.index("infrastructure check") < describes.index("checklist")
    interview = skill_line("**The user gives")
    assert interview.index("infrastructure check") < interview.index("interview:")
    assert conversion.index("## 4. Run the infrastructure check") < conversion.index(
        "## 5. Write the conversion report"
    )


def test_checklist_leaves_remote_mcp_servers_to_the_infrastructure_check():
    row = skill_line("| Tools and systems")

    assert "Ask which remote MCP servers exist" not in row
    assert "infrastructure check" in row


def test_infrastructure_check_explains_mcp_and_what_its_absence_limits():
    text = (SKILL_DIR / "references" / "infrastructure-check.md").read_text()

    assert "`list_secret_blocks`" in text
    assert "`list_deployments`" in text
    assert "a web address that lets an agent use one of your business tools" in text
    assert "**No reachable systems at all.**" in text
    assert "human approval step" in text
    assert "leaving the step out of the first version" in text
    assert "needs a new service" in text
