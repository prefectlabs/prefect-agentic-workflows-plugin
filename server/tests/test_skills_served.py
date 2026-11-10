"""Checks that the server serves the agentic-workflows skill over MCP."""

import json
from importlib import resources
from pathlib import Path
from typing import Any

from fastmcp import Client
from fastmcp.skills import SkillsClientExtension
from mcp.types import TextResourceContents

import prefect_agentic_workflows_mcp
from prefect_agentic_workflows_mcp.server import SKILL_NAME, build_server

SKILL_URI = f"skill://{SKILL_NAME}/SKILL.md"
REPO_ROOT = Path(__file__).resolve().parents[2]


def packaged_files() -> dict[str, bytes]:
    """Return the skill's files as shipped in the package, keyed by relative path."""
    root = resources.files(prefect_agentic_workflows_mcp) / "skills" / SKILL_NAME
    with resources.as_file(root) as directory:
        return {
            path.relative_to(directory).as_posix(): path.read_bytes()
            for path in sorted(directory.rglob("*"))
            if path.is_file()
        }


def as_bytes(content: Any) -> bytes:
    return content.encode() if isinstance(content, str) else bytes(content)


async def test_server_lists_the_skill(cloud_api: Any) -> None:
    async with Client(build_server(), extensions=[SkillsClientExtension()]) as client:
        skills = await client.list_skills()

    assert [str(skill.uri) for skill in skills] == [SKILL_URI]
    assert skills[0].frontmatter.name == SKILL_NAME
    assert skills[0].frontmatter.description


async def test_served_skill_matches_the_packaged_files(cloud_api: Any) -> None:
    async with Client(build_server(), extensions=[SkillsClientExtension()]) as client:
        skill = await client.read_skill(SKILL_URI)

    served = {path: as_bytes(content) for path, content in skill.files.items()}
    assert served == packaged_files()


async def test_client_without_the_extension_can_read_the_manifest(
    mcp_client: Client[Any],
) -> None:
    contents = await mcp_client.read_resource(f"skill://{SKILL_NAME}/_manifest")

    assert isinstance(contents[0], TextResourceContents)
    manifest = json.loads(contents[0].text)
    assert manifest["skill"] == SKILL_NAME
    assert {entry["path"] for entry in manifest["files"]} == set(packaged_files())


def test_repo_skill_directory_is_the_packaged_skill() -> None:
    """`npx skills add` reads `skills/` at the repo root, which links to the package."""
    repo_skill = REPO_ROOT / "skills" / SKILL_NAME
    packaged = Path(prefect_agentic_workflows_mcp.__file__).parent / "skills"

    assert repo_skill.resolve() == (packaged / SKILL_NAME).resolve()
