"""Tool parameter types that more than one tool group uses."""

import json
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

from fastmcp.exceptions import ToolError
from fastmcp.server.dependencies import get_http_request
from pydantic import Field

FlowRunId = Annotated[
    UUID,
    Field(description="ID of the run, from `start_run` or `list_runs`."),
]

WorkspaceId = Annotated[
    UUID | None,
    Field(
        description=(
            "A workspace UUID from your Prefect Cloud OAuth consent grant. "
            "Omit when the grant includes exactly one workspace. With several, "
            "call get_workspace, ask which workspace to use, and pass its UUID "
            "to each tool. Requires OAuth authentication."
        )
    ),
]

PlanDocument = Annotated[
    dict[str, Any] | None,
    Field(
        description=(
            "Execution-plan document as a JSON object, written against the "
            "schema from `get_schema`. Don't include `layout`. Pass either "
            "`plan` or `plan_path`."
        ),
    ),
]
PlanPath = Annotated[
    str | None,
    Field(
        description=(
            "Absolute path to a file that holds the execution-plan document as "
            "JSON, such as the plan file you wrote. Only a server running on "
            "your machine over stdio can read it. A hosted server rejects it, "
            "so pass the document in `plan` instead. Pass either `plan` or "
            "`plan_path`."
        ),
    ),
]


# A plan document is a few kilobytes. The cap keeps a wrong path from reading a
# large file into memory.
MAX_PLAN_FILE_BYTES = 5_000_000


def _serving_http_request() -> bool:
    try:
        get_http_request()
    except RuntimeError:
        return False
    return True


def load_plan(plan: dict[str, Any] | None, plan_path: str | None) -> dict[str, Any]:
    """Return the plan from exactly one of `plan` and `plan_path`.

    Raises `ToolError` with a message the agent can act on when both or
    neither are given, or when the file can't be read as a JSON object.
    """
    if (plan is None) == (plan_path is None):
        raise ToolError("Pass exactly one of `plan` and `plan_path`.")
    if plan is not None:
        return plan
    # Over HTTP the path names a file on the server, not on the client's machine,
    # and a client must not be able to read the server's files.
    if _serving_http_request():
        raise ToolError(
            "This server runs over HTTP and can't read files on your machine. "
            "Pass the plan document in `plan` instead of `plan_path`."
        )
    path = Path(str(plan_path))
    if not path.is_absolute():
        raise ToolError(f"`plan_path` must be an absolute path, not {plan_path!r}.")
    if not path.is_file():
        raise ToolError(f"The plan file {path} is not a regular file.")
    if path.stat().st_size > MAX_PLAN_FILE_BYTES:
        raise ToolError(
            f"The plan file {path} is larger than {MAX_PLAN_FILE_BYTES} bytes."
        )
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError) as exc:
        raise ToolError(f"Could not read the plan file {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ToolError(f"The plan file {path} is not valid JSON: {exc}") from exc
    if not isinstance(loaded, dict):
        raise ToolError(f"The plan file {path} must hold a JSON object.")
    return loaded
