"""Tool parameter types that more than one tool group uses."""

import json
from pathlib import Path
from typing import Annotated, Any

from fastmcp.exceptions import ToolError
from pydantic import Field

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
            "JSON, such as the plan file you wrote. Prefer this over `plan` for "
            "a plan saved on disk. Pass either `plan` or `plan_path`."
        ),
    ),
]


def load_plan(plan: dict[str, Any] | None, plan_path: str | None) -> dict[str, Any]:
    """Return the plan from exactly one of `plan` and `plan_path`.

    Raises `ToolError` with a message the agent can act on when both or
    neither are given, or when the file can't be read as a JSON object.
    """
    if (plan is None) == (plan_path is None):
        raise ToolError("Pass exactly one of `plan` and `plan_path`.")
    if plan is not None:
        return plan
    path = Path(str(plan_path))
    if not path.is_absolute():
        raise ToolError(f"`plan_path` must be an absolute path, not {plan_path!r}.")
    try:
        loaded = json.loads(path.read_text())
    except OSError as exc:
        raise ToolError(f"Could not read the plan file {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ToolError(f"The plan file {path} is not valid JSON: {exc}") from exc
    if not isinstance(loaded, dict):
        raise ToolError(f"The plan file {path} must hold a JSON object.")
    return loaded
