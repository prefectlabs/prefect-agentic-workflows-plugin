"""Tool parameter types that more than one tool group uses."""

from typing import Annotated, Any

from pydantic import Field

PlanDocument = Annotated[
    dict[str, Any],
    Field(
        description=(
            "Execution-plan document as a JSON object, written against the "
            "schema from `get_schema`. Don't include `layout`."
        ),
    ),
]
