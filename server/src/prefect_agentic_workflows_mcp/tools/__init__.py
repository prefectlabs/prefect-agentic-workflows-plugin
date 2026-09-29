"""MCP tool groups.

Each module in this package is one tool group. A module defines
`register(mcp, api)`, which adds the group's tools to the server, and
`build_server` calls it for every module it finds here, except modules whose
names start with `_`. Adding a tool group means adding a module. No other
file changes.

Tools are added with `tool`, which sets the MCP annotations every tool must
have and marks the tool as alpha in its description.
"""

from collections.abc import Callable
from typing import Any, TypeVar

from fastmcp import FastMCP
from mcp.types import ToolAnnotations

F = TypeVar("F", bound=Callable[..., Any])

ALPHA_NOTICE = (
    "This tool is alpha. Its name, arguments, and result can change in any release."
)


def tool(
    mcp: FastMCP[Any],
    *,
    read_only: bool,
    destructive: bool = False,
) -> Callable[[F], F]:
    """Return a decorator that adds a function to `mcp` as an annotated tool.

    The function's docstring is the tool description, followed by the alpha
    notice. `read_only` sets `readOnlyHint`. `destructive` sets
    `destructiveHint` and is only valid for tools that write.
    """
    if read_only and destructive:
        raise ValueError("A read-only tool can't be destructive.")

    def decorator(fn: F) -> F:
        description = f"{(fn.__doc__ or '').strip()}\n\n{ALPHA_NOTICE}"
        mcp.tool(
            fn,
            description=description,
            annotations=ToolAnnotations(
                readOnlyHint=read_only,
                destructiveHint=destructive,
                idempotentHint=read_only,
                openWorldHint=True,
            ),
        )
        return fn

    return decorator
