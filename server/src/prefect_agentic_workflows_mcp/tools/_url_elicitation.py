"""Ask the MCP client to open a Prefect Cloud page for the user.

Tools use this when the user enters something in Prefect Cloud that the agent
must not see, such as a credential. The module name starts with `_`, so
`build_server` doesn't register it as a tool group.

The 2026-07-28 protocol has no server-to-client requests mid-call: the tool
returns the ask, and the client calls the tool again with the answer and the
request state. Older protocols ask during the call.
"""

import uuid

from fastmcp import Context
from mcp.types import (
    ElicitRequest,
    ElicitRequestURLParams,
    ElicitResult,
    InputRequiredResult,
)
from mcp_types.version import MODERN_PROTOCOL_VERSIONS


def client_can_open_url(ctx: Context) -> bool:
    """Return whether the client supports URL elicitation."""
    capabilities = ctx.session.client_capabilities
    return (
        capabilities is not None
        and capabilities.elicitation is not None
        and capabilities.elicitation.url is not None
    )


async def ask_client_to_open_url(
    ctx: Context, *, key: str, message: str, url: str, request_state: str
) -> InputRequiredResult | bool:
    """Ask the client to open `url`, and wait for the user when the protocol allows.

    On the 2026-07-28 protocol, returns the `InputRequiredResult` the tool
    must return. The client then calls the tool again with `request_state`,
    and `client_opened_url` reads the answer. On older protocols, returns
    whether the user accepted.
    """
    elicitation_id = str(uuid.uuid4())
    if ctx.session.protocol_version in MODERN_PROTOCOL_VERSIONS:
        return InputRequiredResult(
            input_requests={
                key: ElicitRequest(
                    params=ElicitRequestURLParams(
                        message=message, url=url, elicitation_id=elicitation_id
                    )
                )
            },
            request_state=request_state,
        )
    answer = await ctx.session.elicit_url(
        message=message,
        url=url,
        elicitation_id=elicitation_id,
        related_request_id=ctx.request_id,
    )
    return accepted(answer)


def client_opened_url(ctx: Context, key: str) -> bool:
    """Return whether the user accepted the page asked for under `key`.

    Use it when the client calls the tool again with the answer.
    """
    return accepted((ctx.input_responses or {}).get(key))


def accepted(answer: object) -> bool:
    return isinstance(answer, ElicitResult) and answer.action == "accept"
