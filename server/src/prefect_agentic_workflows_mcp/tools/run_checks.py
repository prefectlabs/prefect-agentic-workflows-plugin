"""A tool that reads how Prefect Cloud checked a run's expectations.

Prefect Cloud checks each expectation on the outputs that an agent node's
attempt selected, and holds the outputs until every check ends. The agent
session routes serve one check for each expectation of each such attempt.
"""

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any
from uuid import UUID

from fastmcp import FastMCP
from pydantic import Field

from prefect_agentic_workflows_mcp.parameters import FlowRunId, WorkspaceId
from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi

CHECKS_PATH = "/agent-sessions/checks/filter"
# The checks route returns at most 100 checks in one page.
PAGE_LIMIT = 100
# The most checks that `get_run_checks` returns from one run.
MAX_CHECKS_READ = 500
# The route lists the checks of attempts that started in a time range. The
# range ends this long after now, so a clock that runs behind Prefect Cloud's
# doesn't leave out the latest attempt.
RANGE_MARGIN = timedelta(minutes=5)


def check_entry(check: dict[str, Any]) -> dict[str, Any]:
    """Return one check without the IDs that the run already gives."""
    return {
        "node": check["authored_node_id"],
        "activation_id": check["activation_id"],
        "attempt_id": check["attempt_id"],
        "attempt_number": check["attempt_number"],
        "output": check["output"],
        "expectation_key": check["expectation_key"],
        "expectation_text": check["expectation_text"],
        "verdict": check["verdict"],
        "reason": check["reason"],
        "check_error": check["check_error"],
        "content": check["content"],
        "ended_at": check["ended_at"],
    }


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the run check tool to `mcp`."""

    @tool(mcp, read_only=True)
    async def get_run_checks(
        flow_run_id: FlowRunId,
        activation_id: Annotated[
            UUID | None,
            Field(
                description=(
                    "ID of one node activation, from a node's `activation_id` "
                    "in `get_run`. Omit it to read the checks of every node."
                ),
            ),
        ] = None,
        workspace_id: WorkspaceId = None,
    ) -> dict[str, Any]:
        """Read how Prefect Cloud checked the expectations of a run's agent nodes.

        Use it when a node failed with `expectation_violated`,
        `expectation_inconclusive`, or `verification_unavailable`, to say
        which expectation wasn't met and why.

        Returns `checks`, one for each expectation of each attempt whose
        outputs Prefect Cloud checked, oldest attempt first. A check names
        its `node`, `output`, and `expectation_key`, and has the
        `expectation_text` as the check read it. `attempt_number` counts the
        node's checked attempts from 1, so a node that ran again after a
        missed expectation has checks for each attempt, and the highest
        number decided the node's outcome.

        A check ends with a `verdict` or a `check_error`. `verdict` is
        `satisfied`, `violated`, or `inconclusive`, which means the agent's
        session didn't show whether the expectation was met. A `violated` or
        `inconclusive` check can have a `reason`, one sentence that says what
        the check found. `check_error` is set when Prefect Cloud couldn't
        finish the check, with a `cause` and a `detail`.

        `content` says whether the reason is there: `available`, `none`
        when the check has no reason, `withheld` when your role can't view
        agent content, or `unavailable` when Prefect Cloud can no longer
        read it.

        A check is listed a moment after it ends, so an empty `checks` for a
        node that just failed can fill in on the next call. `checks_unread`
        is true when the run has more checks than this tool returns.
        """
        flow_run = await api.call(
            "GET", f"/flow_runs/{flow_run_id}", workspace_id=workspace_id
        )
        body: dict[str, Any] = {
            "range": {
                "start": flow_run["created"],
                "end": (datetime.now(UTC) + RANGE_MARGIN).isoformat(),
            },
            "filters": {"workflow_run_ids": [str(flow_run_id)]},
            "limit": PAGE_LIMIT,
        }
        checks: list[dict[str, Any]] = []
        unread = False
        while True:
            page = await api.call(
                "POST", CHECKS_PATH, json=body, workspace_id=workspace_id
            )
            # The route has no activation filter, so the limit counts only
            # the checks of the activation that was asked for.
            checks.extend(
                check
                for check in page["checks"]
                if activation_id is None or check["activation_id"] == str(activation_id)
            )
            unread = page["next_cursor"] is not None
            if not unread or len(checks) >= MAX_CHECKS_READ:
                break
            body = {**body, "cursor": page["next_cursor"]}
        return {
            "checks": [check_entry(check) for check in checks],
            "checks_unread": unread,
        }
