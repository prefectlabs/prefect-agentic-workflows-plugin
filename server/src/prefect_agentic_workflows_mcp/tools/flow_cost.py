"""A tool that reports how expensive a flow's runs are.

Prefect Cloud doesn't price runs yet, so the tool reports cost in the tokens
that the agent nodes' model calls report. It reads two agent session
analytics routes, filtered to one flow. `run-usage` returns the tokens of each
finished run, as a mean, median, and 95th percentile. `token-usage` sums the
model calls in the range, once by node and once by provider and model.
"""

from asyncio import gather
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any
from uuid import UUID

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import AwareDatetime, Field

from prefect_agentic_workflows_mcp.parameters import WorkspaceId
from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi

RUN_USAGE_PATH = "/agent-sessions/analytics/run-usage"
TOKEN_USAGE_PATH = "/agent-sessions/analytics/token-usage"
DEFAULT_RANGE = timedelta(days=7)
TOKEN_COUNTS = ("input_tokens", "output_tokens", "total_tokens")


def tokens(contributions: dict[str, Any]) -> dict[str, Any]:
    """Return the token counts of a group of model calls, and how many calls
    and whether the counts are complete and final."""
    usage = contributions["usage"]
    return {
        **{count: usage[count]["reported"] for count in TOKEN_COUNTS},
        "model_calls": contributions["observed_call_count"],
        "complete": all(
            usage[count]["coverage"] == "complete" for count in TOKEN_COUNTS
        ),
        "final": all(usage[count]["finality"] == "final" for count in TOKEN_COUNTS),
    }


def breakdowns(token_usage: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the breakdowns of a token-usage response. The filter names one
    flow, so the response has one workflow row, or none when the flow made no
    model calls in the range."""
    return [each for row in token_usage["workflow_rows"] for each in row["breakdowns"]]


def per_run(groups: list[dict[str, Any]]) -> dict[str, Any]:
    """Return the tokens of the flow's finished runs, from its one run-usage
    group, or no runs when the range has none."""
    if not groups:
        return {
            "finished_runs": 0,
            **{count: None for count in TOKEN_COUNTS},
            "complete": True,
            "final": True,
        }
    statistics = groups[0]["statistics"]
    distributions = statistics["tokens_per_run"]

    def summary(distribution: dict[str, Any]) -> dict[str, int] | None:
        # The route returns null statistics only when no run reports the count.
        if distribution["observed_count"] == 0:
            return None
        return {name: round(distribution[name]) for name in ("mean", "median", "p95")}

    return {
        "finished_runs": statistics["eligible_finished_run_count"],
        **{count: summary(distributions[count]) for count in TOKEN_COUNTS},
        "complete": all(
            distributions[count]["coverage"]["status"] == "complete"
            for count in TOKEN_COUNTS
        ),
        "final": not any(distributions[count]["provisional"] for count in TOKEN_COUNTS),
    }


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the flow cost tool to `mcp`."""

    @tool(mcp, read_only=True)
    async def get_flow_cost(
        flow_id: Annotated[
            UUID,
            Field(description="ID of the flow, from `get_flow`."),
        ],
        start: Annotated[
            AwareDatetime | None,
            Field(
                description=(
                    "Start of the time range, with a UTC offset. Omit it for "
                    "the 7 days before `end`."
                ),
            ),
        ] = None,
        end: Annotated[
            AwareDatetime | None,
            Field(
                description="End of the time range, with a UTC offset. Omit it for now."
            ),
        ] = None,
        version_id: Annotated[
            UUID | None,
            Field(
                description=(
                    "Only runs of this plan version, from `list_plan_versions`. "
                    "Omit it for runs of every version."
                ),
            ),
        ] = None,
        workspace_id: WorkspaceId = None,
    ) -> dict[str, Any]:
        """Report how expensive a flow's runs are.

        Use it when the user asks what a workflow costs, whether it is
        expensive or inefficient, or which step uses the most. Prefect Cloud
        doesn't price runs yet, so the result reports cost in the tokens that
        the agent nodes' model calls report. Tell the user that dollar
        amounts aren't available yet.

        `per_run` says what one run costs. It covers the finished runs that
        started in the range, with the `mean`, `median`, and `p95` of each
        run's `input_tokens`, `output_tokens`, and `total_tokens`. A run that
        hasn't finished isn't in `per_run` until it finishes. A token count
        is null when none of the runs reports it.

        `total` sums the model calls that started in the range, including
        those of runs that haven't finished, with the number of `runs`. `nodes`
        splits the total by agent node, and `models` splits it by provider
        and model, the most total tokens first. A node's `invocations` counts
        its activations that made a model call, so its tokens divided by its
        invocations is what the node uses each time it runs.

        `complete` is false when some model calls didn't report a token
        count, so the sums are too low. `final` is false while a counted run
        can still make model calls or its calls can still arrive. `counted_range`
        is the part of the range that the workspace's retention keeps, and
        the counts leave out anything earlier. It is null when the whole
        range is older than the retention.
        """
        range_end = end or datetime.now(UTC)
        range_start = start or range_end - DEFAULT_RANGE
        if range_end <= range_start:
            raise ToolError("`end` must be after `start`.")
        query: dict[str, Any] = {
            "range": {
                "start": range_start.isoformat(),
                "end": range_end.isoformat(),
            },
            "filters": {
                "workflow_ids": [str(flow_id)],
                **({"workflow_version_ids": [str(version_id)]} if version_id else {}),
            },
        }

        async def token_usage(breakdown: str) -> dict[str, Any]:
            return await api.call(
                "POST",
                TOKEN_USAGE_PATH,
                json={**query, "breakdown_by": [breakdown]},
                workspace_id=workspace_id,
            )

        runs, by_node, by_model = await gather(
            api.call("POST", RUN_USAGE_PATH, json=query, workspace_id=workspace_id),
            token_usage("authored_node_id"),
            token_usage("provider_model"),
        )
        return {
            "range": by_node["context"]["requested_range"],
            "counted_range": by_node["context"]["effective_range"],
            "per_run": per_run(runs["groups"]),
            "total": {
                **tokens(by_node["workspace_contributions"]),
                "runs": by_node["workspace_contributions"]["contributing_run_count"],
            },
            "nodes": [
                {
                    "node": breakdown["group"]["authored_node_id"],
                    **tokens(breakdown["contributions"]),
                    "invocations": breakdown["contributions"][
                        "contributing_invocation_count"
                    ],
                }
                for breakdown in breakdowns(by_node)
            ],
            "models": [
                {
                    "provider": breakdown["group"]["provider"],
                    "model": breakdown["group"]["model"],
                    **tokens(breakdown["contributions"]),
                }
                for breakdown in breakdowns(by_model)
            ],
        }
