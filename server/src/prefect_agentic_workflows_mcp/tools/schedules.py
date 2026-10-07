"""Tools that manage the schedules of a flow's execution plan.

A schedule belongs to a flow, not to a plan version. Each time a schedule
fires, Prefect Cloud starts a run of the version that is active at that time
and checks the schedule's parameters against that version first.
"""

from typing import Annotated, Any, Literal
from uuid import UUID

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, ConfigDict, Field

from prefect_agentic_workflows_mcp.tools import tool
from prefect_agentic_workflows_mcp.workspace_api import WorkspaceApi


class CronSchedule(BaseModel):
    """A schedule that fires on a cron expression."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["cron"]
    cron: str = Field(description="Cron expression, for example '0 9 * * 1-5'.")
    timezone: str | None = Field(
        default=None,
        description=(
            "IANA time zone for the expression, for example "
            "'America/New_York'. Defaults to UTC."
        ),
    )
    day_or: bool | None = Field(
        default=None,
        description=(
            "How to combine the day-of-month and day-of-week fields. True, the "
            "default, fires when either matches. False fires only when both match."
        ),
    )


class IntervalSchedule(BaseModel):
    """A schedule that fires at a fixed interval."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["interval"]
    interval: float = Field(
        gt=0, description="Seconds between runs, for example 3600 for hourly."
    )
    anchor_date: str | None = Field(
        default=None,
        description=(
            "ISO 8601 date and time that the intervals count from. Defaults to "
            "the time the schedule is created."
        ),
    )
    timezone: str | None = Field(
        default=None,
        description="IANA time zone for the anchor date. Defaults to UTC.",
    )


class RRuleSchedule(BaseModel):
    """A schedule that fires on an iCalendar recurrence rule."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["rrule"]
    rrule: str = Field(
        description="iCalendar RRULE string, for example 'FREQ=WEEKLY;BYDAY=MO'."
    )
    timezone: str | None = Field(
        default=None,
        description="IANA time zone for the rule. Defaults to UTC.",
    )


ScheduleSpec = Annotated[
    CronSchedule | IntervalSchedule | RRuleSchedule,
    Field(
        discriminator="type",
        description=(
            "When the schedule fires. Set `type` to 'cron', 'interval', or "
            "'rrule', and set the field of the same name."
        ),
    ),
]

FlowId = Annotated[UUID, Field(description="ID of the flow that owns the schedule.")]
ScheduleId = Annotated[UUID, Field(description="ID of the schedule.")]
ScheduleName = Annotated[str, Field(description="Name of the schedule.")]
ScheduleParameters = Annotated[
    dict[str, Any],
    Field(
        description=(
            "Parameters for each scheduled run. Prefect Cloud checks them "
            "against the active plan's parameters when you create or update "
            "the schedule and again each time it fires."
        ),
    ),
]


NO_CHANGES_MESSAGE = (
    "Pass at least one of `name`, `active`, `schedule`, or `parameters` to "
    "update a schedule."
)


def schedules_path(flow_id: UUID) -> str:
    """Return the workspace API path of a flow's schedules."""
    return f"/flows/{flow_id}/execution-plan/schedules"


def register(mcp: FastMCP[Any], api: WorkspaceApi) -> None:
    """Add the schedule tools to `mcp`."""

    @tool(mcp, read_only=False)
    async def create_schedule(
        flow_id: FlowId,
        name: ScheduleName,
        schedule: ScheduleSpec,
        active: Annotated[
            bool, Field(description="Whether the schedule starts runs.")
        ] = True,
        parameters: ScheduleParameters | None = None,
    ) -> dict[str, Any]:
        """Create a schedule that starts runs of a flow's execution plan.

        The flow must have an active plan version. Each run uses the version
        that is active when the schedule fires. The result includes
        `next_scheduled_time`, and after the schedule fires, the ID of the
        last run it started in `last_created_flow_run_id` and any failure to
        start a run in `last_error`. Ask the user to confirm before you call
        this tool.
        """
        return await api.call(
            "POST",
            schedules_path(flow_id),
            json={
                "name": name,
                "active": active,
                "schedule": schedule.model_dump(mode="json", exclude_none=True),
                "parameters": parameters or {},
            },
        )

    @tool(mcp, read_only=True)
    async def list_schedules(flow_id: FlowId) -> dict[str, Any]:
        """List the schedules of a flow's execution plan.

        Returns `schedules`, a list in the same shape `get_schedule` returns.
        """
        return await api.call("GET", schedules_path(flow_id))

    @tool(mcp, read_only=True)
    async def get_schedule(flow_id: FlowId, schedule_id: ScheduleId) -> dict[str, Any]:
        """Read one schedule of a flow's execution plan.

        The result includes `active`, `schedule`, `parameters`,
        `next_scheduled_time`, the ID of the last run the schedule started in
        `last_created_flow_run_id`, and the last failure to start a run in
        `last_error`.
        """
        return await api.call("GET", f"{schedules_path(flow_id)}/{schedule_id}")

    @tool(mcp, read_only=False, destructive=True)
    async def update_schedule(
        flow_id: FlowId,
        schedule_id: ScheduleId,
        name: Annotated[
            str | None, Field(description="New name. Omit it to keep the name.")
        ] = None,
        active: Annotated[
            bool | None,
            Field(
                description=(
                    "False pauses the schedule and true resumes it. Omit it to "
                    "keep the current state."
                ),
            ),
        ] = None,
        schedule: Annotated[
            CronSchedule | IntervalSchedule | RRuleSchedule | None,
            Field(
                discriminator="type",
                description=(
                    "New timing, in the same shape `create_schedule` takes. "
                    "Omit it to keep the timing."
                ),
            ),
        ] = None,
        parameters: Annotated[
            dict[str, Any] | None,
            Field(
                description=(
                    "New parameters. They replace all of the current "
                    "parameters. Omit them to keep the current parameters."
                ),
            ),
        ] = None,
    ) -> dict[str, Any]:
        """Change a schedule of a flow's execution plan.

        Only the arguments you pass are changed. Pass at least one of `name`,
        `active`, `schedule`, or `parameters`. The flow must have an active
        plan version. Returns the updated schedule. Ask the user to confirm
        before you call this tool.
        """
        changes: dict[str, Any] = {}
        if name is not None:
            changes["name"] = name
        if active is not None:
            changes["active"] = active
        if schedule is not None:
            changes["schedule"] = schedule.model_dump(mode="json", exclude_none=True)
        if parameters is not None:
            changes["parameters"] = parameters
        if not changes:
            raise ToolError(NO_CHANGES_MESSAGE)
        return await api.call(
            "PATCH", f"{schedules_path(flow_id)}/{schedule_id}", json=changes
        )

    @tool(mcp, read_only=False, destructive=True)
    async def delete_schedule(
        flow_id: FlowId, schedule_id: ScheduleId
    ) -> dict[str, Any]:
        """Delete a schedule of a flow's execution plan.

        The schedule starts no more runs. Runs it already started are not
        changed. Ask the user to confirm before you call this tool.
        """
        await api.call("DELETE", f"{schedules_path(flow_id)}/{schedule_id}")
        return {
            "deleted": True,
            "flow_id": str(flow_id),
            "schedule_id": str(schedule_id),
        }
