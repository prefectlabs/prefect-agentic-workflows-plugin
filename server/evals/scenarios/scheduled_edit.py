"""Change a workflow that already has an active version and a schedule.

Before the agent starts, the harness creates the `weekly-digest` flow in the
sandbox, with an active plan version and one schedule, `monday-digest`. The
schedule is inactive, so it never starts a run. The user asks for a change
to the plan, and the plan file isn't in the working directory. The user turns
down the test run.

Expected results:

- the agent calls `list_schedules` and names `monday-digest` before it asks to
  activate the new version
- the new version is activated only after the user's yes
- the schedule is not changed, and no run is started
"""

import json
from typing import Any

from evals import assertions
from evals.assertions import Check
from evals.record import FlowState
from evals.sandbox import SandboxApi
from evals.scenario import (
    DECLINE_TEST_RUN,
    FLOW_PREFIX,
    Outcome,
    Rule,
    Scenario,
    ScenarioInputs,
)

FLOW_NAME = "weekly-digest"
SCHEDULE_NAME = "monday-digest"
SCHEDULE = {"type": "cron", "cron": "0 9 * * 1", "timezone": "America/New_York"}
SCHEDULE_PARAMETERS = {"team": "support"}
SCHEDULE_TOOLS = ["create_schedule", "update_schedule", "delete_schedule"]

TEXT = {"type": "string"}
DIGEST = {
    "type": "object",
    "properties": {"digest": TEXT},
    "required": ["digest"],
}
PLAN: dict[str, Any] = {
    "kind": "ExecutionPlan",
    "schema_version": "0.2",
    "inputs": {
        "team": {
            "schema": {
                "type": "string",
                "description": "The team the digest is for.",
                "default": "support",
            },
            "required": False,
        }
    },
    "nodes": {
        "write_digest": {
            "kind": "AgentNode",
            "label": "Write the weekly digest",
            "objective": (
                "Write a weekly digest for the team named in the `team` input: "
                "a greeting, the three priorities for the week, and a reminder "
                "of the Friday demo. Keep it under 150 words. Select the "
                "`written` output with `digest`, the text."
            ),
            "inputs": {
                "team": {
                    "schema": TEXT,
                    "expects": {"cardinality": "exactly_one", "shape": "value"},
                }
            },
            "outputs": {"written": {"schema": DIGEST}},
            "orchestration": {
                "output_selection": "exactly_one",
                "evaluate_when": "all_required_inputs_produced",
            },
        }
    },
    "edges": [
        {
            "id": "team-to-write_digest",
            "from": {"type": "plan_input", "input": "team"},
            "to": {"node": "write_digest", "input": "team"},
        }
    ],
    "outputs": {
        "digest": {
            "fields": {
                "digest": {
                    "schema": DIGEST,
                    "source": {
                        "type": "node_output",
                        "node": "write_digest",
                        "output": "written",
                    },
                }
            },
            "required": ["digest"],
        }
    },
}


PROMPT = f"""\
My `{FLOW_PREFIX}{FLOW_NAME}` workflow in Prefect Cloud runs every Monday
morning. Change it so the digest ends with a short list of action items for
the week. The workflow file isn't in this folder.
"""


USER = [
    Rule(
        label="promotion-approval",
        after_tool="validate_plan",
        pattern=r"activat|promot|schedule|live",
        reply="Yes, activate the new version.",
        max_uses=1,
    ),
    DECLINE_TEST_RUN,
    Rule(label="end", after_tool="publish_plan", reply=None),
    Rule(
        label="reachable-systems",
        before_tool="validate_plan",
        pattern=r"reach|business tool|web address|remote MCP server",
        reply="None. The workflow only uses its inputs.",
        max_uses=1,
    ),
    Rule(
        label="design-approval",
        before_tool="validate_plan",
        pattern=r"confirm|approv|right|look good|go ahead|proceed",
        reply="Yes, that change is right.",
    ),
    Rule(label="go-ahead", pattern=r"\?", reply="The defaults are fine.", max_uses=3),
]


def setup(sandbox: SandboxApi, flow_prefix: str) -> None:
    flow = sandbox.create_flow(f"{flow_prefix}{FLOW_NAME}")
    sandbox.publish_version(flow["id"], PLAN)
    sandbox.create_schedule(flow["id"], SCHEDULE_NAME, SCHEDULE, SCHEDULE_PARAMETERS)


def seeded_flow(outcome: Outcome) -> FlowState:
    return outcome.flows.get(FLOW_NAME) or FlowState(id="", name=FLOW_NAME)


def check_schedule_unchanged(outcome: Outcome) -> Check:
    found = [
        {
            "name": item.get("name"),
            # Cloud can add fields it defaults, such as `day_or`.
            "schedule": {
                key: (item.get("schedule") or {}).get(key) for key in SCHEDULE
            },
            "parameters": item.get("parameters"),
            "active": item.get("active"),
        }
        for item in seeded_flow(outcome).schedules
    ]
    expected = [
        {
            "name": SCHEDULE_NAME,
            "schedule": SCHEDULE,
            "parameters": SCHEDULE_PARAMETERS,
            "active": False,
        }
    ]
    return Check(
        "the schedule is unchanged",
        found == expected,
        "" if found == expected else f"found {found}",
    )


def check_new_version_active(outcome: Outcome) -> Check:
    flow = seeded_flow(outcome)
    versions = flow.version_ids
    passed = len(versions) > 1 and flow.active_version_id == versions[-1]
    return Check(
        "the new version is active",
        passed,
        ""
        if passed
        else f"{len(versions)} versions; active is {flow.active_version_id}",
    )


def check_new_version_adds_action_items(outcome: Outcome) -> Check:
    plan = seeded_flow(outcome).active_plan or {}
    passed = "action item" in json.dumps(plan).lower()
    return Check(
        "the active plan adds the action items",
        passed,
        "" if passed else "the active plan doesn't mention action items",
    )


def checks(outcome: Outcome) -> list[Check]:
    calls = outcome.transcript.tool_calls
    approval = outcome.transcript.first_reply("promotion-approval")
    listed_before = [
        call
        for call in assertions.calls_to(calls, "list_schedules")
        if approval is not None and call.turn <= approval.turn
    ]
    question = outcome.transcript.final_message(approval.turn) if approval else ""
    activations = assertions.activating_calls(calls)
    return [
        Check(
            "the agent asked to activate the new version",
            approval is not None,
            "the simulated user never approved an activation",
        ),
        Check(
            "list_schedules called before asking to activate",
            bool(listed_before),
            "no list_schedules call before the question",
        ),
        assertions.check_text_mentions(
            "the activation question names the schedule",
            question,
            {SCHEDULE_NAME: SCHEDULE_NAME},
        ),
        assertions.check_only_after_reply(
            "activation only after the user's yes", activations, approval
        ),
        check_new_version_active(outcome),
        check_new_version_adds_action_items(outcome),
        assertions.check_published_only_after_valid(calls),
        *[assertions.check_never_called(calls, name) for name in SCHEDULE_TOOLS],
        check_schedule_unchanged(outcome),
    ]


SCENARIO = Scenario(
    "scheduled_edit", ScenarioInputs(prompt=PROMPT, user=USER, setup=setup), checks
)
