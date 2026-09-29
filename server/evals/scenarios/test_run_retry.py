"""Start a run when the response to the first `start_run` is lost.

The fake starts with the `customer-feedback-reply` flow and the quickstart
plan as its active version. It starts the run for the first `start_run`, then
answers 504 as if a gateway timed out. The agent can't tell whether the run
started.

Expected results:

- the second `start_run` uses the same idempotency key and parameters as the
  first
- only one run exists
"""

import json

from evals import assertions
from evals.assertions import Check
from evals.fake_cloud import FakeCloud
from evals.runner import SKILL_DIR
from evals.scenario import Outcome, Rule, RunScenario, assert_passed

FLOW_NAME = "customer-feedback-reply"
PLAN_FILE = SKILL_DIR / "references" / "examples" / "customer-feedback-reply.plan.json"
FEEDBACK = "I love the new dashboard, but the export button is hard to find."

PROMPT = f"""\
Start a test run of my `{FLOW_NAME}` workflow in Prefect Cloud with this
feedback: "{FEEDBACK}" Watch it and tell me the result.
"""

USER = [
    Rule(
        label="form-answer",
        after_tool="start_run",
        pattern=r"approv|reject|decision|form|review",
        reply="I approve the draft.",
        max_uses=1,
    ),
    Rule(
        label="keep-going",
        after_tool="start_run",
        pattern=r"retry|try again|watch|keep|continue",
        reply="Yes, go ahead.",
        max_uses=2,
    ),
    Rule(label="end", after_tool="start_run", reply=None),
    Rule(
        label="run-approval",
        before_tool="start_run",
        pattern=r"start|run|confirm|proceed|\?",
        reply="Yes, start the run.",
        max_uses=2,
    ),
]


def setup(fake: FakeCloud) -> None:
    flow = fake.add_flow(FLOW_NAME)
    fake.add_version(flow["id"], json.loads(PLAN_FILE.read_text()))
    fake.lose_response("POST", r"/flows/[^/]+/execution-plan/runs")


def check_retry_reused_the_key(outcome: Outcome) -> Check:
    starts = assertions.calls_to(outcome.transcript.tool_calls, "start_run")
    name = "the second start_run reused the first one's idempotency key"
    if len(starts) < 2:
        return Check(name, False, f"start_run called {len(starts)} times")
    first, second = starts[0].arguments, starts[1].arguments
    key = first.get("idempotency_key")
    if not key:
        return Check(name, False, "the first start_run had no idempotency key")
    same_key = second.get("idempotency_key") == key
    same_parameters = second.get("parameters") == first.get("parameters")
    passed = same_key and same_parameters
    return Check(
        name,
        passed,
        ""
        if passed
        else f"keys {key!r} and {second.get('idempotency_key')!r}; "
        f"same parameters: {same_parameters}",
    )


def checks(outcome: Outcome) -> list[Check]:
    runs = len(outcome.fake.runs)
    return [
        check_retry_reused_the_key(outcome),
        Check("only one run exists", runs == 1, f"found {runs} runs"),
    ]


async def test_run_retry(run_scenario: RunScenario) -> None:
    outcome = await run_scenario(PROMPT, USER, setup=setup)
    assert_passed(outcome, checks(outcome))
