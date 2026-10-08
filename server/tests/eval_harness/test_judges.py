"""Tests for the LLM judges, with a stand-in model in place of the judge."""

import json
from pathlib import Path
from typing import Any

from harness_plans import approval_plan
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_evals import Case, Dataset

from evals.dataset import build_dataset
from evals.evaluators import Judge
from evals.record import FlowState, Reply, Transcript
from evals.scenario import Judgement, Outcome, ScenarioInputs
from evals.scenarios import no_infrastructure, rejected_approval
from evals.scenarios import release_notes_conversion as release_notes

PREFIX = "eval-abc123-1-"


class StubJudge:
    """Answers every request with one verdict, and records the prompts it read."""

    def __init__(self, passed: bool, reason: str) -> None:
        self.verdict = {"reason": reason, "pass": passed, "score": float(passed)}
        self.prompts: list[str] = []
        self.model = FunctionModel(self.answer)

    def answer(self, messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        self.prompts.append(
            "".join(
                str(getattr(part, "content", ""))
                for message in messages
                for part in message.parts
            )
        )
        return ModelResponse(
            parts=[ToolCallPart(info.output_tools[0].name, self.verdict)]
        )


def outcome(
    transcript: Transcript | None = None,
    plan: dict[str, Any] | None = None,
    flows: dict[str, FlowState] | None = None,
) -> Outcome:
    plans = {"workflow.plan.json": plan} if plan is not None else {}
    return Outcome(transcript or Transcript(), plans, flows or {}, Path("."), PREFIX)


async def judge(judgement: Judgement, found: Outcome, model: Any) -> dict[str, Any]:
    """Run one `Judge` on an outcome, and return its assertions by name."""

    async def task(inputs: ScenarioInputs) -> Outcome:
        return found

    data = Dataset[ScenarioInputs, Outcome, None](
        name="test",
        cases=[
            Case(
                name="only",
                inputs=ScenarioInputs(prompt="", user=[]),
                evaluators=[Judge(judgement, model)],
            )
        ],
    )
    report = await data.evaluate(task, progress=False)
    assert report.cases[0].evaluator_failures == []
    return report.cases[0].assertions


SECRET = "the whole transcript"
JUDGEMENT = Judgement("judge: it works", "The plan works.", lambda found: "the plan")


async def test_the_judge_reads_only_the_rubric_and_the_evidence():
    stub = StubJudge(False, "The plan has no revision node.")
    found = outcome(Transcript(agent_text=[SECRET]))

    assertions = await judge(JUDGEMENT, found, stub.model)

    result = assertions["judge: it works"]
    assert result.value is False
    assert result.reason == "The plan has no revision node."
    [prompt] = stub.prompts
    assert "The plan works." in prompt
    assert "the plan" in prompt
    assert SECRET not in prompt


async def test_the_judge_fails_without_a_call_when_there_is_nothing_to_judge():
    stub = StubJudge(True, "")
    empty = Judgement("judge: it works", "The plan works.", lambda found: "")

    assertions = await judge(empty, outcome(), stub.model)

    assert assertions["judge: it works"].value is False
    assert assertions["judge: it works"].reason == "nothing to judge"
    assert stub.prompts == []


async def test_rejected_approval_judge_reads_the_published_plan():
    stub = StubJudge(True, "The revise node uses the notes.")
    plan = approval_plan()
    flow = FlowState(
        "flow-1",
        f"{PREFIX}{rejected_approval.FLOW_NAME}",
        active_plan=plan,
        version_ids=["version-1"],
    )
    found = outcome(flows={rejected_approval.FLOW_NAME: flow})
    [judgement] = rejected_approval.SCENARIO.judgements

    assertions = await judge(judgement, found, stub.model)

    assert assertions[judgement.name].value is True
    assert json.dumps(plan, indent=2) in stub.prompts[0]


def test_release_notes_evidence_has_the_skill_the_report_and_the_plan():
    report = "## Conversion report\n\n| 4 | Check loop | Two passes |"
    found = outcome(Transcript(agent_text=["Hello.", report]), approval_plan())
    [judgement] = release_notes.SCENARIO.judgements

    evidence = judgement.evidence(found)

    assert "Check the draft until it is clean" in evidence
    assert "<ConversionReport>\n## Conversion report" in evidence
    assert "Hello." not in evidence
    assert '"kind": "HumanInputNode"' in evidence
    assert judgement.evidence(outcome(Transcript(agent_text=[report]))) == ""


def test_no_infrastructure_evidence_has_the_answers_after_the_reply():
    transcript = Transcript(
        replies=[Reply(1, "no-remote-servers", no_infrastructure.NO_SERVERS)],
        turn_results=["Which tools can an agent reach?", "You could paste them."],
    )
    [judgement] = no_infrastructure.SCENARIO.judgements

    evidence = judgement.evidence(outcome(transcript))

    assert "<AgentReply>\nYou could paste them.\n</AgentReply>" in evidence
    assert "Which tools can an agent reach?" not in evidence
    assert "nobody here hosts anything" in evidence
    assert judgement.evidence(outcome()) == ""


def test_judges_are_added_only_with_a_judge_model():
    def judges(dataset: Dataset[ScenarioInputs, Outcome, None]) -> dict[str, int]:
        return {
            str(case.name): sum(isinstance(ev, Judge) for ev in case.evaluators)
            for case in dataset.cases
        }

    assert set(judges(build_dataset()).values()) == {0}
    assert judges(build_dataset(judge_model="anthropic:claude-haiku-5-5")) == {
        "no_infrastructure": 1,
        "unsupported_loop": 0,
        "release_notes_conversion": 1,
        "scheduled_edit": 0,
        "rejected_approval": 1,
    }
