"""Pass rates and failed checks from an evaluation report, and its Markdown copy."""

from dataclasses import dataclass
from typing import Any

from pydantic_evals.reporting import EvaluationReport, ReportCase

Report = EvaluationReport[Any, Any, Any]


@dataclass(frozen=True)
class PassRate:
    case: str
    passed: int
    runs: int

    @property
    def rate(self) -> float:
        return self.passed / self.runs if self.runs else 0.0


def run_passed(case: ReportCase[Any, Any, Any]) -> bool:
    """Return whether every assertion of one run passed and every evaluator finished."""
    return not case.evaluator_failures and all(
        result.value for result in case.assertions.values()
    )


def pass_rates(report: Report) -> list[PassRate]:
    """Return the passed and total runs of each case, by case name.

    A run whose task or lifecycle raised counts as a failed run.
    """
    runs: dict[str, list[bool]] = {}
    for case in report.cases:
        runs.setdefault(case.source_case_name or case.name, []).append(run_passed(case))
    for failure in report.failures:
        runs.setdefault(failure.source_case_name or failure.name, []).append(False)
    return [
        PassRate(name, sum(results), len(results))
        for name, results in sorted(runs.items())
    ]


def one_line(text: str) -> str:
    return " ".join(text.split())


def problems(report: Report) -> list[str]:
    """Return one line for each failed assertion, evaluator error, and failed run."""
    lines: list[str] = []
    for case in sorted(report.cases, key=lambda case: case.name):
        for name, result in case.assertions.items():
            if not result.value:
                reason = f": {one_line(result.reason)}" if result.reason else ""
                lines.append(f"`{case.name}`: {name}{reason}")
        for failure in case.evaluator_failures:
            lines.append(
                f"`{case.name}`: the evaluator {failure.name} raised "
                f"{one_line(failure.error_message)}"
            )
    for failure in sorted(report.failures, key=lambda failure: failure.name):
        lines.append(
            f"`{failure.name}`: the run raised {one_line(failure.error_message)}"
        )
    return lines


def all_passed(report: Report) -> bool:
    return not report.failures and all(run_passed(case) for case in report.cases)


def markdown(
    report: Report,
    rendered: str,
    *,
    agent_model: str | None,
    judge_model: str | None = None,
) -> str:
    """Return the pass rates, the problems, and the rendered report as Markdown."""
    judges = f"`{judge_model}`" if judge_model else "off"
    lines = [
        "## Behavioral evaluations",
        "",
        f"Agent model: `{agent_model or 'default'}`. LLM judges: {judges}.",
        "",
        "| Case | Passed | Runs | Pass rate |",
        "|---|---|---|---|",
    ]
    lines += [
        f"| `{rate.case}` | {rate.passed} | {rate.runs} | {rate.rate:.0%} |"
        for rate in pass_rates(report)
    ]
    found = problems(report)
    if found:
        lines += ["", "### Failed checks", ""]
        lines += [f"- {line}" for line in found]
    lines += [
        "",
        "<details><summary>Full report</summary>",
        "",
        "```text",
        rendered.rstrip(),
        "```",
        "",
        "</details>",
    ]
    return "\n".join(lines) + "\n"
