"""Tests for the pass-rate report."""

from evals.report import pass_rate_lines, pass_rate_markdown

RESULTS = {
    "evals/scenarios/test_scheduled_edit.py::test_scheduled_edit": [True, False],
    "evals/scenarios/test_no_infrastructure.py::test_no_infrastructure": [True],
}


def test_pass_rate_lines():
    assert pass_rate_lines(RESULTS) == [
        "1/2 (50%)  evals/scenarios/test_scheduled_edit.py::test_scheduled_edit",
        "1/1 (100%)  evals/scenarios/test_no_infrastructure.py::test_no_infrastructure",
    ]


def test_pass_rate_markdown():
    assert pass_rate_markdown(RESULTS, "claude-haiku-5-5").splitlines() == [
        "## Behavioral evaluations",
        "",
        "Agent model: `claude-haiku-5-5`",
        "",
        "| Scenario | Passed | Runs | Pass rate |",
        "|---|---|---|---|",
        "| `test_scheduled_edit` | 1 | 2 | 50% |",
        "| `test_no_infrastructure` | 1 | 1 | 100% |",
    ]
