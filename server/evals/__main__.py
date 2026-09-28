"""Run behavioral evaluations and print a pass or fail table.

Run from the `server/` directory:

    uv run python -m evals                       # every scenario, once
    uv run python -m evals release-notes-conversion --repeat 5
    uv run python -m evals --list

Each run starts a real agent, so it costs money and its result can change
from run to run. CI never runs this.
"""

import argparse
import shlex
import shutil
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from evals.runner import DEFAULT_AGENT_TURN_LIMIT, RunResult, run_scenario
from evals.scenarios import SCENARIOS

DEFAULT_LOG_DIR = Path(__file__).parent / "results"


def parse_arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m evals",
        description="Run behavioral evaluations of the agentic-workflows skill "
        "against a fake Prefect Cloud API.",
    )
    parser.add_argument(
        "scenarios",
        nargs="*",
        metavar="SCENARIO",
        help="Scenarios to run. Omit them to run every scenario.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Run each scenario this many times and report the pass rate.",
    )
    parser.add_argument(
        "--agent",
        default="claude",
        help="Command that starts Claude Code. Default: %(default)s.",
    )
    parser.add_argument("--model", help="Model for the agent to use.")
    parser.add_argument(
        "--agent-turn-limit",
        type=int,
        default=DEFAULT_AGENT_TURN_LIMIT,
        help="Most model turns the agent can take to answer one message.",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=DEFAULT_LOG_DIR,
        help="Directory for transcripts and plan files. Default: evals/results.",
    )
    parser.add_argument(
        "--keep-workspaces",
        action="store_true",
        help="Keep each run's working directory and print its path.",
    )
    parser.add_argument(
        "--list", action="store_true", help="List the scenarios and exit."
    )
    return parser.parse_args(argv)


def format_table(results: list[RunResult]) -> str:
    """Return a row per scenario with its runs and pass rate, then each failed check."""
    rows = [("Scenario", "Runs", "Passed", "Pass rate", "Result")]
    names = list(dict.fromkeys(result.scenario for result in results))
    for name in names:
        runs = [result for result in results if result.scenario == name]
        passed = sum(result.passed for result in runs)
        rows.append(
            (
                name,
                str(len(runs)),
                str(passed),
                f"{passed / len(runs):.0%}",
                "PASS" if passed == len(runs) else "FAIL",
            )
        )
    widths = [max(len(row[column]) for row in rows) for column in range(len(rows[0]))]
    lines = [
        "  ".join(
            cell.ljust(width) for cell, width in zip(row, widths, strict=True)
        ).rstrip()
        for row in rows
    ]
    for result in results:
        failed = [check for check in result.checks if not check.passed]
        if not failed:
            continue
        lines.append("")
        lines.append(f"{result.scenario}, attempt {result.attempt}:")
        for check in failed:
            detail = f": {check.detail}" if check.detail else ""
            lines.append(f"  FAIL {check.name}{detail}")
        if result.log_path is not None:
            lines.append(f"  log: {result.log_path}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    if arguments.list:
        for scenario in SCENARIOS.values():
            print(f"{scenario.name}: {scenario.description}")
        return 0

    unknown = [name for name in arguments.scenarios if name not in SCENARIOS]
    if unknown:
        print(f"Unknown scenarios: {', '.join(unknown)}.", file=sys.stderr)
        print(f"Known scenarios: {', '.join(SCENARIOS)}.", file=sys.stderr)
        return 2
    agent = shlex.split(arguments.agent)
    if shutil.which(agent[0]) is None:
        print(
            f"Can't find the agent command {agent[0]!r}. Install Claude Code, or "
            "pass its path with --agent.",
            file=sys.stderr,
        )
        return 2

    log_dir = arguments.log_dir / datetime.now().strftime("%Y%m%d-%H%M%S")
    results = []
    for name in arguments.scenarios or list(SCENARIOS):
        for attempt in range(1, arguments.repeat + 1):
            print(f"Running {name}, attempt {attempt} of {arguments.repeat}...")
            result = run_scenario(
                SCENARIOS[name],
                attempt=attempt,
                agent=agent,
                agent_turn_limit=arguments.agent_turn_limit,
                model=arguments.model,
                log_dir=log_dir,
                keep_workspace=arguments.keep_workspaces,
            )
            print(
                f"  {'PASS' if result.passed else 'FAIL'}, "
                f"${result.transcript.cost_usd:.2f}"
            )
            if result.workspace is not None:
                print(f"  working directory: {result.workspace}")
            results.append(result)

    print()
    print(format_table(results))
    print()
    print(f"Transcripts and plans: {log_dir}")
    return 0 if all(result.passed for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
