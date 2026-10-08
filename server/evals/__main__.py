"""Run the behavioral evaluations with `uv run python -m evals`. See `README.md`."""

import argparse
import asyncio
import os
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

from evals import summary
from evals.dataset import CASE_NAMES, DATASET_NAME, build_dataset
from evals.evaluators import DEFAULT_JUDGE_MODEL
from evals.lifecycle import Session
from evals.runner import scenario_task
from evals.sandbox import (
    Credentials,
    SandboxApi,
    SandboxConfigError,
    read_credentials,
    session_prefix,
)

DEFAULT_MAX_CONCURRENCY = 3
REPORT_WIDTH = 160
USAGE_ERROR = 2
ANTHROPIC_KEY_VARIABLE = "ANTHROPIC_API_KEY"
MISSING_JUDGE_KEY_MESSAGE = (
    "The LLM judges use {model}, which reads ANTHROPIC_API_KEY. Set it, or "
    "pass --no-judge to run only the deterministic checks."
)


def positive_int(text: str) -> int:
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be 1 or more, not {value}")
    return value


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m evals",
        description=(
            "Run the behavioral evaluations against the Prefect Cloud sandbox "
            "workspace in PREFECT_API_URL and PREFECT_API_KEY."
        ),
    )
    parser.add_argument(
        "--repeat",
        type=positive_int,
        default=1,
        help="Run each case this many times. The summary has each case's pass rate.",
    )
    parser.add_argument(
        "--model", help="Model for the agent. Defaults to the Claude Agent SDK's."
    )
    parser.add_argument(
        "--judge-model",
        default=DEFAULT_JUDGE_MODEL,
        help=(
            "Pydantic AI model for the LLM judges, as <provider>:<model>. "
            f"Defaults to {DEFAULT_JUDGE_MODEL}."
        ),
    )
    parser.add_argument(
        "--no-judge",
        dest="judge",
        action="store_false",
        help="Skip the LLM judges and run only the deterministic checks.",
    )
    parser.add_argument(
        "--max-concurrency",
        type=positive_int,
        default=DEFAULT_MAX_CONCURRENCY,
        help=f"Most runs at the same time. Defaults to {DEFAULT_MAX_CONCURRENCY}.",
    )
    parser.add_argument(
        "--case",
        dest="cases",
        action="extend",
        nargs="+",
        choices=CASE_NAMES,
        metavar="NAME",
        help=f"Run only these cases: {', '.join(CASE_NAMES)}. Defaults to all.",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        help="Also write the pass rates and the report to this file, as Markdown.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help=(
            "Directory for each run's transcript and working directory. "
            "Defaults to a new temporary directory."
        ),
    )
    return parser.parse_args(argv)


def judge_model(args: argparse.Namespace) -> str | None:
    """Return the judges' model, or None when the judges are off."""
    return args.judge_model if args.judge else None


async def evaluate(args: argparse.Namespace, credentials: Credentials) -> int:
    """Run the dataset, print the report, and return the exit code.

    Returns 1 when a check failed, a run or an evaluator raised, or a flow
    of the session could not be deleted.
    """
    flow_prefix = session_prefix()
    output_dir = args.output_dir or Path(tempfile.mkdtemp(prefix="evals-"))
    sandbox = SandboxApi(credentials)
    session = Session(sandbox, flow_prefix, output_dir / flow_prefix.rstrip("-"))
    print(f"Writing transcripts to {session.output_dir}")
    try:
        dataset = build_dataset(args.cases, judge_model(args))
        report = await dataset.evaluate(
            scenario_task(args.model),
            name=DATASET_NAME,
            max_concurrency=args.max_concurrency,
            progress=sys.stderr.isatty(),
            repeat=args.repeat,
            lifecycle=session.lifecycle,
            metadata={
                "agent_model": args.model or "default",
                "judge_model": judge_model(args) or "off",
            },
        )
    finally:
        # Each run deletes its own flows. This also deletes the flows of a run
        # whose cleanup failed, or that was cancelled before its teardown.
        try:
            sandbox.delete_flows_with_prefix(flow_prefix)
        except Exception as exc:
            session.cleanup_errors.append(
                f"Could not delete the flows named {flow_prefix}*: {exc}"
            )
        sandbox.close()

    rendered = report.render(width=REPORT_WIDTH)
    print(rendered)
    for line in summary.problems(report):
        print(f"FAILED {line}")
    for error in session.cleanup_errors:
        print(error, file=sys.stderr)
    if args.summary:
        args.summary.write_text(
            summary.markdown(
                report,
                rendered,
                agent_model=args.model,
                judge_model=judge_model(args),
            )
        )
    passed = summary.all_passed(report) and not session.cleanup_errors
    return 0 if passed else 1


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        credentials = read_credentials()
    except SandboxConfigError as exc:
        print(exc, file=sys.stderr)
        return USAGE_ERROR
    model = judge_model(args)
    if (
        model is not None
        and model.startswith("anthropic:")
        and not os.environ.get(ANTHROPIC_KEY_VARIABLE)
    ):
        print(MISSING_JUDGE_KEY_MESSAGE.format(model=model), file=sys.stderr)
        return USAGE_ERROR
    # Only the settings the runner passes configure Prefect for the agent.
    for name in [name for name in os.environ if name.startswith("PREFECT_")]:
        del os.environ[name]
    return asyncio.run(evaluate(args, credentials))


if __name__ == "__main__":
    sys.exit(main())
