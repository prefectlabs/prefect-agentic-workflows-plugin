"""The pass-rate report printed after a session, and its Markdown copy for CI.

Both take the results of each scenario as a list of booleans, one per run, by
the test's node ID.
"""


def pass_rate_lines(results: dict[str, list[bool]]) -> list[str]:
    return [
        f"{sum(runs)}/{len(runs)} ({sum(runs) / len(runs):.0%})  {name}"
        for name, runs in results.items()
    ]


def pass_rate_markdown(results: dict[str, list[bool]], model: str | None) -> str:
    lines = [
        "## Behavioral evaluations",
        "",
        f"Agent model: `{model or 'default'}`",
        "",
        "| Scenario | Passed | Runs | Pass rate |",
        "|---|---|---|---|",
    ]
    for name, runs in results.items():
        scenario = name.rsplit("::", 1)[-1]
        lines.append(
            f"| `{scenario}` | {sum(runs)} | {len(runs)} | "
            f"{sum(runs) / len(runs):.0%} |"
        )
    return "\n".join(lines) + "\n"
