"""Print the commits between two git refs as JSON for the release notes draft."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys

PR_NUMBER = re.compile(r"\(#(\d+)\)$|^Merge pull request #(\d+)")
CHANGE_TYPES = {
    "feat": "feature",
    "fix": "fix",
    "docs": "docs",
    "chore": "chore",
    "ci": "chore",
    "build": "chore",
}


def git(*args: str) -> str:
    result = subprocess.run(["git", *args], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def change_type(subject: str) -> str:
    prefix = re.match(r"^(\w+)(\(.+\))?!?:", subject)
    if prefix is None:
        return "other"
    return CHANGE_TYPES.get(prefix.group(1).lower(), "other")


def pr_number(subject: str) -> int | None:
    match = PR_NUMBER.search(subject)
    if match is None:
        return None
    return int(match.group(1) or match.group(2))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--since",
        help="start tag or commit; defaults to the latest tag before --until",
    )
    parser.add_argument("--until", default="HEAD", help="end ref; defaults to HEAD")
    args = parser.parse_args()

    since = args.since
    if since is None:
        try:
            # Start from the parent so a tag on --until itself is skipped.
            since = git("describe", "--tags", "--abbrev=0", f"{args.until}^")
        except subprocess.CalledProcessError:
            print("No tag found. Pass --since with a tag or commit.", file=sys.stderr)
            return 1

    log = git("log", "--format=%H%x09%s", f"{since}..{args.until}")
    changes = []
    for line in log.splitlines():
        sha, subject = line.split("\t", 1)
        changes.append(
            {
                "sha": sha,
                "subject": subject,
                "pr": pr_number(subject),
                "type": change_type(subject),
            }
        )

    result = {"since": since, "until": args.until, "changes": changes}
    json.dump(result, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
