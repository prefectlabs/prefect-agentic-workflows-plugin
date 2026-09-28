"""A stand-in for `claude -p` that the runner tests start instead of a real agent.

It prints stream-json events like Claude Code does. On the first turn it reads
the schema from the Prefect API URL in its environment, which is the fake, and
asks for approval. On a resumed turn it writes the plan from `STUB_AGENT_PLAN`
to `workflows/demo.plan.json` and reports a validation and a publish.
"""

import json
import os
import sys
from pathlib import Path

import httpx

TOOL_PREFIX = "mcp__prefect-agentic-workflows__"


def emit(**event: object) -> None:
    print(json.dumps(event), flush=True)


def text(value: str) -> None:
    emit(type="assistant", message={"content": [{"type": "text", "text": value}]})


def tool(call_id: str, name: str, arguments: dict[str, object], result: object) -> None:
    emit(
        type="assistant",
        message={
            "content": [
                {
                    "type": "tool_use",
                    "id": call_id,
                    "name": f"{TOOL_PREFIX}{name}",
                    "input": arguments,
                }
            ]
        },
    )
    emit(
        type="user",
        message={
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "content": [{"type": "text", "text": json.dumps(result)}],
                }
            ]
        },
    )


def main() -> None:
    sys.stdin.read()
    emit(type="system", subtype="init", session_id="stub-session")
    if "--resume" not in sys.argv:
        schema = httpx.get(
            f"{os.environ['PREFECT_API_URL']}/execution-plans/schema",
            headers={"Authorization": f"Bearer {os.environ['PREFECT_API_KEY']}"},
        ).json()
        final = (
            "## Conversion report: demo\n\nS1 runs a script. "
            f"Schema {schema['schema_version']}. Do you approve the design?"
        )
    else:
        plan = json.loads(Path(os.environ["STUB_AGENT_PLAN"]).read_text())
        workflows = Path("workflows")
        workflows.mkdir()
        (workflows / "demo.plan.json").write_text(json.dumps(plan))
        tool("1", "validate_plan", {"plan": plan}, {"valid": True, "errors": []})
        tool("2", "publish_plan", {"flow_id": "f", "plan": plan}, {"published": True})
        final = "Published the plan."
    text(final)
    emit(type="result", subtype="success", result=final, total_cost_usd=0.01)


if __name__ == "__main__":
    main()
