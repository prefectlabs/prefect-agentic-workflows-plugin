# prefect-agentic-workflows-mcp

A local stdio MCP server, named `prefect-agentic-workflows`, for authoring and
running Prefect Cloud execution plans.

**This package is alpha.** Tool names, arguments, and results can change in any
release.

The server uses the Prefect Cloud workspace in your active Prefect profile.
Execution plans are only available in Prefect Cloud, and your account needs the
`execution-plans` feature.

Run it from the GitHub repository with `uvx`:

```sh
uvx --from "git+https://github.com/prefectlabs/prefect-agentic-workflows-plugin#subdirectory=server" prefect-agentic-workflows-mcp
```

## Development

Run these commands from this directory.

```sh
uv sync                  # install the package and the dev tools
uv run pytest            # run the mocked test suite
uv run ruff check .      # lint
uv run ruff format .     # format
uv run ty check          # type-check src/ and tests/
```

The mocked tests call tools through an in-memory FastMCP client and mock the
Prefect Cloud API with `respx`. They need no credentials.

The integration tests in `tests/integration/` call a real Cloud workspace. They
skip unless you opt in and your active profile points at a Cloud workspace whose
account has the `execution-plans` feature:

```sh
PREFECT_AGENTIC_WORKFLOWS_INTEGRATION=1 uv run pytest tests/integration
```

The behavioral evaluations in `evals/` run a real agent with the skill and
this server against a Prefect Cloud sandbox workspace. They cost money. The
`Evals` workflow runs them on pull requests that change the skill or the
server, and a failure doesn't block the merge. See
[`evals/README.md`](evals/README.md) to run one or add one.

### Adding a tool group

Each module in `src/prefect_agentic_workflows_mcp/tools/` is one tool group.
A module defines `register(mcp, api)`, and the server calls it at startup.
Add each tool with the `tool` decorator from that package. The decorator sets
`readOnlyHint` and `destructiveHint` and adds the alpha notice to the
description. Send every Cloud request through the `WorkspaceApi` passed to
`register`, so the preflight check runs first.

Put the group's tests in their own file under `tests/`. The fixtures in
`tests/conftest.py` give each test a fake Cloud profile, a `respx` router with
the preflight route mocked, and a connected client.
