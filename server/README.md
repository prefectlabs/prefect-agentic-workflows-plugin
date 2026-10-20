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

## Credentials in HTTP headers

When the server is hosted over HTTP, each client can send its own workspace credentials
in two headers, and the server uses them instead of its Prefect profile for that request:

- `X-Prefect-Api-Url`: the workspace API URL, such as
  `https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>`
- `X-Prefect-Api-Key`: a Prefect Cloud API key for that workspace

Send both headers or neither. A request with only one of them fails, so the
server's own API key is never sent to a URL that a client chose. Without the
headers, and always over stdio, the server uses its Prefect profile.

To serve over HTTP, run the server with the FastMCP CLI from this directory:

```sh
uv run fastmcp run src/prefect_agentic_workflows_mcp/server.py:build_server --transport http
```

Then add it to Claude Code with the headers:

```sh
claude mcp add-json prefect-agentic-workflows '{
  "type": "http",
  "url": "https://your-server.example.com/mcp",
  "headers": {
    "X-Prefect-Api-Url": "https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>",
    "X-Prefect-Api-Key": "<your-api-key>"
  }
}'
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
server, and its `behavioral evals` check must pass before a pull request
merges into `main`. See
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
