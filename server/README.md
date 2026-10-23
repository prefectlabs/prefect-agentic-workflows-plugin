# prefect-agentic-workflows-mcp

An MCP server, named `prefect-agentic-workflows`, for authoring and running
Prefect Cloud execution plans. Run it locally over stdio or host it over HTTP.

**This package is alpha.** Tool names, arguments, and results can change in any
release.

By default, the server uses the Prefect Cloud workspace in your active Prefect
profile. Hosted servers can use each client's API key or Prefect Cloud OAuth.
Execution plans are only available in Prefect Cloud, and your account needs the
`execution-plans` feature.

Run it from the GitHub repository with `uvx`:

```sh
uvx --from "git+https://github.com/prefectlabs/prefect-agentic-workflows-plugin#subdirectory=server" prefect-agentic-workflows-mcp
```

## Credentials in HTTP headers

With OAuth disabled, each HTTP client can send its own workspace credentials
in two headers. The server uses them instead of its Prefect profile for that request:

- `X-Prefect-Api-Url`: the workspace API URL, such as
  `https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>`
- `X-Prefect-Api-Key`: a Prefect Cloud API key for that workspace

Send both headers or neither. A request with only one of them fails, so the
server's own API key is never sent to a URL that a client chose. Without the
headers, the server uses its Prefect profile. Local stdio use also reads the
profile and requires no OAuth configuration.

The header URL must use HTTPS on a trusted host, so a client can't make the
server send requests to `localhost` or other hosts on its network. Only
`api.prefect.cloud` is trusted by default. To trust other hosts, set
`PREFECT_HEADER_API_HOSTS` to a comma-separated list of hostnames. The list
replaces the default, so include `api.prefect.cloud` if you still need it:

```sh
PREFECT_HEADER_API_HOSTS=api.prefect.cloud,api2.prefect.cloud
```

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

## Prefect Cloud OAuth

For a shared hosted server, OAuth lets each user authorize access to their own
Prefect Cloud workspaces. FastMCP Gateway completes the user's Prefect consent
flow and forwards their access token to this server. The server validates the
token against Prefect's public signing keys and reads the workspaces in its
consent grant. It needs no personal API key or signing secret.

Enable OAuth in the hosted server's runtime environment. Set the public URL to
this deployment's externally reachable URL, without `/mcp`. For example, a
server targeting Prefect dev uses:

```sh
export PREFECT_MCP_CLOUD_ENABLED=true
export PREFECT_MCP_CLOUD_PUBLIC_BASE_URL=https://your-server.example.com
export PREFECT_MCP_CLOUD_API_BASE_URL=https://api.prefect.dev
uv run fastmcp run src/prefect_agentic_workflows_mcp/server.py:build_server --transport http
```

Choose the API origin for the environment that owns the users and workspaces:

| Environment | `PREFECT_MCP_CLOUD_API_BASE_URL` |
| --- | --- |
| Production (default) | `https://api.prefect.cloud` |
| Staging | `https://api.stg.prefect.dev` |
| Dev | `https://api.prefect.dev` |

The API origin also determines the trusted token issuer, public-key endpoint,
and consent-grant endpoint. `PREFECT_MCP_CLOUD_AUTH_BASE_URL` overrides that
origin for authentication when it differs from the workspace API origin.
Base URLs require HTTPS. API and auth URLs are origins, without `/api` or
other paths.

In Gateway, configure delegated OAuth against the same Prefect environment.
Use its discovery URL, such as
`https://api.prefect.dev/.well-known/oauth-authorization-server`, and the scope
`prefect-cloud:workspaces`. Set the OAuth resource to this server's full public
MCP URL, such as `https://your-server.example.com/mcp`. Configure Gateway to
forward the access token in the `Authorization` Bearer header. That resource
must match the MCP URL derived from `PREFECT_MCP_CLOUD_PUBLIC_BASE_URL`.

Clients connect with the server URL and complete OAuth sign-in. They send no
`X-Prefect-Api-Url` or `X-Prefect-Api-Key` headers:

```sh
claude mcp add-json prefect-agentic-workflows '{
  "type": "http",
  "url": "https://your-server.example.com/mcp"
}'
```

Call `get_workspace` first. With one consented workspace, all tools use it
automatically. With several, the error lists the available workspace names and
UUIDs. Ask the user which to use, then pass that UUID as `workspace_id` to each
tool. Workspaces outside the grant are rejected. A `workspace_id` argument
requires OAuth, so it cannot switch a profile or API-key client to another
workspace.

OAuth is a separate, opt-in server mode. When enabled, every HTTP request to
the MCP endpoint needs a valid Prefect OAuth token. API-key headers do not
override it, and failed OAuth never falls back to the server's profile or key.
To keep an existing profile or paired-header deployment, leave
`PREFECT_MCP_CLOUD_ENABLED` unset or set it to `false`.

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
