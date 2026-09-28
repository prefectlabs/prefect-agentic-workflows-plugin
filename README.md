# Prefect agentic workflows

> [!WARNING]
> **This project is alpha.** Tool names, arguments, results, and the skill's
> steps can change in any commit. The install commands below track the `main`
> branch, and there are no tagged releases yet.

Prefect Cloud can run an agentic workflow as an execution plan: a JSON graph of
agent, human-input, and deployment nodes attached to a flow. This repository
has two parts that help an agent build, publish, run, and schedule execution
plans. You install each part on its own.

- **The MCP server**, `prefect-agentic-workflows`, is a local stdio server in
  [`server/`](server/). Its tools read the plan schema, validate and publish
  plans, create the flow, start and watch runs, answer human-input forms, and
  manage schedules.
- **The skill**, `agentic-workflows`, is in
  [`skills/agentic-workflows/`](skills/agentic-workflows/). It tells an agent
  how to design a plan that fits the platform limits and walks it through
  drafting, validating, publishing, and testing the plan with the server's
  tools. The skill is Markdown only, so it needs no Python.

## Prerequisites

- A Prefect Cloud workspace. Execution plans are not available in a
  self-hosted Prefect server.
- The `execution-plans` feature turned on for your Prefect Cloud account. Ask
  your Prefect contact to turn it on.
- An object storage bucket configured for the workspace. Prefect Cloud stores
  plan versions in it. Ask your Prefect contact to configure one.
- [uv](https://docs.astral.sh/uv/getting-started/installation/), which
  provides `uvx`, to run the server.
- Node.js, which provides `npx`, if you install the skill with the `skills`
  CLI.

The server uses the workspace and API key in your active Prefect profile. If
you have not logged in to Prefect Cloud on this machine, log in and pick the
workspace:

```sh
uvx prefect cloud login
```

## Install the MCP server

The server runs from this repository with `uvx`. You don't need to clone the
repository or install a package first:

```sh
uvx --from "git+https://github.com/prefectlabs/prefect-agentic-workflows-plugin#subdirectory=server" prefect-agentic-workflows-mcp
```

Your agent runs this command for you. Add it to your agent's MCP configuration
with one of the snippets below.

### Claude Code

```sh
claude mcp add --scope user prefect-agentic-workflows -- \
  uvx --from "git+https://github.com/prefectlabs/prefect-agentic-workflows-plugin#subdirectory=server" prefect-agentic-workflows-mcp
```

`--scope user` makes the server available in every project. Leave it out to
add the server to the current project only. Run `claude mcp list` to check
that the server connects.

### Other MCP clients

Most MCP clients read a JSON file with an `mcpServers` object. Add this entry:

```json
{
  "mcpServers": {
    "prefect-agentic-workflows": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/prefectlabs/prefect-agentic-workflows-plugin#subdirectory=server",
        "prefect-agentic-workflows-mcp"
      ]
    }
  }
}
```

To use a Prefect profile other than the active one, add
`"env": {"PREFECT_PROFILE": "<profile name>"}` to the entry.

The server starts even when the profile is wrong. The first tool call checks
the profile and the workspace and returns an error that says what to fix, for
example a profile that points at a self-hosted server or an account without
the `execution-plans` feature.

### Update the server

`uvx` caches the version it installed. To get the latest commit on `main`, run
the install command once with `--refresh` after `uvx`, then restart your agent.
The command starts the server and waits for input, so stop it with Ctrl-C once
it has started.

## Install the skill

```sh
npx skills add prefectlabs/prefect-agentic-workflows-plugin
```

The `skills` CLI asks which agents to install the skill for. Add `--global` to
install it for your user instead of the current project.

To install the skill by hand, copy `skills/agentic-workflows/` into your
agent's skills directory. For Claude Code, that is `~/.claude/skills/` for
every project or `.claude/skills/` in one project:

```sh
git clone https://github.com/prefectlabs/prefect-agentic-workflows-plugin
cp -R prefect-agentic-workflows-plugin/skills/agentic-workflows ~/.claude/skills/
```

## Build a workflow

Ask your agent for a workflow that runs in Prefect Cloud. The skill has three
ways to start:

- **Describe the workflow.** For example: "Build a Prefect workflow that
  summarizes yesterday's failed flow runs and posts the summary to Slack." The
  agent fills in what your description covers, picks safe defaults for the
  rest, and asks only about the gaps.
- **Give it nothing.** For example: "Help me build a Prefect agentic
  workflow." The agent interviews you with one short checklist that includes a
  recommended default for each item.
- **Convert an existing skill.** Point the agent at a skill directory that
  works locally. The agent reports which parts need a change to run in Prefect
  Cloud, for example a local script or a stdio MCP server, and proposes a
  substitute for each one.

All three ways continue with the same steps:

1. The agent writes a plain-language summary of the plan and waits for you to
   confirm it.
2. It drafts the plan in `workflows/<flow-name>.plan.json` in your working
   directory.
3. It validates the plan against the live schema and fixes errors until the
   plan passes.
4. It publishes the plan to a flow, which it creates if needed.
5. It offers a test run. You answer any human-input form yourself.
6. It reports the flow and run links, the results, and any platform limits
   that still affect the workflow.

The agent asks for your approval at four points: the design summary, before
each run it starts, before it activates a new version on a flow that already
has an active plan, and before it creates, changes, or deletes a schedule. A
run always uses the flow's active version, and the agent's report names the
version a run used. Credentials stay in Prefect
Secret blocks. The agent refers to a block by its ID and never asks for a
secret value.

## Tools

Every tool sets the MCP `readOnlyHint` and `destructiveHint` annotations, so
your agent can ask before a tool writes.

| Tools | What they do |
|---|---|
| `get_schema`, `validate_plan` | Read the current plan schema and validate a plan without publishing it |
| `get_or_create_flow` | Find a flow by name, or create it |
| `publish_plan`, `get_plan`, `list_plan_versions`, `activate_plan_version` | Publish a plan as a new version, read versions, and activate one, for example to roll back |
| `start_run`, `get_run`, `get_run_output`, `submit_human_input` | Start a run of the active plan, watch its status, read outputs, and answer human-input forms |
| `create_schedule`, `list_schedules`, `get_schedule`, `update_schedule`, `delete_schedule` | Manage a flow's plan schedules |
| `list_secret_blocks` | List Secret block names and IDs, never their values |

## Development

See [`server/README.md`](server/README.md) to run the server's tests and type
checker.

## License

[Apache 2.0](LICENSE)
