# Prefect agentic workflows

> [!WARNING]
> **This project is alpha.** Tool names, arguments, results, and the skill's
> steps can change in any commit. The install commands below track the `main`
> branch, and there are no tagged releases yet.

Prefect Cloud can run a workflow in which AI agents and people each do a step.
For example, an agent drafts a reply, a person approves it, and an agent
finishes it. This repository has two parts that let an AI agent app, such as
Claude Code, build these workflows for you and publish them to Prefect Cloud.

- **The MCP server**, `prefect-agentic-workflows`, is in
  [`server/`](server/). It gives your agent the tools to publish, run, and
  schedule workflows in your Prefect Cloud workspace.
- **The skill**, `agentic-workflows`, is in
  [`skills/agentic-workflows/`](skills/agentic-workflows/). It tells your agent
  how to design a workflow that Prefect Cloud can run, and which questions to
  ask you.

Prefect calls a workflow of this kind an *execution plan*: a JSON graph of
agent, human-input, and deployment nodes attached to a flow. You install the
server and the skill separately.

## Quickstart: reply to customer feedback

This quickstart builds a workflow that answers one piece of customer feedback:

1. You paste in the feedback when you start a run.
2. An AI agent sorts it into a bug, a feature request, praise, or a complaint,
   and drafts a reply.
3. A manager reads the draft and approves it, or rejects it with notes.
4. If the manager rejects it, an AI agent rewrites the draft once, using the
   notes.
5. The run ends with the final reply and the category.

The workflow doesn't connect to any of your other tools, so it needs no setup
beyond the steps below. You copy the reply and send it yourself.
[Connect it to your tools](#connect-it-to-your-tools) explains how a later
version can post the reply for you.

### What you need

- A Prefect Cloud workspace. Ask your Prefect contact to turn on the workflow
  feature, `execution-plans`, for your account, and to set up storage for the
  workspace.
- An AI agent app that supports MCP servers and skills. The commands below are
  for [Claude Code](https://docs.anthropic.com/en/docs/claude-code).
- [uv](https://docs.astral.sh/uv/getting-started/installation/) and
  [Node.js](https://nodejs.org/). The install commands use them.

### 1. Connect to Prefect Cloud

Open a terminal and run this command. It opens a browser, where you log in to
Prefect Cloud and pick your workspace:

```sh
uvx prefect cloud login
```

### 2. Install the server and the skill

Run these two commands in the same terminal:

```sh
claude mcp add --scope user prefect-agentic-workflows -- \
  uvx --from "git+https://github.com/prefectlabs/prefect-agentic-workflows-plugin#subdirectory=server" prefect-agentic-workflows-mcp
npx skills add prefectlabs/prefect-agentic-workflows-plugin --global
```

The second command asks which agents to install the skill for. Choose Claude
Code. For another agent app, see [Install reference](#install-reference).

### 3. Ask your agent for the workflow

Start Claude Code in an empty folder. The agent saves the workflow file there.
Paste this request:

```text
Use the agentic-workflows skill to build a Prefect workflow named
customer-feedback-reply. Start from the customer feedback reply example.
Each run takes one piece of customer feedback as text. An AI agent sorts it
into bug, feature request, praise, or complaint, and drafts a reply. A
manager approves the draft, or rejects it with notes. If the manager rejects
it, an agent revises the draft once, using the notes. The workflow returns
the final reply and the category. It doesn't connect to any other tools.
```

### 4. Answer the agent's questions

The agent asks a few questions before it publishes anything. It may skip some
of these if your prompt already answered them. Answer them like this:

| The agent asks | Answer |
|---|---|
| Which of your business tools the workflow can use, through a "remote MCP server" | "None. The workflow only uses the feedback I paste in." |
| Anything else about the design | "The defaults are fine." |
| Whether its summary of the workflow is right | Check that it has three steps: sort and draft, manager review, and finish the reply. Check that it has one input, `feedback`, and two results, `reply` and `category`. Then answer "Yes, build it." |
| Whether to start a test run | "No. I'll run it in Prefect Cloud." |

The agent then saves the workflow as
`workflows/customer-feedback-reply.plan.json`, checks it, and publishes it to
your workspace. It ends with a link to the workflow in Prefect Cloud.

### 5. Run the workflow

1. Open the link the agent gave you. You can also find the workflow under
   **Flows** in Prefect Cloud, by the name `customer-feedback-reply`.
2. Click **Run**.
3. In **Customer feedback**, paste one piece of feedback, for example: "I
   ordered a blue sweater two weeks ago and it still hasn't arrived. I emailed
   twice and nobody answered."
4. Click **Run**.

The run's page opens and shows the workflow's steps.

### 6. Approve or reject the draft

1. When **Sort the feedback and draft a reply** has finished, click it. Under
   **Outputs**, click `drafted` to read the category and the draft.
2. Click **Provide input** at the top of the page, or on the **Manager review**
   step.
3. In **Decision**, choose `approved`. Or choose `rejected`, and write in
   **Notes** what the reply should change.
4. Click **Submit**.

The run waits until someone answers. A manager can answer from their own
Prefect Cloud login.

### 7. Read the reply

When the run completes, click **Finish the reply**. Under **Outputs**, click
`approved_reply` if the manager approved the draft. The step copied the draft
unchanged. Click `revised_reply` if the manager rejected it. The other output
shows **Not selected**.

You can also ask your agent: "Show me the reply and the category from the last
customer-feedback-reply run." It reads the run's `reply` and `category`
results.

The `category` result holds the draft as well as the category, because a
workflow result takes a whole step output.

## Connect it to your tools

In the quickstart, you send each reply yourself. For the workflow to post the
approved reply to a Slack channel, it needs two more things.

**A remote MCP server.** This is a web address that lets an AI agent use one of
your business tools, such as Slack or your helpdesk. Many services offer one.
Search the service's help pages for "MCP server". Prefect Cloud runs the agent
steps, so it has to reach that address over the internet. A server that runs
only on your own computer, or only inside your company network, doesn't work.

**A Secret block.** The MCP server needs a credential, such as an access token,
to act for you. You store the credential in Prefect Cloud as a Secret block,
and the workflow refers to the block by its ID. The agent never sees the
value. When a block is missing, the agent gives you the steps to create it.

The workflow sends the credential as a fixed value with each request, so the
MCP server has to accept a credential that way. Check the service's
documentation before you plan on it. For example, Slack's
[MCP server documentation](https://docs.slack.dev/ai/mcp-server) describes
OAuth sign-in for MCP clients, and it doesn't describe sending a fixed token.
Ask your Slack admin whether a token works before you build on it.

When you have the web address and the Secret block, ask your agent:

```text
Change the customer-feedback-reply workflow: after the reply is final, post
it to the #customer-replies Slack channel.
```

The agent checks which MCP servers and Secret blocks the workflow can use, and
tells you before the design when something is missing. The change publishes a
new version of the workflow. The agent asks before it replaces the version
that runs now.

## Install reference

### Prerequisites

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

### Install the MCP server

The server runs from this repository with `uvx`. You don't need to clone the
repository or install a package first:

```sh
uvx --from "git+https://github.com/prefectlabs/prefect-agentic-workflows-plugin#subdirectory=server" prefect-agentic-workflows-mcp
```

Your agent runs this command for you. Add it to your agent's MCP configuration
with one of the snippets below.

#### Claude Code

```sh
claude mcp add --scope user prefect-agentic-workflows -- \
  uvx --from "git+https://github.com/prefectlabs/prefect-agentic-workflows-plugin#subdirectory=server" prefect-agentic-workflows-mcp
```

`--scope user` makes the server available in every project. Leave it out to
add the server to the current project only. Run `claude mcp list` to check
that the server connects.

#### Other MCP clients

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

#### Update the server

`uvx` caches the version it installed. To get the latest commit on `main`, run
the install command once with `--refresh` after `uvx`, then restart your agent.
The command starts the server and waits for input, so stop it with Ctrl-C once
it has started.

### Install the skill

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

## Build other workflows

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

Before any design questions, and before a conversion report, the agent checks
what the workflow can use: which of your business tools an agent can reach
from Prefect Cloud, which Secret blocks exist, and which deployments exist. If
a step needs a tool it can't reach yet, the agent tells you before you spend
time on the design.

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
| `list_deployments` | List deployment names and IDs, for Deployment nodes |

## Development

See [`server/README.md`](server/README.md) to run the server's tests and type
checker.

## License

[Apache 2.0](LICENSE)
