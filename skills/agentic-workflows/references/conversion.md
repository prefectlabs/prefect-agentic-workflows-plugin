# Converting a skill

A skill runs on the user's machine, with scripts, local files, stdio MCP servers, and loops. A plan runs in Prefect Cloud, where an agent node's only tools are remote MCP servers. Converting means finding every part that depends on the local machine and agreeing a substitute with the user. This guide is for skills the user owns and trusts.

1. **Find the skill.** A `SKILL.md` path or its directory, an installed skill's name, or pasted text. For a name, use your file search tool to find `SKILL.md` files whose `name:` matches, in the project's and the user's skill directories for the agent you're running in, such as `.claude/skills`, `~/.claude/skills`, and `~/.claude/plugins/cache` for Claude Code, or `.agents/skills` and `~/.agents/skills`. Use the newest version, and ask when several skills match. For pasted text, ask the user to paste each file it refers to.
2. **Read the whole skill** with your file tools: `SKILL.md`, references, scripts, templates, and config. For each script, note its inputs, output, and side effects. Keep credential values found in files out of the report and the plan.
3. **List the steps** as S1, S2, and so on, including setup, questions to the user, saved files, and self-checks. Match each step against the table below.
4. **Run the infrastructure check** for every system the skill touches: each stdio MCP server, each service a script calls, and each credential.
5. **Write the conversion report**, and take it to pipeline step 1, where the user decides every row with the summary.

| Part in the source skill | Proposed substitute |
|---|---|
| A local script or shell command | A tool on a remote MCP server when a later step needs its result ("needs a new service"). A Deployment node when only success or failure matters, since it passes on only the child run's ID and state. |
| A stdio MCP server | The same tools from a remote Streamable-HTTP MCP server, with credentials as Secret blocks. |
| A loop until done | A fixed number of passes, or a human checkpoint that approves or sends back. |
| A timed wait | Not supported yet. A human checkpoint, or a second workflow on a schedule. |
| A list of items | Handle the list in one node, or fan out to a fixed set of nodes. |
| A local file read or write | A typed output to the next node, a plan output, or a remote MCP tool. |
| A question mid-work | A plan input when it's known up front, otherwise a human-input node. |
| A credential in an env var or config | A Secret block reference. |
| A human approval with edit rounds | A human-input node, with a fixed number of revision rounds. |
| Reference rules or a style guide | Copied into the node's objective. |
| Setup instructions | The node's `mcp` config. No node. |

The report has three parts:

```markdown
## Conversion report: <skill name>

### Parts that need a decision
| # | Part | Step | Proposed substitute | Other options |

### Step map
| Step | What it does | Where it goes in the plan |

### Questions
1. <each choice the user must make>
```

Every unsupported part gets a decision row, and every step gets a step-map row: a node, a merge into another node, or a decision row.
