# Converting a skill

A skill runs on the user's machine: it can run scripts, start stdio MCP servers, read and write local files, and loop until it is done. An execution plan runs in Prefect Cloud, where an agent node's only tools are the remote MCP servers in its `mcp` config. Converting a skill means finding every part that depends on the local machine and agreeing a substitute with the user before you draft.

Work through these steps in order. Finish each step before starting the next.

## 1. Find the skill

| The user gives you | What to do |
|---|---|
| A path to a `SKILL.md` file | The skill's directory is the file's parent directory. |
| A path to a directory | The directory must contain `SKILL.md`. If it doesn't, list what it contains and ask which skill they mean. |
| A skill name | Search the skill directories with the command below. |
| Pasted text | Use the text as `SKILL.md`. There is no directory to read. |

To find an installed skill, search the project's and the user's skill directories and the plugin cache. Replace `<name>` with the name the user gave:

```bash
find .claude/skills .agents/skills ~/.claude/skills ~/.agents/skills ~/.claude/plugins/cache \
  -name SKILL.md 2>/dev/null | xargs grep -l -E "^name: *<name> *$" 2>/dev/null
```

When there is no match, search for the name in directory paths with `-path "*<name>*"` and show the user what you find. When the plugin cache has several versions of one skill, use the newest version. When different skills match, show the user their paths and ask which one to convert.

The step is done when you have the skill's directory, or the pasted text.

## 2. Read the whole skill

List every file in the skill directory with `find <skill directory> -type f`, then read every file: `SKILL.md`, its reference files, scripts, templates, and config. A reference file often holds the rules a step checks against, and a script's code shows what it reads, what it prints, and what it changes. For each script, write down its inputs, its output, and its side effects.

When the text is pasted, ask the user to paste each file it names or links to, such as a script or a reference file.

The step is done when you have read every file in the directory and every file that `SKILL.md` names.

## 3. List the steps and the unsupported parts

Number every step of the source skill as S1, S2, and so on. Include the steps the skill doesn't number: setup instructions, questions it asks the user, files it saves, and checks it runs on its own work.

Then check each step against the table below. Each match is an unsupported part. One step can have several. The table also lists parts that do convert, so you can map them in the step map.

| Part in the source skill | Why it can't run as written | Proposed substitute |
|---|---|---|
| A local script or shell command, such as `python3 scripts/x.py`, `git log`, or a CLI tool | An agent node has no shell and no local files. | When a later node needs the script's result, expose the script as a tool on a remote MCP server that the user hosts, and call it from an agent node. When only success or failure matters, run it in a Deployment node. Say in the report that a Deployment node passes on only the child run's ID and state, never the script's output. If a remote MCP server the plan already uses can do the same work, offer that as a third option. |
| A stdio MCP server: a `command` and `args`, `npx`, `uvx`, or `docker run -i` | Plans accept only remote MCP servers over Streamable HTTP. | A remote Streamable-HTTP MCP server with the same tools, for example the service's hosted MCP endpoint. Ask the user for its URL. Its credentials become Secret block references. |
| A loop: repeat until done, retry until it passes, keep going until clean | A plan graph can't have cycles. | A fixed number of steps, such as two check-and-fix passes, or a human checkpoint where a person approves the result or sends it back. Name the number of passes you propose. |
| A timed wait: sleep, wait an hour, check back tomorrow, poll every few minutes | Timer nodes can't be activated yet. | Flag it as not yet supported. Offer a human checkpoint where a person continues the run when the time has passed, or a second workflow on a schedule. |
| A long task: many items, large documents, or many tool calls in one step | An agent node has at most 60 seconds. | Split the task across nodes at natural boundaries, such as one node per phase. Move deterministic parts into a Deployment node. |
| One run of a step per item of a list | Plans have no mapping. | Handle the list inside one agent node when it fits in 60 seconds, or fan out to a fixed set of nodes. |
| Reading or writing a local file, such as saving a draft to disk | An agent node can't reach the user's files. | Pass the content to the next node as a typed output. Return a final file as a plan output, or write it with a remote MCP tool. |
| A question to the user in the middle of the work | The run has no conversation. | A plan input when the answer is known before the run starts. A human-input node when the answer depends on earlier work. |
| A credential in an environment variable or config file | Plans refer to credentials only through Secret blocks. | A Secret block reference. See [secret-blocks.md](secret-blocks.md). |
| A human approval | Converts as written. | A human-input node with one output per choice. |
| A reference file with rules or a style guide | Converts as written. | Copy the rules the node needs into its objective. |
| Setup instructions, such as adding an MCP server | Not work the run does. | The node's `mcp` config. Setup needs no node. |

The step is done when every step has a number, and every match is listed.

## 4. Write the conversion report

Show the user the report in this form:

```markdown
## Conversion report: <skill name>

Source: <path, or "pasted text">. Files read: <every file, by path>.

### Parts that need a decision

| # | Part | Where | Why it can't run as written | Proposed substitute | Other options |
|---|---|---|---|---|---|
| 1 | Runs `scripts/collect_changes.py` | S1 | No shell or local files in a plan | Remote MCP tool, because S2 to S4 need its JSON | Deployment node: passes on only the child run's ID and state, so later nodes can't read the JSON |

### Step map

| Step | What it does | Where it goes in the plan |
|---|---|---|
| S1 | Collect the changes | Agent node `collect_changes`, after decision 1 |
| S5 | Get approval | Human-input node `approve_draft` |

### Questions

1. <each choice the user must make, numbered to match the rows above>
```

Every unsupported part from step 3 gets a row in "Parts that need a decision". Every step from step 3 gets a row in the step map. A step's "Where it goes" names a node, a merge into another step's node, or a decision row. When you propose leaving a step out of the plan, give it a decision row too.

The step is done when the report has a row for every unsupported part and every step.

## 5. Stop for the user's decisions

Stop after the report. Ask the user to accept or change each proposed substitute, and wait for their answer. Accepting the whole report in one reply counts as a decision on every row. When the user's answers change the plan, update the step map and show it again.

The step is done when every row has a decision from the user. Then continue with step 1 of the pipeline in `SKILL.md`. The summary includes the step map with each decision, so every source step is in the plan or left out by the user's choice.
