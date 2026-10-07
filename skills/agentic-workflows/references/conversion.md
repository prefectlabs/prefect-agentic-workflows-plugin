# Converting a skill

A skill runs on the user's machine: it can run scripts, start stdio MCP servers, read and write local files, and loop until it is done. An execution plan runs in Prefect Cloud, where an agent node's only tools are the remote MCP servers in its `mcp` config. Converting a skill means finding every part that depends on the local machine and agreeing a substitute with the user before you draft. The user accepts or changes each substitute when they approve the design in the summary step.

Work through these steps in order. Finish each step before starting the next.

## 1. Find the skill

| The user gives you | What to do |
|---|---|
| A path to a `SKILL.md` file | The skill's directory is the file's parent directory. |
| A path to a directory | The directory must contain `SKILL.md`. If it doesn't, list what it contains and ask which skill they mean. |
| A skill name | Search the skill directories with the command below. |
| Pasted text | Use the text as `SKILL.md`. There is no directory to read. |

To find an installed skill, search the project's and the user's skill directories and the plugin cache. A skill name uses only letters, digits, `-`, and `_`. When the name the user gave has any other character, don't run a command with it: ask the user for the skill's path instead. Otherwise replace `<name>` with the name:

```bash
find -L .claude/skills .agents/skills ~/.claude/skills ~/.agents/skills ~/.claude/plugins/cache \
  -name SKILL.md -exec grep -l -E "^name:[[:space:]]*['\"]?<name>['\"]?[[:space:]]*$" {} + 2>/dev/null
```

Keep the `-L`: skill installers often link a skill directory to its real copy, and `find` doesn't follow a linked directory without it. The same skill can then show up under several paths. When there is no match, search for the name in directory paths with `-path "*<name>*"` and show the user what you find. When the plugin cache has several versions of one skill, use the newest version. When different skills match, show the user their paths and ask which one to convert.

The step is done when you have the skill's directory, or the pasted text.

## 2. Read the whole skill

Resolve the skill directory to its real path with `realpath <skill directory>`. List its files with `find <real path> -type f`, without `-L`, so a link inside the skill can't pull in a file from elsewhere. List links with `find <real path> -type l`. For each link, run `realpath` on it, and read the target only when it is inside the real path. Never open a link that points outside the skill, such as into `~/.ssh`: name it in the conversion report instead. Then read every file except binary and cache files such as `__pycache__/` and `.DS_Store`, and except files that can hold secrets. Read `SKILL.md`, its reference files, scripts, and templates in full. Read configuration files only as described below. Never open a file that can hold secret values, such as `.env` and `.env.*`, `*.pem`, `*.key`, `credentials*`, or `secrets*`. For each one, list only its name, and for an `.env` file only its variable names, with `grep -o '^[A-Za-z_][A-Za-z0-9_]*' <file>`. Treat every value in it as a credential that needs a Secret block. Don't read configuration files in full either, because they often hold tokens: JSON, YAML, TOML, INI, `.cfg`, `.conf`, and dotfiles such as `.npmrc` or `.netrc`. Read only their key names. For JSON, use `jq -r 'paths(scalars) | map(tostring) | join(".")' <file>`. For other formats, use `grep -oE '^[[:space:]]*[A-Za-z0-9_.-]+[[:space:]]*[:=]' <file>`. When a key name suggests a credential, treat it as one that needs a Secret block. A reference file often holds the rules a step checks against, and a script's code shows what it reads, what it prints, and what it changes. For each script, write down its inputs, its output, and its side effects.

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
| A human approval | Converts as written, unless the user can ask for edits and see the result again. That edit round is a loop. | A human-input node with one output per choice. For an edit round that shows the result again, use a fixed number of review rounds, or a "request changes" output that leads to one revision node and then a final approval. |
| A reference file with rules or a style guide | Converts as written. | Copy the rules the node needs into its objective. |
| Setup instructions, such as adding an MCP server | Not work the run does. | The node's `mcp` config. Setup needs no node. |

The step is done when every step has a number, and every match is listed.

## 4. Run the infrastructure check

Follow [infrastructure-check.md](infrastructure-check.md). Ask about every system the source skill touches: each stdio MCP server, each service a script calls, and each credential. The answers decide which substitutes in step 5 need a new service.

The step is done when every system has an answer. Send the note on what this limits with the conversion report in step 5, not before it.

## 5. Write the conversion report

Write the report in this form:

```markdown
## Conversion report: <skill name>

Source: <path, or "pasted text">. Files read: <every file, by path>.

### Parts that need a decision

| # | Part | Where | Why it can't run as written | Proposed substitute | Other options |
|---|---|---|---|---|---|
| 1 | Runs `scripts/collect_changes.py` | S1 | No shell or local files in a plan | Remote MCP tool, because S2 to S4 need its JSON. Needs a new service. | Deployment node: passes on only the child run's ID and state, so later nodes can't read the JSON. Human approval step: a person pastes the changes in. |

### Step map

| Step | What it does | Where it goes in the plan |
|---|---|---|
| S1 | Collect the changes | Agent node `collect_changes`, after decision 1 |
| S5 | Get approval | Human-input node `approve_draft` |

### Questions

1. <each choice the user must make, numbered to match the rows above>
```

Every unsupported part from step 3 gets a row in "Parts that need a decision". When a proposed substitute or option means hosting a new service, write "Needs a new service." in it. When a row's system isn't reachable yet, list the options that need no new service: a human approval step, or leaving the step out of the first version. Every step from step 3 gets a row in the step map. A step's "Where it goes" names a node, a merge into another step's node, or a decision row. When you propose leaving a step out of the plan, give it a decision row too.

The step is done when the report has a row for every unsupported part and every step.

## 6. Take the report to the summary

Continue with step 1 of the pipeline in `SKILL.md`, and show the report together with the summary. The user decides every row and confirms the summary in one design approval, so don't ask for decisions on the report alone. Accepting the whole report counts as a decision on every row. When the user's answers change the plan, update the step map and the summary, and show both again.
