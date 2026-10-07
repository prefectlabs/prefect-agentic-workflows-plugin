# Infrastructure check

The check finds out what a workflow can use before the user spends time on its design. An agent node in Prefect Cloud can reach the user's business tools only through a remote MCP server. A step that runs code needs a Prefect deployment. A credential needs a Secret block.

Many users don't know what MCP is. Write every message in this check for that reader. Say "remote MCP server" only next to its plain meaning: a web address that lets an agent use one of your business tools, such as Slack or your helpdesk.

Work through these steps in order.

## 1. Read the workspace

Call `list_secret_blocks` and `list_deployments`. Keep both results for the design and the draft. A Secret block's name often names a system the user already has credentials for, such as `slack-bot-token`.

When a call fails with an error that says how to fix the Prefect profile or the workspace, relay that error and stop. The workflow can't be published until the user fixes it.

## 2. Ask which business tools an agent can reach

Ask in one message, before any design question. Name the systems you already know the workflow needs: from the user's description, from the source skill's stdio MCP servers, scripts, and credentials on the conversion path, or from the Secret block names. On the interview path, ask about the tools the user expects the workflow to touch.

Use wording like this:

> Before we design the workflow, I need to know which of your business tools it can use. A step that an AI agent runs in Prefect Cloud can only use a tool through a *remote MCP server*: a web address that lets an agent use one of your business tools, such as Slack or your helpdesk. Many services offer one. Search the service's help pages for "MCP server". Prefect Cloud connects to that address over the internet, so a server that runs only on your own computer or inside your company network won't work.
>
> For each tool this workflow needs, do you have that web address? If you're not sure, say so, and I'll plan as if you don't.

For each system, record one of these answers:

- **Reachable.** The user gave an `https://` URL on the public internet, with no `localhost`, private IP address, or internal-only host name. The URL has no user name or password, query string (`?`), fragment (`#`), or path parameters (`;`), because a plan can't use those. When a credential is in the URL, ask the user for the base URL and treat the credential as a Secret block for a header. The server must also accept a fixed credential, such as an API key or token sent in a header. A plan can only send a fixed credential from a Secret block. A server that needs an interactive sign-in, such as OAuth in a browser, is not reachable yet. When the user doesn't know which kind the server uses, ask them to check its docs.
- **Not reachable yet.** The user has no URL, isn't sure, or the URL runs only on their machine or network. A stdio MCP server in a source skill is in this group until the user names a remote URL for the same tools.

The step is done when every system the workflow needs has one of these two answers.

## 3. Tell the user what this limits

Send a short note before the checklist or the gap questions. On the conversion path, send it with the conversion report instead. It covers each case below that applies.

- **What the workflow can use.** The reachable systems, the Secret blocks that match them, and the deployments that match a step.
- **A system that isn't reachable yet.** Name the steps that need it. Offer the two options that need no new service: a human approval step, where a person does that part by hand and then continues the run, or leaving the step out of the first version. Then name the third option, a remote MCP server for that system, and say plainly that someone has to find or host that service first.
- **No reachable systems at all.** Say this plainly: every agent step can work only with the text the run gives it, which means run inputs, results from earlier steps, and a person's answers to a form. No step can read from or write to the user's business tools. A workflow that drafts a reply from customer feedback pasted in as a run input still works. A workflow that posts the reply to the helpdesk doesn't.
- **A step that runs code with no matching deployment.** Say that the code has to be set up as a Prefect deployment first, or a person does that step by hand in a human approval step.
- **A new service to host.** Flag each substitute that means hosting a new service, with the words "needs a new service". The most common one: a script whose result a later step needs has to become a tool on a remote MCP server that the user hosts. A Deployment node can't hand a script's result to a later step.

The step is done when the user has read the note and every step that needs an unreachable system has an option attached. Carry each option into the design: the user picks between them in the summary step, and in the conversion report's "Parts that need a decision".
