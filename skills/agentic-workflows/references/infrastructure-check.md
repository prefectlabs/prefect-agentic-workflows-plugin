# Infrastructure check

Find out what the workflow can use before the user spends time on its design. An agent node reaches business tools only through a remote MCP server, code runs only in a Prefect deployment, and credentials live in Secret blocks.

Many users don't know what MCP is. Write for that reader.

1. Call `list_secret_blocks` and `list_deployments`. When a call fails with an error about the Prefect profile or workspace, relay it and stop.
2. In one message, before any design question, ask which business tools the workflow needs and whether each has a remote MCP server. Use wording like:

   > A step that an AI agent runs in Prefect Cloud can only use a business tool, such as Slack or your helpdesk, through a *remote MCP server*: a web address that lets the agent use that tool. Many services offer one; search their help pages for "MCP server". It has to be reachable over the internet and accept an API key or token, not a browser sign-in. For each tool this workflow needs, do you have that address? If you're not sure, I'll plan as if you don't.

   A system is **reachable** when the user gives a public `https://` URL that fits the limits in [design-guidance.md](design-guidance.md) and accepts a fixed credential. Anything else is **not reachable yet**, including a stdio MCP server in a source skill.
3. Tell the user, briefly, what this limits. For each unreachable system, name the steps that need it and offer a human approval step (a person does that part) or leaving the step out. A remote MCP server is a third option that needs a new service. With no reachable systems, every agent step works only with run inputs, earlier results, and form answers. A step that runs code needs a deployment first. On the conversion path, send this note with the conversion report.

The check is done when every system the workflow needs is reachable or has an option the user can pick in the summary.
