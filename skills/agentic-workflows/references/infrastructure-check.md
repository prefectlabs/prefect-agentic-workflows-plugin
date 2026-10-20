# Infrastructure check

Find out what the workflow can use before the user spends time on its design. An agent node reaches business tools only through a remote MCP server, and credentials live in Secret blocks. Code that needs the user's systems runs in a Prefect deployment; self-contained commands can run in the agent node's sandbox.

Many users don't know what MCP is. Write for that reader.

1. Call `list_secret_blocks`, `list_deployments`, and `list_provider_connections`. When a call fails with an error about the Prefect profile or workspace, relay it and stop.
2. Ask which business tools the workflow needs and whether each has a remote MCP server. Send this question as a message of its own, and wait for the answer: the design questions, the summary, and any conversion report come after it. When you don't know yet what the workflow does, first ask the user to describe it in a sentence or two, so you know which systems to ask about. Use wording like:

   > A step that an AI agent runs in Prefect Cloud can only use a business tool, such as Slack or your helpdesk, through a *remote MCP server*: a web address that lets the agent use that tool. Many services offer one; search their help pages for "MCP server". It has to be reachable over the internet, and if it needs a sign-in, it has to accept an API key or token rather than a browser sign-in. For each tool this workflow needs, do you have that address? If you're not sure, I'll plan as if you don't.

   A system is **reachable** when the user gives a public `https://` URL that fits the limits in [design-guidance.md](design-guidance.md) and needs either no sign-in or a fixed credential. Anything else is **not reachable yet**, including a stdio MCP server in a source skill.
3. Tell the user, briefly, what this limits. For each unreachable system, name the steps that need it and offer a human approval step (a person does that part) or leaving the step out. A remote MCP server is a third option that needs a new service. With no reachable systems, every agent step works only with run inputs, earlier results, and form answers. A step that runs code needs a deployment first. On the conversion path, send this note with the conversion report, after the user has answered step 2.
4. When `list_provider_connections` returns none, ask in the same note which model the agent steps should use, and say Prefect's models are the easiest start. [Models](../SKILL.md#models) covers the choices.

The check is done when every system the workflow needs is reachable or has an option the user can pick in the summary.
