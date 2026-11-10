# MCP connections

An MCP connection saves a remote MCP server and its credential in Prefect Cloud: a person's sign-in, a fixed credential such as an API key, or a service account the connection acts as. An agent node names the connection by ID, and Prefect Cloud adds the credentials when the node starts, so the plan holds no credential and you never see one. Prefect Cloud also refreshes tokens, so a server that needs a browser sign-in works too.

## Find or create a connection

1. Call `list_mcp_connections`. When one has the server's URL and the account the user wants, use it. When its `status.state` is `authorization_needed` or `reauthorization_required`, send the user its `setup_url` and call `check_mcp_connection` after they finish, as in steps 4 and 5. Each person can have their own connection to the same server, named after their account.
2. Otherwise call `discover_mcp_server` with the URL, and create the connection from its answer with `create_mcp_connection`:

   | Answer | Create with |
   |---|---|
   | `requires_auth` is false | no `oauth` and no `headers` |
   | `oauth` is set | `oauth`, usually `{}` |
   | `requires_auth` is true and `oauth` is null | `headers`, with the header name and prefix from the provider's documentation, such as `Authorization` with `value_prefix` `Bearer ` |
   | a Google Cloud API on `googleapis.com`, such as `https://bigquery.googleapis.com/mcp` | `service_identity` with `type` `google_service_account`, unless the user asks to sign in themselves |

   When the user says to use an API key for a server that also offers OAuth, follow the user and create it with `headers`. When the user names a service account or asks for a machine identity, create it with `service_identity`; Prefect Cloud accepts that only for `googleapis.com` URLs. Don't send `Authorization` as a header on an OAuth or service identity connection; Prefect Cloud sets it from the token.
3. Name the connection after the provider and the account, such as `GitHub (eddie-park)`. Names are unique in the workspace. Ask the user for the account name when you don't know it.
4. `create_mcp_connection` opens the setup page, where the user signs in, enters each header value, or pastes the service account's key file and names the Google Cloud project to bill. When `setup` is `client_cannot_open_links` or `declined`, send the user `connection.setup_url` and say what they'll do there. When `oauth.supports_dcr` from `discover_mcp_server` was false, tell them first that they need the client ID, and the secret if it has one, of an OAuth app they registered at the provider.
5. After the user finishes, call `check_mcp_connection`. `succeeded` means the server listed its tools with the credentials. It doesn't show that each tool call is allowed.

Never ask the user to type a token, key, key file, or password in this conversation. If they paste one, don't use it: tell them to rotate it and to enter the new one on the setup page.

The user can finish setup after the design: a plan that references a connection that isn't set up yet still publishes. `publish_plan` lists it in `version.warnings` with its `setup_url`, and runs fail until the user finishes it.

## Reference a connection in a plan

Add the connection to the agent node's `mcp.mcpServers` map under a short alias:

```json
"mcp": {
  "mcpServers": {
    "issues": { "type": "connection", "connection_id": "<id from list_mcp_connections>" }
  }
}
```

Give a node only the connections it uses.

## Limit a node to some of a connection's tools

Without `tools`, the node's agent gets every tool the server lists. Add `tools` to the server entry to give it only the tools you name:

```json
"issues": {
  "type": "connection",
  "connection_id": "<id>",
  "tools": ["search_issues", "get_issue"]
}
```

1. Read the tools with `get_mcp_connection`. `last_check.tools` has each tool's `name`, `description`, and the server's `read_only` and `destructive` hints. When it is null, call `check_mcp_connection` first.
2. Choose the tools the node's objective needs, and no others. A node that only reads gets only tools whose `read_only` is true. A hint is the server's own claim, and null means the server gave none, so read the description too.
3. Write the names exactly as `name` gives them. The agent's tool names start with the server alias, but `tools` takes the server's own names.
4. Tell the user which tools each node gets in the summary, and name every tool that makes changes or whose `destructive` is true.

Leave `tools` out when the node needs most of the server's tools, or when the user wants the node to pick up tools the provider adds later. A node with `tools` never gets a new tool until you add its name and publish again.

`tools` also works on an inline `http` server. No tool list is stored for one, so take the names from the provider's documentation.

## Access and repair

- `mcp_connection_access_denied` from `validate_plan` or `publish_plan` means the user can't reference that connection. Its message names the connection. Tell the user to ask a workspace owner for access, or to create their own connection to the server.
- Creating a connection needs permission to manage MCP connections, which the Owner and Developer roles have. When `create_mcp_connection` is refused, tell the user to ask a workspace owner.
- A `mcp_tool_not_listed` warning from `publish_plan`, or a node that fails with that code, means the server has no tool by one of the node's `tools` names. Read the current names with `check_mcp_connection`, fix the plan, and publish again.
- A node that fails with `tool_selection_not_enabled` has `tools`, and the account can't limit tools yet. Tell the user to ask Prefect to turn the feature on, or remove `tools` and publish again.
- A run's node that fails with `mcp_connection_unavailable` or `mcp_authentication_failed` uses a connection that needs the user. Read the node's connections with `get_mcp_connection`, and send the `setup_url` of each one whose `status.state` is `authorization_needed` or `reauthorization_required`.
- When `list_mcp_connections` says MCP connections aren't enabled for the account, use an inline `http` server instead, with its credential header as a Secret block reference. See [design-guidance.md](design-guidance.md).
