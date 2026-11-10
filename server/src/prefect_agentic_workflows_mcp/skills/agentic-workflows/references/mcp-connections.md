# MCP connections

An MCP connection saves a remote MCP server and its sign-in in Prefect Cloud. An agent node names the connection by ID, and Prefect Cloud adds the credentials when the node starts, so the plan holds no credential and you never see one. Prefect Cloud also refreshes OAuth tokens, so a server that needs a browser sign-in works too.

## Find or create a connection

1. Call `list_mcp_connections`. When one has the server's URL and the account the user wants, use it. When its `status.state` is `authorization_needed` or `reauthorization_required`, send the user its `setup_url` and call `check_mcp_connection` after they finish, as in steps 4 and 5. Each person can have their own connection to the same server, named after their account.
2. Otherwise call `discover_mcp_server` with the URL, and create the connection from its answer with `create_mcp_connection`:

   | Answer | Create with |
   |---|---|
   | `requires_auth` is false | no `oauth` and no `headers` |
   | `oauth` is set | `oauth`, usually `{}` |
   | `requires_auth` is true and `oauth` is null | `headers`, with the header name and prefix from the provider's documentation, such as `Authorization` with `value_prefix` `Bearer ` |

   When the user says to use an API key for a server that also offers OAuth, follow the user and create it with `headers`. Don't send `Authorization` as a header on an OAuth connection; Prefect Cloud sets it from the token.
3. Name the connection after the provider and the account, such as `GitHub (eddie-park)`. Names are unique in the workspace. Ask the user for the account name when you don't know it.
4. `create_mcp_connection` opens the setup page, where the user signs in or enters each header value. When `setup` is `client_cannot_open_links` or `declined`, send the user `connection.setup_url` and say what they'll do there. When `oauth.supports_dcr` from `discover_mcp_server` was false, tell them first that they need the client ID, and the secret if it has one, of an OAuth app they registered at the provider.
5. After the user finishes, call `check_mcp_connection`. `succeeded` means the server listed its tools with the credentials. It doesn't show that each tool call is allowed.

Never ask the user to type a token, key, or password in this conversation. If they paste one, don't use it: tell them to rotate it and to enter the new one on the setup page.

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

## Access and repair

- `mcp_connection_access_denied` from `validate_plan` or `publish_plan` means the user can't reference that connection. Its message names the connection. Tell the user to ask a workspace owner for access, or to create their own connection to the server.
- Creating a connection needs permission to manage MCP connections, which the Owner and Developer roles have. When `create_mcp_connection` is refused, tell the user to ask a workspace owner.
- A run's node that fails with `mcp_connection_unavailable` or `mcp_authentication_failed` uses a connection that needs the user. Read the node's connections with `get_mcp_connection`, and send the `setup_url` of each one whose `status.state` is `authorization_needed` or `reauthorization_required`.
- When `list_mcp_connections` says MCP connections aren't enabled for the account, use an inline `http` server instead, with its credential header as a Secret block reference. See [design-guidance.md](design-guidance.md).
