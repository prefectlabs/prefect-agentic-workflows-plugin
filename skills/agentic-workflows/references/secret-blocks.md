# Secret blocks

A plan refers to a credential through a Prefect Secret block. The plan holds the block's ID, and Prefect Cloud reads the value when a node runs. The value never appears in the plan, the conversation, or the plan file.

## Reference a block

Call `list_secret_blocks`. It returns each block's `name` and `id`, never its value. Put the ID where the credential goes, for example an MCP server header:

```json
"headers": {
  "Authorization": {"$ref": {"block_document_id": "<id from list_secret_blocks>"}}
}
```

When several blocks could fit, show the user their names and ask which one to use.

## When the block is missing

Give the user these steps, and ask them to tell you the block's name when they are done. Suggest a name that says what the credential is for, such as `slack-bot-token`.

In the Prefect Cloud UI:

1. Open the workspace and go to **Blocks**.
2. Click **+** to add a block, and choose **Secret**.
3. Enter the block name and the secret value, then click **Create**.

From a terminal with the same Prefect profile, `prefect block create secret` prints a link to the same form.

Then call `list_secret_blocks` again and use the new block's ID. If the user pastes a secret value into the conversation, don't copy it into the plan file or any tool call. Tell the user to store it in a Secret block and to rotate the credential, since it is now in the transcript.

## When publishing can't see a block

`publish_plan` checks that the publisher can see every Secret block the plan references. When this check fails, the block may have been deleted, the ID may be wrong, or the user's API key may lack access to Secret blocks. Call `list_secret_blocks` to check the ID, and ask the user to check the key's permissions.
