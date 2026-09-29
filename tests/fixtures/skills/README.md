# Fixture skills

These skills are inputs for testing the "convert a skill" path of the `agentic-workflows` skill. They are not installed or run by the test suite.

## release-notes

`release-notes/` drafts GitHub release notes and publishes them as a draft release. It has one part for each case the conversion report must handle:

| Part | Where | Substitute the conversion report proposes |
|---|---|---|
| Local script whose result a later step needs | Step 1 runs `scripts/collect_changes.py`, and steps 2 to 4 read its JSON | A remote MCP tool, because a Deployment node passes on only the child run's ID and state |
| stdio MCP server | Setup adds the `github` server with `docker run -i`, and steps 2 and 6 use it | A remote Streamable-HTTP MCP server |
| Repeat-until-done loop | Step 4 checks and fixes the draft until a full pass finds no problems | A fixed number of check passes, or a human checkpoint |
| Human approval | Step 5 waits for the user to approve the draft | A human-input node |

The skill also has a reference file, `references/style.md`, so the conversion path must read the whole skill directory to find the rules step 4 checks against.

## post-review

`post-review/` writes a short blog post and has a reviewer check it. Its only unsupported part is a loop: step 3 repeats the review and the revision until the reviewer is happy. The `unsupported-loop` evaluation scenario converts it and checks that the conversion report proposes a fixed number of passes or a human checkpoint, and that nothing is published before the user decides.
