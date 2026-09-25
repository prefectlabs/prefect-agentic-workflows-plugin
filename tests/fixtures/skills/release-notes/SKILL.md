---
name: release-notes
description: Draft and publish GitHub release notes for a repository. Use when the user asks for release notes, a changelog entry, or a draft release for a new tag.
---

# Release notes

Draft release notes for the changes since the last tag, get the user's approval, then publish them as a draft GitHub release.

## Setup

This skill uses the GitHub MCP server. If the `github` server is not connected, ask the user to add it:

```bash
claude mcp add github -e GITHUB_PERSONAL_ACCESS_TOKEN -- \
  docker run -i --rm -e GITHUB_PERSONAL_ACCESS_TOKEN ghcr.io/github/github-mcp-server
```

## Steps

1. **Collect the changes.** If the request doesn't name the new tag, ask the user for it. From the root of the user's repository, run this skill's script:

   ```bash
   python3 <this skill's directory>/scripts/collect_changes.py --until HEAD
   ```

   The script finds the latest tag and prints JSON with every commit since it: the SHA, the subject, the pull request number if there is one, and a change type (`feature`, `fix`, `docs`, `chore`, or `other`). Pass `--since <tag or commit>` to pick a different starting point. Keep the JSON. Every later step reads from it.

2. **Look up the pull requests.** For each change with a pull request number, use the `github` MCP server to read the pull request's title, body, labels, and author. A pull request labeled `skip-changelog` is left out of the notes. When the body has a "Release note" section, use that text for the entry.

3. **Write the draft.** Group the entries under `Features`, `Fixes`, `Documentation`, and `Other`, following [references/style.md](references/style.md). Credit each author with their GitHub handle. Save the draft to `release-notes.md`.

4. **Check the draft until it is clean.** Check `release-notes.md` against every rule in the style guide. Fix each problem you find, then check the whole file again. Repeat until one full pass finds no problems. Every change from step 1 must appear exactly once, except pull requests labeled `skip-changelog` and `chore` changes the style guide says to drop.

5. **Get approval.** Show the user the full draft and the tag it will be published under. Wait for the user to approve it or ask for edits. Apply any edits and show the draft again. Continue only after the user approves.

6. **Publish.** Use the `github` MCP server to create a draft release for the tag with the approved notes as its body. Give the user the link to the draft release.
