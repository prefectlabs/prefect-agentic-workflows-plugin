# Release notes style guide

## Sections

- Use these section headings, in this order: `Features`, `Fixes`, `Documentation`, `Other`.
- Leave out a section that has no entries.
- Put `chore` changes under `Other` only when a user would notice them, for example a dependency bump that changes the minimum Python version. Drop the rest.

## Entries

- Write one line per entry.
- Start each entry with a verb in the present tense: "Adds", "Fixes", "Removes".
- Describe what changed for the user, not how the code changed.
- End each entry with the pull request link and the author's handle, like this: `(#482 by @octocat)`.
- An entry for a change with no pull request ends with the short SHA instead: `(3f9c2ab)`.

## Breaking changes

- Mark a breaking change with `**Breaking:**` at the start of its entry.
- List breaking changes first within their section.
- Every breaking change entry says what the user must do to upgrade.
