# 3. One head per repository, on a generation log

Source: [issue 7372](https://gitlab.com/gitlab-org/gitaly/-/issues/7372), decision item 3.

## Context

Several nodes write the same repository. They need one answer to "what is the current state" that no node can
overwrite by accident.

## Decision

In object storage, a repository's head is the newest entry of an append-only generation log. A writer advances
the head by creating generation N+1 only if it does not exist, so exactly one writer wins. The local filesystem
backend keeps one mutable pointer file.

Code must:

- Read the head through the storage interface, and carry its generation together with its hash.
- Never overwrite a generation entry.

## Rule

In object storage, a repository's head is written only by creating the next generation if it is absent.
