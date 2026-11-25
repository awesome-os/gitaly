# 4. Every publish goes through the lock, the merge and the retry

Source: [issue 7372](https://gitlab.com/gitlab-org/gitaly/-/issues/7372), decision item 4.

## Context

Two writers that prepare from the same head both try to advance it. Without conflict handling, the loser fails
even when the two writes touch different references.

## Decision

A publish takes the per-repository publish lock and attempts the compare-and-swap. When it loses, it runs a
three-way manifest merge against the winner and retries. A real overlap on the same reference is returned to the
client. `PublishSerialized` is the one entry point. The single-shot `LocalCache.Commit` is kept for tests only
and is due for removal in [issue 7567](https://gitlab.com/gitlab-org/gitaly/-/issues/7567).

## Rule

No publish skips the publish lock or the retry loop, and no new production code calls the single-shot commit.
