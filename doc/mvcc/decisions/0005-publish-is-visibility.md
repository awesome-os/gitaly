# 5. Publish is the visibility boundary

Source: [issue 7372](https://gitlab.com/gitlab-org/gitaly/-/issues/7372), decision items 5 and 6.

## Context

Gitaly normally hides in-flight objects in a quarantine directory. The `poc/scaling-git` branch also tracked
in-flight writes in a session registry shared between RPCs.

## Decision

What an RPC writes stays in that RPC's `mvcc.Cache` until its manifest is published. The publish makes the write
visible to every other RPC and node at once. MVCC repositories use no quarantine directory and no session
registry.

On `master` the decision is partly in place:

- Operations RPCs publish through `mvcc.Cache`.
- Service code still creates a quarantine directory with no MVCC check, in the operations, conflicts, diff and
  repository services. [Issue 7458](https://gitlab.com/gitlab-org/gitaly/-/issues/7458) tracks `FetchRemote`,
  where the quarantine breaks MVCC repositories. The other callers are open under decision item 6.
- A push is not yet published, because the publish point in the receive-report hook is not ported (decision
  item 5).

## Rule

No new code uses a quarantine directory on an MVCC repository, and write state is kept only in the per-RPC cache.
