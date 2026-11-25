# 6. Route, do not relay

Source: [issue 7372](https://gitlab.com/gitlab-org/gitaly/-/issues/7372), decision item 9.

## Context

The `poc/scaling-git` branch forwarded an RPC from the node that received it to the node that could serve it,
through an RPC proxy.

## Decision

A request is sent to a node that can serve it. A Gitaly node never forwards an RPC to another Gitaly node, and
the proof of concept's RPC proxy is not ported. Routing itself is not built yet.

## Rule

No code forwards or proxies an RPC from one Gitaly node to another.
