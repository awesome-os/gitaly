# AGENTS.md

Entrypoint for agents working on Gitaly.

This document follows the [AGENTS.md](https://agents.md) format. [Agent Skills](https://agentskills.io) are documented under the `skills/` directory.

## Context loading

Read the relevant context before starting a task:

- Working in the codebase: layout, build/test/lint commands, Go/error/proto conventions →
  [doc/development.md](doc/development.md), [STYLE.md](STYLE.md)
- Adding or changing a CLI command or flag (`gitaly`/`praefect`) → [doc/help_text_style_guide.md](doc/help_text_style_guide.md)
- Commits, merge requests, and code review → [CONTRIBUTING.md](CONTRIBUTING.md), [REVIEWING.md](REVIEWING.md), [STYLE.md](STYLE.md)
- Running or writing tests → [doc/development.md](doc/development.md), [STYLE.md](STYLE.md)
- Observability: logs, metrics, traces, and Git-level instrumentation →
  [doc/observability.md](doc/observability.md) for which signal answers which question, then
  [doc/logging.md](doc/logging.md) or [doc/trace2_integration.md](doc/trace2_integration.md).
  Logging *conventions* live in [`STYLE.md`](STYLE.md#logging) instead
- Architecture → [doc/DESIGN.md](doc/DESIGN.md), [doc/design_ha.md](doc/design_ha.md)
- MVCC code (`internal/git/mvcc`, `ee/mvcc`, `internal/grpc/middleware/mvcc`) → the design decisions in
  [doc/mvcc/decisions/](doc/mvcc/decisions/README.md)

## Always-on rules

- Apply the smallest change that solves the problem; prefer the standard library and already-vendored
  dependencies over new ones, and deletion over addition; follow the existing patterns in the file you edit.
- Keep code comments short and precise, and follow the comment conventions in
  [STYLE.md](STYLE.md#comments): say why the code is the way it is rather than what it does, open a doc
  comment with one self-contained sentence, and leave the change history to the commit message.
- Read [CONTRIBUTING.md](CONTRIBUTING.md#commits) before writing a commit message and follow the
  commit hygiene it describes. This applies to every commit, including amends and rebases.
- Open agent-authored Draft merge requests with `[skip ci]` at the end of the title to skip
  unnecessary merge request pipelines while iterating. When ready for CI, remove `[skip ci]` and
  start a pipeline. Editing the title alone starts no pipeline.

## Project notes

- Gitaly is a gRPC server that provides RPC access to Git repositories by running Git close to disk.
- **Gitaly can run in two distinct deployment modes:**
  - *Standalone (sharded):* GitLab connects to Gitaly nodes directly and shards repositories across
    them. A complete, common deployment with no Praefect.
  - *Gitaly Cluster (Praefect):* Praefect proxies and replicates across multiple Gitaly nodes for
    high availability, backed by a PostgreSQL state store.
