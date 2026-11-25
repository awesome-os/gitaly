# 1. One flag enables MVCC for new repositories

Source: [issue 7372](https://gitlab.com/gitlab-org/gitaly/-/issues/7372), decision item 1.

## Context

The `poc/scaling-git` branch had its own `S3BackendEnabled` switch beside the feature flag, so a repository's
backend depended on two settings.

## Decision

The `new_repo_mvcc_backend` feature flag is the only switch that puts a new repository on MVCC. The
`mvcc_migration` feature flag moves existing repositories to MVCC and is the only other switch. The storage
backend follows from `[mvcc] object_storage_bucket_url`: when it is set the state lives in the bucket, and when
it is empty the state lives on the local filesystem.

## Rule

Production code adds no further flag, configuration key or environment variable that turns MVCC or one of its
storage backends on or off.
