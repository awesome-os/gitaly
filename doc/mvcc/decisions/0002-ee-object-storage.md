# 2. Object storage is an EE backend behind one interface

Source: [issue 7372](https://gitlab.com/gitlab-org/gitaly/-/issues/7372), decision item 2.

## Context

The cloud backend is licensed under `ee/LICENSE`. Community builds must compile and run without it.

## Decision

Durable storage sits behind the `ObjectStorage` interface in `internal/git/mvcc`. The cloud implementation lives
in `ee/mvcc/objectstorage` and reaches every provider through gocloud. The package itself carries no build tag.
It stays out of community binaries because only files with the `ee` build tag import it, such as
`mvcc_objectstorage_ee.go`, which has `mvcc_objectstorage_stub.go` as its community counterpart. A community
build has the local filesystem backend only, and returns an error when a bucket URL is configured.

Code must:

- Import `ee/` only from files that carry the `ee` build tag.
- Reach the bucket through gocloud. The one exception is `github.com/aws/smithy-go`, which `isCreateConflict`
  uses to recognize an S3 create conflict that gocloud reports as unknown. Any other direct use of a provider
  SDK needs its own decision record.

## Rule

Only files that carry the `ee` build tag import `ee/`, and code reaches a bucket through gocloud.
