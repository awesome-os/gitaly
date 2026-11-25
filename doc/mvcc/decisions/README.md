# MVCC design decisions

Each file in this directory records one decision about the MVCC backend that code is expected to follow.
The decisions were agreed in [issue 7372](https://gitlab.com/gitlab-org/gitaly/-/issues/7372), which reconciles
the `master` and `poc/scaling-git` implementations.

- [1. One flag enables MVCC for new repositories](0001-one-flag.md)
- [2. Object storage is an EE backend behind one interface](0002-ee-object-storage.md)
- [3. One head per repository, on a generation log](0003-generation-log-head.md)
- [4. Every publish goes through the lock, the merge and the retry](0004-serialized-publish.md)
- [5. Publish is the visibility boundary](0005-publish-is-visibility.md)
- [6. Route, do not relay](0006-route-do-not-relay.md)

## How the records are used

- Reviewers and authors cite a record by number when a change touches the area it covers.
- Duo Code Review checks MVCC changes against the rule at the end of each record. The instruction in
  `.gitlab/duo/mr-review-instructions.yaml` points Duo at this directory, so a rule is written in its record only.

## Adding or changing a record

1. Agree the decision in an issue first. A record states a decision that is already taken.
1. Add a file named `NNNN-short-title.md` with the sections `Context`, `Decision` and `Rule`. Keep `Rule` to one
   sentence that a reviewer can check against a diff.
1. Add the record to the list above.
1. To reverse a decision, change its record in the same merge request as the code that departs from it.
