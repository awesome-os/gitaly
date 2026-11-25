package localrepo

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v19/internal/command"
	"gitlab.com/gitlab-org/gitaly/v19/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
)

// ErrReplayConflict happens when a rebase failed due to a conflict
var ErrReplayConflict = errors.New("replay has conflicts")

type rebaseConfig struct {
	committer *git.Signature
	base      string
}

// RebaseOption is a function that sets a config in rebaseConfig.
type RebaseOption func(*rebaseConfig)

// RebaseWithCommitter provides a signature to be used as the committer for
// generated commits during rebase.
func RebaseWithCommitter(committer git.Signature) RebaseOption {
	return func(options *rebaseConfig) {
		options.committer = &committer
	}
}

// RebaseWithBase provides the commit the branch was originally based on. Only
// commits reachable from the branch but from neither base nor upstream are
// rebased, similar to git-rebase(1) with `--onto <upstream> <base>`. It must be
// an ancestor of the branch. Without it, the merge base of upstream and branch
// is used.
func RebaseWithBase(base string) RebaseOption {
	return func(options *rebaseConfig) {
		options.base = base
	}
}

// Rebase implements a basic support for rebase using git-merge-tree(1), it
// follows what git-rebase(1) does but omits the abundant options.
// Our rebase roughly follows the core logic of git-rebase itself. Specifically,
// we check if fast-forward is possible firstly, if not, we
// 1. generate a *pick* only todo_list using git-rev-list
// 2. process the todo_list using git-merge-tree based cherry-pick
func (repo *Repo) Rebase(ctx context.Context, upstream, branch string, options ...RebaseOption) (git.ObjectID, error) {
	var config rebaseConfig

	for _, option := range options {
		option(&config)
	}

	if config.committer == nil {
		return "", structerr.NewInvalidArgument("committer missing")
	}

	upstreamOID, err := repo.ResolveRevision(ctx, git.Revision(upstream+"^{commit}"))
	if err != nil {
		return "", structerr.NewInvalidArgument("resolving upstream commit: %w", err).WithMetadata("revision", upstream)
	}

	branchOID, err := repo.ResolveRevision(ctx, git.Revision(branch+"^{commit}"))
	if err != nil {
		return "", structerr.NewInvalidArgument("resolving branch commit: %w", err).WithMetadata("revision", branch)
	}

	// Validate the base before the fast-forward checks so that an invalid base
	// is rejected rather than silently ignored.
	var baseOID git.ObjectID
	if config.base != "" {
		baseOID, err = repo.ResolveRevision(ctx, git.Revision(config.base+"^{commit}"))
		if err != nil {
			return "", structerr.NewInvalidArgument("resolving base commit: %w", err).WithMetadata("revision", config.base)
		}

		isAncestor, err := repo.IsAncestor(ctx, baseOID.Revision(), branchOID.Revision())
		if err != nil {
			return "", structerr.NewInternal("checking base ancestry: %w", err)
		}
		if !isAncestor {
			return "", structerr.NewFailedPrecondition("base commit is not an ancestor of branch").WithMetadata("revision", config.base)
		}
	}

	var stdout bytes.Buffer
	if err := repo.ExecAndWait(ctx,
		gitcmd.Command{
			Name: "merge-base",
			Args: []string{upstreamOID.String(), branchOID.String()},
		},
		gitcmd.WithStdout(&stdout),
	); err != nil {
		return "", structerr.NewInternal("get merge-base: %w", err)
	}

	// fast-forward if possible
	mergeBase := text.ChompBytes(stdout.Bytes())
	if mergeBase == upstreamOID.String() {
		return branchOID, nil
	}
	if mergeBase == branchOID.String() {
		return upstreamOID, nil
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return "", structerr.NewInternal("getting object hash %w", err)
	}

	var replayExcludes, mergeTreeExcludes []git.ObjectID
	if config.base != "" {
		// The branch has no commits of its own on top of base.
		if baseOID == branchOID {
			return upstreamOID, nil
		}

		// Upstream stays excluded so that a base only ever drops commits. Otherwise
		// commits the branch got by merging upstream would be replayed again.
		replayExcludes = []git.ObjectID{baseOID, upstreamOID}
		mergeTreeExcludes = []git.ObjectID{baseOID}
	} else {
		mergeBaseOID, err := objectHash.FromHex(mergeBase)
		if err != nil {
			return "", structerr.NewInternal("parse merge-base: %w", err)
		}
		replayExcludes = []git.ObjectID{mergeBaseOID}
	}

	var newOID git.ObjectID
	if featureflag.RebaseWithReplay.IsEnabled(ctx) {
		newOID, err = repo.rebaseUsingReplay(ctx, config, upstreamOID, branchOID, replayExcludes, objectHash)
		if errors.Is(err, ErrReplayConflict) {
			newOID, err = repo.rebaseUsingMergeTree(ctx, config, upstreamOID, branchOID, mergeTreeExcludes, objectHash)
			if err != nil {
				return "", structerr.NewInternal("rebase using merge-tree: %w", err)
			}
		}
		if err != nil {
			return "", structerr.NewInternal("rebase using replay: %w", err)
		}
	} else {
		newOID, err = repo.rebaseUsingMergeTree(ctx, config, upstreamOID, branchOID, mergeTreeExcludes, objectHash)
		if err != nil {
			return "", structerr.NewInternal("rebase using merge-tree: %w", err)
		}
	}

	return newOID, nil
}

func (repo *Repo) rebaseUsingReplay(ctx context.Context, cfg rebaseConfig, upstreamOID, branchOID git.ObjectID, excludeOIDs []git.ObjectID, objectHash git.ObjectHash) (git.ObjectID, error) {
	flags := []gitcmd.Option{
		gitcmd.ValueFlag{Name: "--onto", Value: upstreamOID.String()},
		// Because `--ref` is used together with `--ref-action=print` no
		// ref is updated, we only care about the resulting commit ID,
		// thus it doesn't matter value is passed to `--ref`.
		gitcmd.ValueFlag{Name: "--ref", Value: "refs/temp/replay"},
		gitcmd.ValueFlag{Name: "--ref-action", Value: "print"},
		gitcmd.Flag{Name: "--linearize"},
	}

	env := []string{
		fmt.Sprintf("GIT_COMMITTER_DATE=%s", git.FormatTime(cfg.committer.When)),
		fmt.Sprintf("GIT_COMMITTER_NAME=%s", cfg.committer.Name),
		fmt.Sprintf("GIT_COMMITTER_EMAIL=%s", cfg.committer.Email),
	}

	args := make([]string, 0, len(excludeOIDs)+1)
	for _, excludeOID := range excludeOIDs {
		args = append(args, fmt.Sprintf("^%s", excludeOID))
	}
	args = append(args, branchOID.String())

	var stderr bytes.Buffer
	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name:  "replay",
		Flags: flags,
		Args:  args,
	}, gitcmd.WithStderr(&stderr), gitcmd.WithSetupStdout(), gitcmd.WithEnv(env...))
	if err != nil {
		return "", structerr.NewInternal("start git replay: %w", err)
	}

	var newOID git.ObjectID
	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		if newOID != "" {
			return "", structerr.NewInternal("rebase replay only one ref expected")
		}
		parts := strings.Split(strings.TrimSpace(scanner.Text()), " ")
		if len(parts) != 4 {
			return "", structerr.NewInternal("rebase replay unexpected output format: %q", scanner.Text())
		}
		if parts[0] != "update" {
			return "", structerr.NewInternal("rebase replay expected 'update' instead of %q", parts[0])
		}
		newOID, err = objectHash.FromHex(parts[2])
		if err != nil {
			return "", structerr.NewInternal("rebase replay new OID %q not parsed: %w", parts[2], err)
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scanning replay output: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		exitCode, ok := command.ExitStatus(err)
		if ok && exitCode == 1 {
			return "", ErrReplayConflict
		}
		return "", structerr.NewInternal("git replay: %w", err).WithMetadata("stderr", stderr.String())
	}
	if newOID == "" {
		return "", structerr.NewInternal("git replay gave no result")
	}

	return newOID, nil
}

func (repo *Repo) rebaseUsingMergeTree(ctx context.Context, cfg rebaseConfig, upstreamOID, branchOID git.ObjectID, excludeOIDs []git.ObjectID, objectHash git.ObjectHash) (git.ObjectID, error) {
	// Flags of git-rev-list to get the pick-only todo_list for a rebase.
	// Currently we drop clean cherry-picks and merge commits, which is
	// also what git2go does.
	// The flags are inferred from https://github.com/git/git/blob/v2.41.0/sequencer.c#L5704-L5714
	flags := []gitcmd.Option{
		gitcmd.Flag{Name: "--cherry-pick"},
		gitcmd.Flag{Name: "--right-only"},
		gitcmd.Flag{Name: "--no-merges"},
		gitcmd.Flag{Name: "--topo-order"},
		gitcmd.Flag{Name: "--reverse"},
	}

	var stderr bytes.Buffer
	// The notation "<upstream>...<branch>" is used to calculate the symmetric
	// difference between upstream and branch. It will return the commits that
	// are reachable exclusively from either side but not both. Combined with
	// the provided --right-only flag, the result should be only commits which
	// exist on the branch that is to be rebased.
	args := []string{fmt.Sprintf("%s...%s", upstreamOID, branchOID)}
	for _, excludeOID := range excludeOIDs {
		args = append(args, fmt.Sprintf("^%s", excludeOID))
	}

	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name:  "rev-list",
		Flags: flags,
		Args:  args,
	}, gitcmd.WithStderr(&stderr), gitcmd.WithSetupStdout())
	if err != nil {
		return "", structerr.NewInternal("start git rev-list: %w", err)
	}

	var todoList []string
	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		todoList = append(todoList, strings.TrimSpace(scanner.Text()))
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scanning rev-list output: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return "", structerr.NewInternal("git rev-list: %w", err).WithMetadata("stderr", stderr.String())
	}

	upstreamCommit, err := repo.ReadCommit(ctx, git.Revision(upstreamOID))
	if err != nil {
		return "", fmt.Errorf("reading upstream commit: %w", err)
	}

	oursCommitOID := upstreamOID
	oursTreeOID := git.ObjectID(upstreamCommit.GetTreeId())
	for _, todoItem := range todoList {
		theirsCommit, err := repo.ReadCommit(ctx, git.Revision(todoItem))
		if err != nil {
			return "", fmt.Errorf("reading todo list commit: %w", err)
		}

		opts := []MergeTreeOption{WithAllowUnrelatedHistories()}
		if len(theirsCommit.GetParentIds()) > 0 {
			opts = append(opts, WithMergeBase(git.Revision(theirsCommit.GetParentIds()[0])))
		}

		newTreeOID, err := repo.MergeTree(ctx, oursCommitOID.String(), theirsCommit.GetId(), opts...)
		if err != nil {
			var conflictErr *MergeTreeConflictError
			if errors.As(err, &conflictErr) {
				return newTreeOID, &RebaseConflictError{
					Commit:        theirsCommit.GetId(),
					ConflictError: conflictErr,
				}
			}
			return "", fmt.Errorf("merging todo list commit: %w", err)
		}

		// When no tree changes detected, we need to further check
		// 1. if the commit itself introduces no changes, pick it anyway.
		// 2. if the commit is not empty to start and is not clean cherry-picks of any
		//    upstream commit, but become empty after rebasing, we just ignore it.
		// Refer to https://git-scm.com/docs/git-rebase#Documentation/git-rebase.txt---emptydropkeepask
		if newTreeOID == oursTreeOID {
			if len(theirsCommit.GetParentIds()) == 0 {
				if theirsCommit.GetTreeId() != objectHash.EmptyTreeOID.String() {
					continue
				}
			} else {
				theirsParentCommit, err := repo.ReadCommit(ctx, git.Revision(theirsCommit.GetParentIds()[0]))
				if err != nil {
					return "", fmt.Errorf("reading parent commit: %w", err)
				}

				if theirsCommit.GetTreeId() != theirsParentCommit.GetTreeId() {
					continue
				}
			}
		}

		author := getSignatureFromCommitAuthor(theirsCommit.GetAuthor())

		newCommitOID, err := repo.WriteCommit(ctx, WriteCommitConfig{
			Parents:        []git.ObjectID{oursCommitOID},
			AuthorName:     author.Name,
			AuthorEmail:    author.Email,
			AuthorDate:     author.When,
			CommitterName:  cfg.committer.Name,
			CommitterEmail: cfg.committer.Email,
			CommitterDate:  cfg.committer.When,
			Message:        string(theirsCommit.GetBody()),
			TreeID:         newTreeOID,
		})
		if err != nil {
			return "", fmt.Errorf("write commit: %w", err)
		}
		oursCommitOID = newCommitOID
		oursTreeOID = newTreeOID
	}

	return oursCommitOID, nil
}

// RebaseConflictError encapsulates any conflicting file info and messages that occur
// when a git-merge-tree based rebase fails.
type RebaseConflictError struct {
	Commit        string
	ConflictError *MergeTreeConflictError
}

// Error returns the error string for a rebase conflict error. It is especially designed
// to keep compatible with git2go rebase.
func (c *RebaseConflictError) Error() string {
	return fmt.Sprintf("rebase: commit %q: there are conflicting files", c.Commit)
}

// getSignatureFromCommitAuthor gets a Signature from gitalypb.CommitAuthor. It translates
// CommitAuthor.Timezone into time offsets which can applied to time.Time so that we won't
// lose any timezone info.
func getSignatureFromCommitAuthor(author *gitalypb.CommitAuthor) (signature *git.Signature) {
	signature = &git.Signature{
		Name:  string(author.GetName()),
		Email: string(author.GetEmail()),
		When:  time.Now().In(time.UTC),
	}

	if timestamp := author.GetDate(); timestamp != nil {
		signature.When = timestamp.AsTime().In(time.UTC)
	}

	// care about timezone
	timezone := author.GetTimezone()
	if len(timezone) != 5 {
		return signature
	}

	loc, err := time.Parse("-0700", string(timezone))
	if err != nil {
		return signature
	}

	signature.When = signature.When.In(loc.Location())

	return signature
}
