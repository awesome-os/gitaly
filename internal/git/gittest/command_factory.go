package gittest

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/command"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
)

// CommandFactory is a Git command factory for tests. Tests often run Git with a bare context,
// outside of the MVCC middleware that marks the context in production. So it marks the context
// itself for MVCC repositories: only the MVCC binary can read them.
type CommandFactory struct {
	*gitcmd.ExecCommandFactory
	locator storage.Locator
}

// WrapCommandFactory wraps factory so that it selects the MVCC binary for MVCC repositories.
func WrapCommandFactory(cfg config.Cfg, factory *gitcmd.ExecCommandFactory) *CommandFactory {
	return &CommandFactory{
		ExecCommandFactory: factory,
		locator:            config.NewLocator(cfg),
	}
}

// New creates a new Git command for the given repository with the binary that can read it.
func (f *CommandFactory) New(ctx context.Context, repo storage.Repository, sc gitcmd.Command, opts ...gitcmd.CmdOpt) (*command.Command, error) {
	return f.ExecCommandFactory.New(f.withMVCC(ctx, repo), repo, sc, opts...)
}

// withMVCC returns ctx carrying the MVCC marker if repo exists and uses MVCC. It keeps a marker the
// caller already put in ctx, like the MVCC binary that migrates a repository to MVCC. Unlike in
// production, a marker cleared with gitcmd.ContextWithoutMVCC is re-detected from disk.
func (f *CommandFactory) withMVCC(ctx context.Context, repo storage.Repository) context.Context {
	if gitcmd.MVCCFromContext(ctx) {
		return ctx
	}

	repoPath, err := f.locator.GetRepoPath(ctx, repo, storage.WithRepositoryVerificationSkipped())
	if err != nil {
		return ctx
	}

	return contextWithDetectedMVCC(ctx, repoPath)
}

// NewCommandFactory creates a new Git command factory. It marks the context for MVCC repositories,
// see CommandFactory.
func NewCommandFactory(tb testing.TB, cfg config.Cfg, opts ...gitcmd.ExecCommandFactoryOption) *CommandFactory {
	tb.Helper()
	factory, cleanup, err := gitcmd.NewExecCommandFactory(cfg, testhelper.SharedLogger(tb), opts...)
	if err != nil {
		if strings.Contains(err.Error(), "no such file or directory") {
			require.FailNowf(tb, "Gitaly tests execute against the bundled Git environment. Have you run `make build-bundled-git`? Error: %s", err.Error())
		}
	}
	require.NoError(tb, err)
	tb.Cleanup(cleanup)
	return WrapCommandFactory(cfg, factory)
}
