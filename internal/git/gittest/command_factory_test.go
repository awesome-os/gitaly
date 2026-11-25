package gittest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testcfg"
)

func TestCommandFactory_referenceBackend(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, repoPath := CreateMVCCRepository(t, ctx, cfg)
	WriteCommit(t, cfg, repoPath, WithBranch("main"))

	revParse := gitcmd.Command{Name: "rev-parse", Args: []string{"refs/heads/main"}}
	factory := NewCommandFactory(t, cfg)

	t.Run("wrapper selects the MVCC binary", func(t *testing.T) {
		cmd, err := factory.New(ctx, repo, revParse)
		require.NoError(t, err)
		require.NoError(t, cmd.Wait())
	})

	t.Run("plain factory uses the default binary", func(t *testing.T) {
		cmd, err := factory.ExecCommandFactory.New(ctx, repo, revParse)
		require.NoError(t, err)
		require.Error(t, cmd.Wait())
	})
}

func TestMVCCCacheContext(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	t.Run("MVCC repository", func(t *testing.T) {
		repo, _ := CreateMVCCRepository(t, ctx, cfg)
		require.NotNil(t, mvcc.CacheFromContext(MVCCCacheContext(t, ctx, cfg, repo)))
	})

	t.Run("non-MVCC repository", func(t *testing.T) {
		testhelper.SkipWithMVCC(t, "all repositories use MVCC in this test run")

		repo, _ := CreateRepository(t, ctx, cfg, CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		require.Equal(t, ctx, MVCCCacheContext(t, ctx, cfg, repo))
	})
}
