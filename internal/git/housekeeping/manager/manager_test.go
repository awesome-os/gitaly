package manager

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
)

func TestRepositoryManager_TryLockRepository(t *testing.T) {
	t.Parallel()

	m := New(prometheus.Config{}, testhelper.NewLogger(t), nil)
	repo := &gitalypb.Repository{StorageName: "default", RelativePath: "repo.git"}

	unlock, ok := m.TryLockRepository(repo)
	require.True(t, ok)

	running, _ := m.repositoryStates.tryRunningHousekeeping(repo)
	require.False(t, running, "housekeeping must not run while the repository is locked")
	_, ok = m.TryLockRepository(repo)
	require.False(t, ok, "the repository can only be locked once")

	other, ok := m.TryLockRepository(&gitalypb.Repository{StorageName: "default", RelativePath: "other.git"})
	require.True(t, ok, "other repositories are not affected")
	other()

	unlock()
	running, done := m.repositoryStates.tryRunningHousekeeping(repo)
	require.True(t, running, "housekeeping runs again once the repository is unlocked")
	done()
}
