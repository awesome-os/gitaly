//go:build !ee

package gitaly

import (
	"context"
	"fmt"

	gitmvcc "gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
)

// mvccObjectStorageOptions returns no options when url is empty, leaving the
// MVCC backend on the local filesystem. A non-empty url requests the cloud
// object-storage backend, which is an Enterprise Edition feature (see
// ee/LICENSE) compiled in only with -tags ee; this community build fails fast
// rather than silently falling back to the local filesystem.
func mvccObjectStorageOptions(_ context.Context, url string) ([]gitmvcc.CacheOption, error) {
	if url == "" {
		return nil, nil
	}

	return nil, fmt.Errorf("MVCC cloud object storage requires an Enterprise Edition build (build with -tags ee)")
}
