//go:build !ee

package gitaly

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v19/internal/log"
	"gitlab.com/gitlab-org/gitaly/v19/internal/packfileuri"
)

// packfileURIOffloadManager returns no manager when offload is off. Offload
// reads from the MVCC cloud object storage, an Enterprise Edition feature
// (see ee/LICENSE) compiled in only with -tags ee; this community build fails
// fast rather than silently serving inline packs. In practice the bucket URL
// the offload requires is already rejected by mvccObjectStorageOptions first.
func packfileURIOffloadManager(_ context.Context, enabled, _ bool, _ string, _ log.Logger) (*packfileuri.Manager, func(context.Context) error, error) {
	if !enabled {
		return nil, nil, nil
	}

	return nil, nil, fmt.Errorf("packfile-URI offload requires an Enterprise Edition build (build with -tags ee)")
}
