//go:build ee

package gitaly

import (
	"context"

	eeobjectstorage "gitlab.com/gitlab-org/gitaly/v19/ee/mvcc/objectstorage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/log"
	"gitlab.com/gitlab-org/gitaly/v19/internal/packfileuri"
)

// packfileURIOffloadManager builds the packfile-URI manager when offload is
// enabled, opening its own handle on the MVCC object-storage bucket; the
// returned cleanup closes it. It returns a nil manager when offload is off.
//
// This file is compiled only into Enterprise Edition builds (-tags ee), like
// the cloud backend the manager reads from; see mvcc_objectstorage_ee.go. The
// bucket handle is separate from the CacheFactory's so the two tear down
// independently.
func packfileURIOffloadManager(ctx context.Context, enabled, dynamicPacks bool, bucketURL string, logger log.Logger) (*packfileuri.Manager, func(context.Context) error, error) {
	if !enabled {
		return nil, nil, nil
	}

	storage, err := eeobjectstorage.OpenStorage(ctx, bucketURL, eeobjectstorage.HashedPathPrefix)
	if err != nil {
		return nil, nil, err
	}

	var opts []packfileuri.ManagerOption
	if dynamicPacks {
		opts = append(opts, packfileuri.WithDynamicPacks())
	}

	return packfileuri.NewManager(packfileuri.NewSink(storage), logger, opts...), storage.TearDown, nil
}
