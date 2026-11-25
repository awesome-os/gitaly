//go:build ee

package gitaly

import (
	"context"

	eeobjectstorage "gitlab.com/gitlab-org/gitaly/v19/ee/mvcc/objectstorage"
	gitmvcc "gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
)

// mvccObjectStorageOptions returns the CacheFactory options that back the MVCC
// reference backend with the cloud object store at url. When url is empty it
// returns no options, leaving the factory on the local filesystem.
//
// The returned Storage owns the bucket and closes it from its TearDown, which
// the CacheFactory runs on Close, so no separate close handler is needed here.
//
// This file is compiled only into Enterprise Edition builds (-tags ee); the
// cloud backend it wires in is licensed under ee/LICENSE. The gocloud blob
// drivers (s3://, gs://, azblob://) are registered by importing the ee
// objectstorage package. Object storage is a single cluster-wide bucket, so the
// factory ignores the local storage path and returns the shared Storage;
// repository relative paths namespace within the bucket via HashedPathPrefix.
func mvccObjectStorageOptions(ctx context.Context, url string) ([]gitmvcc.CacheOption, error) {
	if url == "" {
		return nil, nil
	}

	storage, err := eeobjectstorage.OpenStorage(ctx, url, eeobjectstorage.HashedPathPrefix)
	if err != nil {
		return nil, err
	}

	factory := func(string) (gitmvcc.ObjectStorage, error) {
		return storage, nil
	}
	return []gitmvcc.CacheOption{gitmvcc.WithObjectStorageFactory(factory)}, nil
}
