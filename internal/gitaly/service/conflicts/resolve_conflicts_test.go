package conflicts

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha1"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v19/internal/signature"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestResolveConflicts(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testResolveConflicts)
}

func testResolveConflicts(t *testing.T, ctx context.Context) {
	type setupData struct {
		cfg                  config.Cfg
		requestHeader        *gitalypb.ResolveConflictsRequest_Header
		requestsFilesJSON    []*gitalypb.ResolveConflictsRequest_FilesJson
		client               gitalypb.ConflictsServiceClient
		repo                 *gitalypb.Repository
		repoPath             string
		expectedContent      map[string]map[string][]byte
		expectedCommitAuthor *gitalypb.CommitAuthor
		expectedResponse     *gitalypb.ResolveConflictsResponse
		expectedError        error
		skipCommitCheck      bool
		additionalChecks     func()
	}

	conflictResolutionCommitMessage := "Solve conflicts"
	defaultUser := gittest.TestUser
	defaultTimestamp := &timestamppb.Timestamp{Seconds: 1234567890}
	defaultCommitAuthor := &gitalypb.CommitAuthor{
		Name:     []byte("Jane Doe"),
		Email:    []byte("janedoe@gitlab.com"),
		Date:     defaultTimestamp,
		Timezone: []byte(gittest.TimezoneOffset),
	}

	for _, tc := range []struct {
		desc  string
		setup func(testing.TB, context.Context) setupData
	}{
		{
			"no conflict present",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "b", Mode: "100644", Content: "apricot"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "c", Mode: "100644", Content: "acai"}))

				filesJSON, err := json.Marshal([]map[string]interface{}{})
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON},
					},
					expectedCommitAuthor: defaultCommitAuthor,
					expectedResponse:     &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"b": []byte("apricot"),
							"c": []byte("acai"),
						},
					},
				}
			},
		},
		{
			"single file conflict, pick ours",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				baseCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
				))

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apricot"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "acai"}))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 1, 1): "head",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON[:50]},
						{FilesJson: filesJSON[50:]},
					},
					expectedCommitAuthor: defaultCommitAuthor,
					expectedResponse:     &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"a": []byte("apricot"),
						},
					},
				}
			},
		},
		{
			"single file conflict, pick theirs",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				baseCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
				))

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apricot"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "acai"}))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 1, 1): "origin",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON[:50]},
						{FilesJson: filesJSON[50:]},
					},
					expectedCommitAuthor: defaultCommitAuthor,
					expectedResponse:     &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"a": []byte("acai"),
						},
					},
				}
			},
		},
		{
			"single file in directory conflict, pick ours",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				baseCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "subdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
						{Path: "a", Mode: "100644", Content: "apple"},
					})}),
				)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("ours"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "subdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
							{Path: "a", Mode: "100644", Content: "apricot"},
						})}),
				)
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "subdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
							{Path: "a", Mode: "100644", Content: "acai"},
						})}),
				)

				files := []map[string]interface{}{
					{
						"old_path": "subdir/a",
						"new_path": "subdir/a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("subdir/a")), 1, 1): "head",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON[:50]},
						{FilesJson: filesJSON[50:]},
					},
					expectedCommitAuthor: defaultCommitAuthor,
					expectedResponse:     &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"subdir/a": []byte("apricot"),
						},
					},
				}
			},
		},
		{
			"single file conflict without ancestor, pick ours",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apricot"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "acai"}))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 1, 1): "head",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON[:50]},
						{FilesJson: filesJSON[50:]},
					},
					expectedCommitAuthor: defaultCommitAuthor,
					expectedResponse:     &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"a": []byte("apricot"),
						},
					},
				}
			},
		},
		{
			"single file multi conflict",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				baseCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple\n" + strings.Repeat("filler\n", 10) + "banana"},
				))

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{
						Path: "a", Mode: "100644",
						Content: "apricot\n" + strings.Repeat("filler\n", 10) + "berries",
					}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{
						Path: "a", Mode: "100644",
						Content: "acai\n" + strings.Repeat("filler\n", 10) + "birne",
					}))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 1, 1):   "head",
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 12, 12): "origin",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON[:50]},
						{FilesJson: filesJSON[50:]},
					},
					expectedCommitAuthor: defaultCommitAuthor,
					expectedResponse:     &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"a": []byte("apricot\n" + strings.Repeat("filler\n", 10) + "birne"),
						},
					},
				}
			},
		},
		{
			"multi file multi conflict",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				baseCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple\n" + strings.Repeat("filler\n", 10) + "banana"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "strawberry"},
				))

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{
						Path: "a", Mode: "100644",
						Content: "apricot\n" + strings.Repeat("filler\n", 10) + "berries",
					}, gittest.TreeEntry{Path: "b", Mode: "100644", Content: "blueberry"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{
						Path: "a", Mode: "100644",
						Content: "acai\n" + strings.Repeat("filler\n", 10) + "birne",
					}, gittest.TreeEntry{Path: "b", Mode: "100644", Content: "raspberry"}))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 1, 1):   "head",
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 12, 12): "origin",
						},
					},
					{
						"old_path": "b",
						"new_path": "b",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("b")), 1, 1): "head",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON[:50]},
						{FilesJson: filesJSON[50:]},
					},
					expectedCommitAuthor: defaultCommitAuthor,
					expectedResponse:     &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"a": []byte("apricot\n" + strings.Repeat("filler\n", 10) + "birne"),
							"b": []byte("blueberry"),
						},
					},
				}
			},
		},
		{
			"multi file with move to dir",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				baseCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple\n" + strings.Repeat("filler\n", 10) + "banana"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "strawberry"},
					gittest.TreeEntry{Path: "c", Mode: "100644", Content: "blue\n" + strings.Repeat("filler\n", 10) + "apple\n"},
				))

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("ours"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Path: "a", Mode: "100644",
							Content: "apricot\n" + strings.Repeat("filler\n", 10) + "berries",
						},
						gittest.TreeEntry{Path: "b", Mode: "100644", Content: "blueberry"},
						gittest.TreeEntry{Path: "c", Mode: "100644", Content: "raw\n" + strings.Repeat("filler\n", 10) + "mango\n"},
					))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Path: "a", Mode: "100644",
							Content: "acai\n" + strings.Repeat("filler\n", 10) + "birne",
						},
						gittest.TreeEntry{Path: "b", Mode: "100644", Content: "raspberry"},
						gittest.TreeEntry{Path: "subdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
							{Path: "c", Mode: "100644", Content: "kiwi\n" + strings.Repeat("filler\n", 10) + "orange\n"},
						})},
					))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 1, 1):   "head",
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 12, 12): "origin",
						},
					},
					{
						"old_path": "b",
						"new_path": "b",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("b")), 1, 1): "head",
						},
					},
					{
						"old_path": "c",
						"new_path": "subdir/c",
						"content":  "new content",
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON[:50]},
						{FilesJson: filesJSON[50:]},
					},
					expectedCommitAuthor: defaultCommitAuthor,
					expectedResponse:     &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"a":        []byte("apricot\n" + strings.Repeat("filler\n", 10) + "birne"),
							"b":        []byte("blueberry"),
							"subdir/c": []byte("new content"),
						},
					},
				}
			},
		},
		{
			"multi file conflict, but missing file resolution",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				baseCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple\n" + strings.Repeat("filler\n", 10) + "banana"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "strawberry"},
				))

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{
						Path: "a", Mode: "100644",
						Content: "apricot\n" + strings.Repeat("filler\n", 10) + "berries",
					}, gittest.TreeEntry{Path: "b", Mode: "100644", Content: "blueberry"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{
						Path: "a", Mode: "100644",
						Content: "acai\n" + strings.Repeat("filler\n", 10) + "birne",
					}, gittest.TreeEntry{Path: "b", Mode: "100644", Content: "raspberry"}))

				files := []map[string]interface{}{
					{
						"old_path": "b",
						"new_path": "b",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("b")), 1, 1): "head",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON[:50]},
						{FilesJson: filesJSON[50:]},
					},
					expectedResponse: &gitalypb.ResolveConflictsResponse{
						ResolutionError: "Missing resolutions for the following files: a",
					},
					skipCommitCheck: true,
				}
			},
		},
		{
			"single file conflict, remote repo",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				testcfg.BuildGitalySSH(t, cfg)

				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)
				targetRepo, targetRepoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apricot"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, targetRepoPath, gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "acai"}))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 1, 1): "head",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: targetRepo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON[:50]},
						{FilesJson: filesJSON[50:]},
					},
					expectedCommitAuthor: defaultCommitAuthor,
					expectedResponse:     &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"a": []byte("apricot"),
						},
					},
				}
			},
		},
		{
			"single file with only newline",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "\n"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "\n"}))

				files := []map[string]interface{}{}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON},
					},
					expectedCommitAuthor: defaultCommitAuthor,
					expectedResponse:     &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"a": []byte("\n"),
						},
					},
				}
			},
		},
		{
			"conflicting newline with embedded character",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "\nA\n"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "\nB\n"}))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 2, 2): "head",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON},
					},
					expectedCommitAuthor: defaultCommitAuthor,
					expectedResponse:     &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"a": []byte("\nA\n"),
						},
					},
				}
			},
		},
		{
			"conflicting carriage-return newlines",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "A\r\nB\r\nC\r\nD\r\nE\r\n"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "A\r\nB\r\nX\r\nD\r\nE\r\n"}))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 3, 3): "origin",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON},
					},
					expectedCommitAuthor: defaultCommitAuthor,
					expectedResponse:     &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"a": []byte("A\r\nB\r\nX\r\nD\r\nE\r\n"),
						},
					},
				}
			},
		},
		{
			"conflict with no trailing newline",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "A\nB"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "X\nB"}))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 1, 1): "head",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON},
					},
					expectedCommitAuthor: defaultCommitAuthor,
					expectedResponse:     &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"a": []byte("A\nB"),
						},
					},
				}
			},
		},
		{
			"conflict with existing conflict markers",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "<<<<<<< HEAD\nA\nB\n======="}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "X\nB"}))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 1, 1): "head",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON},
					},
					expectedError:   structerr.NewInvalidArgument(`resolve: parse conflict for "a": unexpected conflict delimiter`),
					skipCommitCheck: true,
				}
			},
		},
		{
			"invalid OID",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apricot"}))
				gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "acai"}))

				files := []map[string]interface{}{}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     "conflict-resolvable",
							TheirCommitOid:   "conflict-start",
							SourceBranch:     []byte("ours"),
							TargetBranch:     []byte("theirs"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON},
					},
					expectedError:   structerr.NewInternal("Rugged::InvalidError: unable to parse OID - contains invalid characters"),
					skipCommitCheck: true,
				}
			},
		},
		{
			"resolved content is same as the conflict",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apricot"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "acai"}))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"content":  "<<<<<<< a\napricot\n=======\nacai\n>>>>>>> a\n",
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON},
					},
					expectedResponse: &gitalypb.ResolveConflictsResponse{ResolutionError: "Resolved content has no changes for file a"},
					skipCommitCheck:  true,
				}
			},
		},
		{
			"missing resolution for section",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apricot"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "acai"}))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 4, 4): "head",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON},
					},
					expectedResponse: &gitalypb.ResolveConflictsResponse{
						ResolutionError: fmt.Sprintf("Missing resolution for section ID: %x_%d_%d", sha1.Sum([]byte("a")), 1, 1),
					},
					skipCommitCheck: true,
				}
			},
		},
		{
			"empty User",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TheirCommitOid:   gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							Timestamp:        defaultTimestamp,
						},
					},
					skipCommitCheck: true,
					expectedError:   structerr.NewInvalidArgument("empty User"),
				}
			},
		},
		{
			"empty Repository",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							User:             defaultUser,
							TargetRepository: repo,
							OurCommitOid:     gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TheirCommitOid:   gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							Timestamp:        defaultTimestamp,
						},
					},
					skipCommitCheck: true,
					expectedError:   structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			"empty TargetRepository",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							User:           defaultUser,
							Repository:     repo,
							OurCommitOid:   gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TheirCommitOid: gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TargetBranch:   []byte("theirs"),
							SourceBranch:   []byte("ours"),
							CommitMessage:  []byte(conflictResolutionCommitMessage),
							Timestamp:      defaultTimestamp,
						},
					},
					skipCommitCheck: true,
					expectedError:   structerr.NewInvalidArgument("empty TargetRepository"),
				}
			},
		},
		{
			"empty OurCommitID",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							User:             defaultUser,
							Repository:       repo,
							TargetRepository: repo,
							TheirCommitOid:   gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							Timestamp:        defaultTimestamp,
						},
					},
					skipCommitCheck: true,
					expectedError:   structerr.NewInvalidArgument("empty OurCommitOid"),
				}
			},
		},
		{
			"empty TheirCommitID",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							User:             defaultUser,
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							Timestamp:        defaultTimestamp,
						},
					},
					skipCommitCheck: true,
					expectedError:   structerr.NewInvalidArgument("empty TheirCommitOid"),
				}
			},
		},
		{
			"empty CommitMessage",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							User:             defaultUser,
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TheirCommitOid:   gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							Timestamp:        defaultTimestamp,
						},
					},
					skipCommitCheck: true,
					expectedError:   structerr.NewInvalidArgument("empty CommitMessage"),
				}
			},
		},
		{
			"empty SourceBranch",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							User:             defaultUser,
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TheirCommitOid:   gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TargetBranch:     []byte("theirs"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							Timestamp:        defaultTimestamp,
						},
					},
					skipCommitCheck: true,
					expectedError:   structerr.NewInvalidArgument("empty SourceBranch"),
				}
			},
		},
		{
			"empty TargetBranch",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							User:             defaultUser,
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TheirCommitOid:   gittest.DefaultObjectHash.EmptyTreeOID.String(),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							Timestamp:        defaultTimestamp,
						},
					},
					skipCommitCheck: true,
					expectedError:   structerr.NewInvalidArgument("empty TargetBranch"),
				}
			},
		},
		{
			"uses quarantine repo",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				testcfg.BuildGitalySSH(t, cfg)
				testcfg.BuildGitalyHooks(t, cfg)

				// We set up a custom "pre-receive" hook which simply prints the commits content to stdout and
				// then exits with an error. Like this, we can both assert that the hook can see the
				// quarantined tag, and it allows us to fail the RPC before we migrate quarantined objects.
				gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte(
					`#!/bin/sh
						read oldval newval ref &&
						git cat-file -p $newval^{commit}:a &&
						exit 1
					`))

				baseCommit := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"}))
				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommit),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apricot"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("theirs"),
					gittest.WithParents(baseCommit),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "acai"}))

				files := []map[string]interface{}{
					{
						"old_path": "a",
						"new_path": "a",
						"sections": map[string]string{
							fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 1, 1): "head",
						},
					},
				}

				filesJSON, err := json.Marshal(files)
				require.NoError(t, err)

				objectsBefore := len(strings.Split(text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-list", "--objects", "--all")), "\n"))

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User:             defaultUser,
							Timestamp:        defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON},
					},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"a": []byte("apple"),
						},
					},
					expectedError:   structerr.NewInternal("running pre-receive hooks: apricot"),
					skipCommitCheck: true,
					additionalChecks: func() {
						objectsAfter := len(strings.Split(text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-list", "--objects", "--all")), "\n"))
						require.Equal(t, objectsBefore, objectsAfter, "No new objects should've been added")
					},
				}
			},
		},
		{
			"no user timezone specified, use UTC",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("ours"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "b", Mode: "100644", Content: "apricot"}))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("theirs"),
					gittest.WithTreeEntries(gittest.TreeEntry{Path: "c", Mode: "100644", Content: "acai"}))

				filesJSON, err := json.Marshal([]map[string]interface{}{})
				require.NoError(t, err)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     ourCommitID.String(),
							TheirCommitOid:   theirCommitID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User: &gitalypb.User{
								Name:  []byte("Jane Doe"),
								Email: []byte("janedoe@gitlab.com"),
								GlId:  "user-123",
							},
							Timestamp: defaultTimestamp,
						},
					},
					requestsFilesJSON: []*gitalypb.ResolveConflictsRequest_FilesJson{
						{FilesJson: filesJSON},
					},
					expectedCommitAuthor: &gitalypb.CommitAuthor{
						Name:     []byte("Jane Doe"),
						Email:    []byte("janedoe@gitlab.com"),
						Date:     defaultTimestamp,
						Timezone: []byte("+0000"),
					},
					expectedResponse: &gitalypb.ResolveConflictsResponse{},
					expectedContent: map[string]map[string][]byte{
						"refs/heads/ours": {
							"b": []byte("apricot"),
							"c": []byte("acai"),
						},
					},
				}
			},
		},
		{
			"invalid user timezone",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				return setupData{
					cfg:      cfg,
					client:   client,
					repoPath: repoPath,
					repo:     repo,
					requestHeader: &gitalypb.ResolveConflictsRequest_Header{
						Header: &gitalypb.ResolveConflictsRequestHeader{
							Repository:       repo,
							TargetRepository: repo,
							OurCommitOid:     gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TheirCommitOid:   gittest.DefaultObjectHash.EmptyTreeOID.String(),
							TargetBranch:     []byte("theirs"),
							SourceBranch:     []byte("ours"),
							CommitMessage:    []byte(conflictResolutionCommitMessage),
							User: &gitalypb.User{
								Name:     []byte("Jane Doe"),
								Email:    []byte("janedoe@gitlab.com"),
								GlId:     "user-123",
								Timezone: "Invalid/Timezone",
							},
							Timestamp: defaultTimestamp,
						},
					},
					expectedError:   structerr.NewInvalidArgument("unknown time zone Invalid/Timezone"),
					skipCommitCheck: true,
				}
			},
		},
	} {
		ctx := ctx

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t, ctx)

			mdGS := testcfg.GitalyServersMetadataFromCfg(t, setup.cfg)
			mdFF, _ := metadata.FromOutgoingContext(ctx)
			ctx = metadata.NewOutgoingContext(ctx, metadata.Join(mdGS, mdFF))

			stream, err := setup.client.ResolveConflicts(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{ResolveConflictsRequestPayload: setup.requestHeader}))
			for _, req := range setup.requestsFilesJSON {
				require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{ResolveConflictsRequestPayload: req}))
			}

			r, err := stream.CloseAndRecv()
			testhelper.RequireGrpcError(t, setup.expectedError, err)
			testhelper.ProtoEqual(t, setup.expectedResponse, r)

			for branch, pathAndContent := range setup.expectedContent {
				for path, content := range pathAndContent {
					actual := gittest.Exec(t, setup.cfg, "-C", setup.repoPath, "cat-file", "-p", fmt.Sprintf("%s:%s", branch, path))
					require.Equal(t, content, actual)
				}
			}

			if setup.requestHeader.Header != nil && !setup.skipCommitCheck {
				repo := localrepo.NewTestRepo(t, setup.cfg, setup.repo)
				headCommit, err := repo.ReadCommit(ctx, git.Revision(setup.requestHeader.Header.GetSourceBranch()))
				require.NoError(t, err)
				require.Contains(t, headCommit.GetParentIds(), setup.requestHeader.Header.GetOurCommitOid())
				require.Contains(t, headCommit.GetParentIds(), setup.requestHeader.Header.GetTheirCommitOid())
				testhelper.ProtoEqual(t, setup.expectedCommitAuthor, headCommit.GetAuthor())
				testhelper.ProtoEqual(t, setup.expectedCommitAuthor, headCommit.GetCommitter())
				require.Equal(t, string(headCommit.GetSubject()), conflictResolutionCommitMessage)
			}

			if setup.additionalChecks != nil {
				setup.additionalChecks()
			}
		})
	}
}

func TestResolveConflicts_sign(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testResolveConflictsSign)
}

func testResolveConflictsSign(t *testing.T, ctx context.Context) {
	t.Parallel()

	cfg := testcfg.Build(t)
	cfg.Git.CommitterName = "GitLab Server"
	cfg.Git.CommitterEmail = "gitlab@example.com"

	signingKeyPath := writeSSHSigningKey(t)

	var opts []testserver.GitalyServerOpt
	if featureflag.GPGSigning.IsEnabled(ctx) {
		opts = append(opts, testserver.WithSigningKey(signingKeyPath))
	}

	cfg.SocketPath = runConflictsServer(t, cfg, nil, opts...)
	client, conn := NewConflictsClient(t, cfg.SocketPath)
	t.Cleanup(func() { conn.Close() })

	if featureflag.GPGSigning.IsEnabled(ctx) {
		testcfg.BuildGitalyGPG(t, cfg)
	}

	for _, tc := range []struct {
		desc string
		sign bool
	}{
		{
			desc: "with Sign=false",
			sign: false,
		},
		{
			desc: "with Sign=true",
			sign: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			baseCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
				gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
			))
			ourCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("ours"),
				gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apricot"}))
			theirCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(baseCommitID), gittest.WithBranch("theirs"),
				gittest.WithTreeEntries(gittest.TreeEntry{Path: "a", Mode: "100644", Content: "acai"}))

			filesJSON, err := json.Marshal([]map[string]interface{}{
				{
					"old_path": "a",
					"new_path": "a",
					"sections": map[string]string{
						fmt.Sprintf("%x_%d_%d", sha1.Sum([]byte("a")), 1, 1): "head",
					},
				},
			})
			require.NoError(t, err)

			mdGS := testcfg.GitalyServersMetadataFromCfg(t, cfg)
			mdFF, _ := metadata.FromOutgoingContext(ctx)
			ctx := metadata.NewOutgoingContext(ctx, metadata.Join(mdGS, mdFF))

			stream, err := client.ResolveConflicts(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
				ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
					Header: &gitalypb.ResolveConflictsRequestHeader{
						Repository:       repoProto,
						TargetRepository: repoProto,
						OurCommitOid:     ourCommitID.String(),
						TheirCommitOid:   theirCommitID.String(),
						TargetBranch:     []byte("theirs"),
						SourceBranch:     []byte("ours"),
						CommitMessage:    []byte("Solve conflicts"),
						User:             gittest.TestUser,
						Sign:             tc.sign,
					},
				},
			}))
			require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
				ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_FilesJson{
					FilesJson: filesJSON,
				},
			}))

			response, err := stream.CloseAndRecv()
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.ResolveConflictsResponse{}, response)

			commitID := gittest.ResolveRevision(t, cfg, repoPath, "refs/heads/ours")
			commit, err := repo.ReadCommit(ctx, commitID.Revision())
			require.NoError(t, err)

			data, err := repo.ReadObject(ctx, commitID)
			require.NoError(t, err)

			gpgsig, dataWithoutGpgSig := signature.ExtractSignature(t, ctx, data)

			if featureflag.GPGSigning.IsEnabled(ctx) && tc.sign {
				signingKey, err := signature.ParseSigningKeys(signingKeyPath)
				require.NoError(t, err)
				require.NoError(t, signingKey.Verify([]byte(gpgsig), []byte(dataWithoutGpgSig)))

				require.Equal(t, []byte(cfg.Git.CommitterName), commit.GetCommitter().GetName())
				require.Equal(t, []byte(cfg.Git.CommitterEmail), commit.GetCommitter().GetEmail())
			} else {
				require.Empty(t, gpgsig, "commit should not be signed")

				require.Equal(t, gittest.TestUser.GetName(), commit.GetCommitter().GetName())
				require.Equal(t, gittest.TestUser.GetEmail(), commit.GetCommitter().GetEmail())
			}

			require.Equal(t, gittest.TestUser.GetName(), commit.GetAuthor().GetName())
			require.Equal(t, gittest.TestUser.GetEmail(), commit.GetAuthor().GetEmail())
		})
	}
}

// writeSSHSigningKey generates an ed25519 key so that no private key needs to be
// committed as a test fixture.
func writeSSHSigningKey(t *testing.T) string {
	t.Helper()

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	block, err := ssh.MarshalPrivateKey(privateKey, "")
	require.NoError(t, err)

	path := filepath.Join(testhelper.TempDir(t), "signing_ssh_key_ed25519")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(block), 0o600))

	return path
}
