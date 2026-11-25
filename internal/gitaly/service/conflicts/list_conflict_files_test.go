package conflicts

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
)

type conflictFile struct {
	Header  *gitalypb.ConflictFileHeader
	Content []byte
}

func TestListConflictFiles(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	type setupData struct {
		request       *gitalypb.ListConflictFilesRequest
		client        gitalypb.ConflictsServiceClient
		expectedFiles []*conflictFile
		expectedError error
	}

	for _, tc := range []struct {
		desc  string
		setup func(testing.TB, context.Context) setupData
	}{
		{
			"Lists the expected conflict files",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "banana"},
				))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:  client,
					request: request,
					expectedFiles: []*conflictFile{
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid: ourCommitID.String(),
								TheirPath: []byte("a"),
								OurPath:   []byte("a"),
								OurMode:   int32(0o100644),
							},
							Content: []byte("<<<<<<< a\napple\n=======\nmango\n>>>>>>> a\n"),
						},
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid: ourCommitID.String(),
								TheirPath: []byte("b"),
								OurPath:   []byte("b"),
								OurMode:   int32(0o100644),
							},
							Content: []byte("<<<<<<< b\nbanana\n=======\npeach\n>>>>>>> b\n"),
						},
					},
				}
			},
		},
		{
			"Lists the expected conflict files without content",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "banana"},
				))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
					SkipContent:    true,
				}

				return setupData{
					client:  client,
					request: request,
					expectedFiles: []*conflictFile{
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid: ourCommitID.String(),
								TheirPath: []byte("a"),
								OurPath:   []byte("a"),
								OurMode:   int32(0o100644),
							},
						},
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid: ourCommitID.String(),
								TheirPath: []byte("b"),
								OurPath:   []byte("b"),
								OurMode:   int32(0o100644),
							},
						},
					},
				}
			},
		},
		{
			"Lists the expected conflict files with short OIDs",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "banana"},
				))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String()[:5],
					TheirCommitOid: theirCommitID.String()[:5],
				}

				return setupData{
					client:  client,
					request: request,
					expectedFiles: []*conflictFile{
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid: ourCommitID.String()[:5],
								TheirPath: []byte("a"),
								OurPath:   []byte("a"),
								OurMode:   int32(0o100644),
							},
							Content: []byte("<<<<<<< a\napple\n=======\nmango\n>>>>>>> a\n"),
						},
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid: ourCommitID.String()[:5],
								TheirPath: []byte("b"),
								OurPath:   []byte("b"),
								OurMode:   int32(0o100644),
							},
							Content: []byte("<<<<<<< b\nbanana\n=======\npeach\n>>>>>>> b\n"),
						},
					},
				}
			},
		},
		{
			"conflict in submodules commits are not handled",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)
				_, subRepoPath := gittest.CreateRepository(tb, ctx, cfg)

				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					Mode:    "100644",
					Path:    "abc",
					Content: "foo",
				}))
				ourCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))

				newSubCommitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					Mode:    "100644",
					Path:    "abc",
					Content: "bar",
				}))
				theirCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: newSubCommitID, Mode: "160000", Path: "sub"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:  client,
					request: request,
					expectedError: testhelper.WithInterceptedMetadata(
						structerr.NewFailedPrecondition("getting objectreader: object not found"),
						"revision", subCommitID.String(),
					),
				}
			},
		},
		{
			"Lists the expected conflict files with ancestor path",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				commonCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "banana"},
				))
				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "a", Mode: "100644", Content: "grape"},
						gittest.TreeEntry{Path: "b", Mode: "100644", Content: "pineapple"},
					),
				)
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
						gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
					),
				)

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:  client,
					request: request,
					expectedFiles: []*conflictFile{
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid:    ourCommitID.String(),
								TheirPath:    []byte("a"),
								OurPath:      []byte("a"),
								OurMode:      int32(0o100644),
								AncestorPath: []byte("a"),
							},
							Content: []byte("<<<<<<< a\ngrape\n=======\nmango\n>>>>>>> a\n"),
						},
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid:    ourCommitID.String(),
								TheirPath:    []byte("b"),
								OurPath:      []byte("b"),
								OurMode:      int32(0o100644),
								AncestorPath: []byte("b"),
							},
							Content: []byte("<<<<<<< b\npineapple\n=======\npeach\n>>>>>>> b\n"),
						},
					},
				}
			},
		},
		{
			"Directory rename conflict without explicit paths",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				commonCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "z", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
						{Path: "b", Mode: "100644", Content: "b"},
						{Path: "c", Mode: "100644", Content: "c"},
					})},
				))

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "y", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
							{Path: "b", Mode: "100644", Content: "b"},
						})},
						gittest.TreeEntry{Path: "w", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
							{Path: "c", Mode: "100644", Content: "c"},
						})},
					),
				)

				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "z", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
							{Path: "b", Mode: "100644", Content: "b"},
							{Path: "c", Mode: "100644", Content: "c"},
							{Path: "d", Mode: "100644", Content: "d"},
						})},
					),
				)

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:  client,
					request: request,
				}
			},
		},
		{
			"Lists the expected conflict files with huge diff",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: strings.Repeat("a\n", 128*1024)},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: strings.Repeat("b\n", 128*1024)},
				))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: strings.Repeat("x\n", 128*1024)},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: strings.Repeat("y\n", 128*1024)},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:  client,
					request: request,
					expectedFiles: []*conflictFile{
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid: ourCommitID.String(),
								TheirPath: []byte("a"),
								OurPath:   []byte("a"),
								OurMode:   int32(0o100644),
							},
							Content: []byte(fmt.Sprintf("<<<<<<< a\n%s=======\n%s>>>>>>> a\n",
								strings.Repeat("a\n", 128*1024),
								strings.Repeat("x\n", 128*1024),
							)),
						},
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid: ourCommitID.String(),
								TheirPath: []byte("b"),
								OurPath:   []byte("b"),
								OurMode:   int32(0o100644),
							},
							Content: []byte(fmt.Sprintf("<<<<<<< b\n%s=======\n%s>>>>>>> b\n",
								strings.Repeat("b\n", 128*1024),
								strings.Repeat("y\n", 128*1024),
							)),
						},
					},
				}
			},
		},
		{
			"invalid commit id on 'our' side",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   "foobar",
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:        client,
					request:       request,
					expectedError: structerr.NewFailedPrecondition("could not lookup 'our' OID: reference not found"),
				}
			},
		},
		{
			"invalid commit id on 'their' side",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: "foobar",
				}

				return setupData{
					client:        client,
					request:       request,
					expectedError: structerr.NewFailedPrecondition("could not lookup 'their' OID: reference not found"),
				}
			},
		},
		{
			"conflict side missing",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				commonCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "banana"},
				))
				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
					),
				)
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
					),
				)

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:        client,
					request:       request,
					expectedError: structerr.NewFailedPrecondition("conflict side missing"),
				}
			},
		},
		{
			"allow tree conflicts",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				commonCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "banana"},
				))
				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
					),
				)
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
					),
				)

				request := &gitalypb.ListConflictFilesRequest{
					Repository:         repo,
					OurCommitOid:       ourCommitID.String(),
					TheirCommitOid:     theirCommitID.String(),
					AllowTreeConflicts: true,
				}

				return setupData{
					client:  client,
					request: request,
					expectedFiles: []*conflictFile{
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid:    ourCommitID.String(),
								OurPath:      []byte("a"),
								OurMode:      int32(0o100644),
								AncestorPath: []byte("a"),
							},
							Content: []byte("mango"),
						},
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid:    ourCommitID.String(),
								TheirPath:    []byte("b"),
								AncestorPath: []byte("b"),
							},
							Content: []byte("peach"),
						},
					},
				}
			},
		},
		{
			"conflict does not exist when using merge:union attribute",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				baseCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitattributes",
						Content: "a merge=union",
					},
					gittest.TreeEntry{
						Path: "a", Mode: "100644",
						Content: `- A test change
- Update readme`,
					},
				))

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommit),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Path: "a", Mode: "100644",
							Content: `- A test change
- Update readme
- Fire the chef!`,
						},
					))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(baseCommit),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Path: "a", Mode: "100644",
							Content: `- A test change
- Update readme
- Add to somechanges file`,
						},
					))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				data := setupData{
					client:  client,
					request: request,
				}

				return data
			},
		},
		{
			"encoding error",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "a\xc5z"},
				))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "ascii normal"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:        client,
					request:       request,
					expectedError: structerr.NewFailedPrecondition("unsupported encoding"),
				}
			},
		},
		{
			"empty repo",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				_, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
				))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     nil,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:        client,
					request:       request,
					expectedError: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			"empty OurCommitId field",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   "",
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:        client,
					request:       request,
					expectedError: structerr.NewInvalidArgument("empty OurCommitOid"),
				}
			},
		},
		{
			"empty TheirCommitId field",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsService(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: "",
				}

				return setupData{
					client:        client,
					request:       request,
					expectedError: structerr.NewInvalidArgument("empty TheirCommitOid"),
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			data := tc.setup(t, ctx)
			c, err := data.client.ListConflictFiles(ctx, data.request)
			if err != nil {
				testhelper.RequireGrpcError(t, data.expectedError, err)
				return
			}

			files, err := getConflictFiles(t, c)
			testhelper.RequireGrpcError(t, data.expectedError, err)
			testhelper.ProtoEqual(t, data.expectedFiles, files)
		})
	}
}

func TestStreamConflictContent(t *testing.T) {
	t.Parallel()

	ours := strings.Repeat("1", 40)
	theirs := strings.Repeat("2", 40)
	replacements := []oidReplacement{
		{oid: []byte(ours), path: []byte("our/path")},
		{oid: []byte(theirs), path: []byte("their/path")},
	}

	conflict := fmt.Sprintf("<<<<<<< %s\nmango\n=======\npeach\n>>>>>>> %s\n", ours, theirs)
	expectedConflict := "<<<<<<< our/path\nmango\n=======\npeach\n>>>>>>> their/path\n"

	for _, tc := range []struct {
		desc            string
		content         string
		expectedContent string
		expectedErr     error
	}{
		{
			desc: "empty content",
		},
		{
			desc:            "content without OIDs",
			content:         "apple\nbanana\n",
			expectedContent: "apple\nbanana\n",
		},
		{
			desc:            "conflict markers",
			content:         conflict,
			expectedContent: expectedConflict,
		},
		{
			desc:            "OIDs at start and end of content",
			content:         ours + "\n" + theirs,
			expectedContent: "our/path\ntheir/path",
		},
		{
			desc:            "adjacent OIDs",
			content:         ours + theirs + ours,
			expectedContent: "our/paththeir/pathour/path",
		},
		{
			desc:            "partial OID at end of content",
			content:         "apple " + ours[:39],
			expectedContent: "apple " + ours[:39],
		},
		{
			desc:            "content spanning many chunks",
			content:         strings.Repeat("apple\n"+conflict, 100),
			expectedContent: strings.Repeat("apple\n"+expectedConflict, 100),
		},
		{
			desc:            "multi-byte characters",
			content:         strings.Repeat("h\u00e9llo \u4e16\u754c \U0001f389 "+ours+"\n", 20),
			expectedContent: strings.Repeat("h\u00e9llo \u4e16\u754c \U0001f389 our/path\n", 20),
		},
		{
			desc:        "invalid UTF-8 at start of content",
			content:     "\xc5z" + conflict,
			expectedErr: structerr.NewFailedPrecondition("unsupported encoding"),
		},
		{
			desc:        "invalid UTF-8 after many chunks",
			content:     strings.Repeat(conflict, 10) + "a\xc5z" + conflict,
			expectedErr: structerr.NewFailedPrecondition("unsupported encoding"),
		},
		{
			desc:        "truncated multi-byte character at end of content",
			content:     conflict + "\xe4\xb8",
			expectedErr: structerr.NewFailedPrecondition("unsupported encoding"),
		},
	} {
		for _, reader := range []struct {
			desc  string
			apply func(io.Reader) io.Reader
		}{
			{desc: "full reads", apply: func(r io.Reader) io.Reader { return r }},
			{desc: "one byte reads", apply: iotest.OneByteReader},
			{desc: "half reads", apply: iotest.HalfReader},
			{desc: "data with EOF", apply: iotest.DataErrReader},
		} {
			for _, bufSize := range []int{48, 64, readBufferSize} {
				t.Run(fmt.Sprintf("%s/%s/buffer of %d bytes", tc.desc, reader.desc, bufSize), func(t *testing.T) {
					t.Parallel()

					var output bytes.Buffer
					err := streamConflictContent(bufio.NewReaderSize(reader.apply(strings.NewReader(tc.content)), bufSize), replacements, func(p []byte) error {
						_, err := output.Write(p)
						return err
					})

					if tc.expectedErr != nil {
						require.Equal(t, tc.expectedErr, err)
						return
					}

					require.NoError(t, err)
					require.Equal(t, tc.expectedContent, output.String())
				})
			}
		}
	}

	t.Run("read error", func(t *testing.T) {
		t.Parallel()

		err := streamConflictContent(bufio.NewReaderSize(iotest.ErrReader(assert.AnError), readBufferSize), replacements, func([]byte) error {
			return nil
		})
		require.Equal(t, structerr.NewInternal("reading conflict object: %w", assert.AnError), err)
	})

	t.Run("emit error", func(t *testing.T) {
		t.Parallel()

		err := streamConflictContent(bufio.NewReaderSize(strings.NewReader(conflict), readBufferSize), replacements, func([]byte) error {
			return assert.AnError
		})
		require.Equal(t, assert.AnError, err)
	})
}

func getConflictFiles(t *testing.T, c gitalypb.ConflictsService_ListConflictFilesClient) ([]*conflictFile, error) {
	t.Helper()

	var files []*conflictFile
	var currentFile *conflictFile
	var err error

	for {
		var r *gitalypb.ListConflictFilesResponse
		r, err = c.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return files, err
		}

		for _, file := range r.GetFiles() {
			// If there's a header this is the beginning of a new file
			if header := file.GetHeader(); header != nil {
				if currentFile != nil {
					files = append(files, currentFile)
				}

				currentFile = &conflictFile{Header: header}
			} else {
				// Append to current file's content
				currentFile.Content = append(currentFile.Content, file.GetContent()...)
			}
		}
	}

	// Append leftover file
	if currentFile != nil {
		files = append(files, currentFile)
	}

	return files, nil
}
