package repository

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
)

func TestFetchSourceBranch(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.FetchSourceBranchNegotiationTips,
		featureflag.FetchSourceBranchHideRefs,
	).Run(t, testFetchSourceBranch)
}

func testFetchSourceBranch(t *testing.T, ctx context.Context) {
	t.Parallel()

	type setupData struct {
		cfg         config.Cfg
		client      gitalypb.RepositoryServiceClient
		request     *gitalypb.FetchSourceBranchRequest
		verify      func()
		expectedErr error
		// skipGitalyServersMetadata omits the Gitaly servers metadata from the request.
		skipGitalyServersMetadata bool
	}

	for _, tc := range []struct {
		desc             string
		setup            func(t *testing.T) setupData
		expectedResponse *gitalypb.FetchSourceBranchResponse
	}{
		{
			desc: "success",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)
				commitClient := gitalypb.NewCommitServiceClient(gittest.DialService(t, ctx, cfg))

				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				commitID := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("master"))

				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceRepository: sourceRepo,
						SourceBranch:     []byte("master"),
						TargetRef:        []byte("refs/tmp/fetch-source-branch-test"),
					},
					verify: func() {
						actualCommit := gittest.ResolveRevisionAPI(t, ctx, commitClient, repo, "refs/tmp/fetch-source-branch-test^{commit}")
						require.Equal(t, commitID, actualCommit)
					},
				}
			},
			expectedResponse: &gitalypb.FetchSourceBranchResponse{Result: true},
		},
		{
			desc: "success with expected_target_old_oid",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)
				commitClient := gitalypb.NewCommitServiceClient(gittest.DialService(t, ctx, cfg))
				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				sourceCommit := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch(git.DefaultBranch))

				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))
				secondCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommit), gittest.WithBranch(git.DefaultBranch))

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:           repo,
						SourceRepository:     sourceRepo,
						SourceBranch:         []byte(git.DefaultBranch),
						TargetRef:            []byte(git.DefaultRef),
						ExpectedTargetOldOid: secondCommit.String(),
					},
					verify: func() {
						actualCommit := gittest.ResolveRevisionAPI(t, ctx, commitClient, repo, git.DefaultRef.String()+"^{commit}")
						require.Equal(t, sourceCommit, actualCommit)
					},
				}
			},
			expectedResponse: &gitalypb.FetchSourceBranchResponse{Result: true},
		},
		{
			desc: "success + same repository",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				commitID := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("master"))
				commitClient := gitalypb.NewCommitServiceClient(gittest.DialService(t, ctx, cfg))

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       sourceRepo,
						SourceRepository: sourceRepo,
						SourceBranch:     []byte("master"),
						TargetRef:        []byte("refs/tmp/fetch-source-branch-test"),
					},
					verify: func() {
						actualCommit := gittest.ResolveRevisionAPI(t, ctx, commitClient, sourceRepo, "refs/tmp/fetch-source-branch-test^{commit}")
						require.Equal(t, commitID, actualCommit)
					},
				}
			},
			expectedResponse: &gitalypb.FetchSourceBranchResponse{Result: true},
		},
		{
			desc: "success + same repository without Gitaly servers metadata",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				commitID := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("master"))
				commitClient := gitalypb.NewCommitServiceClient(gittest.DialService(t, ctx, cfg))

				request := &gitalypb.FetchSourceBranchRequest{
					Repository:       sourceRepo,
					SourceRepository: sourceRepo,
					SourceBranch:     []byte("master"),
					TargetRef:        []byte("refs/tmp/fetch-source-branch-test"),
				}

				// In this test case, we want to test that when source and target repositories
				// are the same, we don't need the Gitaly metadata since we do not need to
				// create a remote repo. This Praefect test case here is simply to prove that
				// if metadata is skipped when Praefect is used, it returns an error.
				if testhelper.IsPraefectEnabled() {
					return setupData{
						cfg:                       cfg,
						client:                    client,
						request:                   request,
						expectedErr:               structerr.NewInternal("remote repository: empty gitaly-servers metadata"),
						skipGitalyServersMetadata: true,
					}
				}

				return setupData{
					cfg:     cfg,
					client:  client,
					request: request,
					verify: func() {
						actualCommit := gittest.ResolveRevisionAPI(t, ctx, commitClient, sourceRepo, "refs/tmp/fetch-source-branch-test^{commit}")
						require.Equal(t, commitID, actualCommit)
					},
					skipGitalyServersMetadata: true,
				}
			},
			expectedResponse: testhelper.GitalyOrPraefect(&gitalypb.FetchSourceBranchResponse{Result: true}, nil),
		},
		{
			desc: "success + same repository resolves source remotely with Praefect",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				localCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithMessage("local"))
				commitClient := gitalypb.NewCommitServiceClient(gittest.DialService(t, ctx, cfg))

				// Set up a second server that holds a repository with the same storage name and
				// relative path, but a different commit. The Gitaly servers metadata points to
				// this server, so the fetched commit tells which repository was resolved.
				remoteCfg, _ := setupRepositoryService(t)
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, remoteCfg, gittest.CreateRepositoryConfig{
					RelativePath: repo.GetRelativePath(),
				})
				remoteCommit := gittest.WriteCommit(t, remoteCfg, remoteRepoPath, gittest.WithBranch("master"), gittest.WithMessage("remote"))

				// Praefect rewrites the target repository to its replica path but leaves the
				// source repository untouched, so Gitaly treats the source as remote and
				// resolves it through the Gitaly servers metadata. Without Praefect, the source
				// is the same repository and is resolved locally.
				data := setupData{
					// this config is used to populate gitaly server metadata
					cfg:    remoteCfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceRepository: repo,
						SourceBranch:     []byte("master"),
						TargetRef:        []byte("refs/tmp/fetch-source-branch-test"),
					},
					verify: func() {
						actualCommit := gittest.ResolveRevisionAPI(t, ctx, commitClient, repo, "refs/tmp/fetch-source-branch-test^{commit}")
						require.Equal(t, testhelper.GitalyOrPraefect(localCommit, remoteCommit), actualCommit)
					},
				}

				if !testhelper.IsPraefectEnabled() {
					data.skipGitalyServersMetadata = true
				}
				return data
			},
			expectedResponse: &gitalypb.FetchSourceBranchResponse{Result: true},
		},
		{
			desc: "failure due to incorrect expected_target_old_oid",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				sourceCommit := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch(git.DefaultBranch))

				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))
				concurrentCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommit), gittest.WithBranch(git.DefaultBranch))

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:           repo,
						SourceRepository:     sourceRepo,
						SourceBranch:         []byte(git.DefaultBranch),
						TargetRef:            []byte(git.DefaultRef),
						ExpectedTargetOldOid: firstCommit.String(),
					},
					expectedErr: testhelper.WithInterceptedMetadataItems(
						structerr.NewInternal("commit: reference does not point to expected object"),
						structerr.MetadataItem{Key: "actual_object_id", Value: concurrentCommit.String()},
						structerr.MetadataItem{Key: "expected_object_id", Value: sourceCommit.String()},
						structerr.MetadataItem{Key: "reference", Value: "refs/heads/main"},
					),
				}
			},
		},
		{
			desc: "failure due to branch not found",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, sourceRepoPath)
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceRepository: sourceRepo,
						SourceBranch:     []byte("master"),
						TargetRef:        []byte("refs/tmp/fetch-source-branch-test"),
					},
				}
			},
			expectedResponse: &gitalypb.FetchSourceBranchResponse{Result: false},
		},
		{
			desc: "failure due to branch not found (same repo)",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, sourceRepoPath)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       sourceRepo,
						SourceRepository: sourceRepo,
						SourceBranch:     []byte("master"),
						TargetRef:        []byte("refs/tmp/fetch-source-branch-test"),
					},
				}
			},
			expectedResponse: &gitalypb.FetchSourceBranchResponse{Result: false},
		},
		{
			desc: "failure due to no repository provided",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						SourceRepository: sourceRepo,
						SourceBranch:     []byte("master"),
						TargetRef:        []byte("refs/tmp/fetch-source-branch-test"),
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "failure due to no source branch",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, _ := gittest.CreateRepository(t, ctx, cfg)
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceRepository: sourceRepo,
						TargetRef:        []byte("refs/tmp/fetch-source-branch-test"),
					},
					expectedErr: structerr.NewInvalidArgument("empty revision"),
				}
			},
		},
		{
			desc: "failure due to blanks in source branch",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, _ := gittest.CreateRepository(t, ctx, cfg)
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceBranch:     []byte("   "),
						SourceRepository: sourceRepo,
						TargetRef:        []byte("refs/tmp/fetch-source-branch-test"),
					},
					expectedErr: structerr.NewInvalidArgument("revision can't contain whitespace"),
				}
			},
		},
		{
			desc: "failure due to source branch starting with -",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, _ := gittest.CreateRepository(t, ctx, cfg)
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceBranch:     []byte("-ref"),
						SourceRepository: sourceRepo,
						TargetRef:        []byte("refs/tmp/fetch-source-branch-test"),
					},
					expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
				}
			},
		},
		{
			desc: "failure due to source branch with :",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, _ := gittest.CreateRepository(t, ctx, cfg)
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceBranch:     []byte("some:ref"),
						SourceRepository: sourceRepo,
						TargetRef:        []byte("refs/tmp/fetch-source-branch-test"),
					},
					expectedErr: structerr.NewInvalidArgument("revision can't contain ':'"),
				}
			},
		},
		{
			desc: "failure due to source branch with NUL",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, _ := gittest.CreateRepository(t, ctx, cfg)
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceBranch:     []byte("some\x00ref"),
						SourceRepository: sourceRepo,
						TargetRef:        []byte("refs/tmp/fetch-source-branch-test"),
					},
					expectedErr: structerr.NewInvalidArgument("revision can't contain NUL"),
				}
			},
		},
		{
			desc: "failure due to no target ref",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("master"))
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceBranch:     []byte("master"),
						SourceRepository: sourceRepo,
					},
					expectedErr: structerr.NewInvalidArgument("empty revision"),
				}
			},
		},
		{
			desc: "failure due to blanks in target ref",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("master"))
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceBranch:     []byte("master"),
						SourceRepository: sourceRepo,
						TargetRef:        []byte("   "),
					},
					expectedErr: structerr.NewInvalidArgument("revision can't contain whitespace"),
				}
			},
		},
		{
			desc: "failure due to target ref starting with -",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("master"))
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceBranch:     []byte("master"),
						SourceRepository: sourceRepo,
						TargetRef:        []byte("-ref"),
					},
					expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
				}
			},
		},
		{
			desc: "failure due to target ref with :",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("master"))
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceBranch:     []byte("master"),
						SourceRepository: sourceRepo,
						TargetRef:        []byte("some:ref"),
					},
					expectedErr: structerr.NewInvalidArgument("revision can't contain ':'"),
				}
			},
		},
		{
			desc: "failure due to target ref with NUL",
			setup: func(t *testing.T) setupData {
				cfg, client := setupRepositoryService(t)

				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("master"))
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceBranch:     []byte("master"),
						SourceRepository: sourceRepo,
						TargetRef:        []byte("some\x00ref"),
					},
					expectedErr: structerr.NewInvalidArgument("revision can't contain NUL"),
				}
			},
		},
		{
			desc: "failure during/after fetch doesn't clean out fetched objects",
			setup: func(t *testing.T) setupData {
				cfg := testcfg.Build(t)

				testcfg.BuildGitalyHooks(t, cfg)
				testcfg.BuildGitalySSH(t, cfg)

				// We simulate a failed fetch where we actually fetch but just exit
				// with status 1, this will actually fetch the refs but gitaly will think
				// git failed. We match against a config value that is only present during
				// a fetch.
				gitCmdFactory := gittest.NewInterceptingCommandFactory(t, ctx, cfg, func(execEnv gitcmd.ExecutionEnvironment) string {
					return fmt.Sprintf(`#!/usr/bin/env bash
						if [[ "$@" =~ "fetch.writeCommitGraph" ]]; then
							%q "$@"
							exit 1
						fi
						exec %q "$@"`, execEnv.BinaryPath, execEnv.BinaryPath)
				})

				client, serverSocketPath := runRepositoryService(t, cfg, testserver.WithGitCommandFactory(gitCmdFactory))
				cfg.SocketPath = serverSocketPath

				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				commitID := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("master"))
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:    cfg,
					client: client,
					request: &gitalypb.FetchSourceBranchRequest{
						Repository:       repo,
						SourceRepository: sourceRepo,
						SourceBranch:     []byte("master"),
						TargetRef:        []byte("refs/tmp/fetch-source-branch-test"),
					},
					verify: func() {
						repo := localrepo.NewTestRepo(t, cfg, repo)
						exists, err := repo.HasRevision(ctx, commitID.Revision()+"^{commit}")
						require.NoError(t, err)
						require.False(t, exists, "fetched commit isn't discarded")
					},
				}
			},
			expectedResponse: &gitalypb.FetchSourceBranchResponse{Result: false},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			data := tc.setup(t)

			ctx := ctx
			if !data.skipGitalyServersMetadata {
				md := testcfg.GitalyServersMetadataFromCfg(t, data.cfg)
				ctx = testhelper.MergeOutgoingMetadata(ctx, md)
			}

			resp, err := data.client.FetchSourceBranch(ctx, data.request)
			testhelper.RequireGrpcError(t, data.expectedErr, err)

			if data.verify != nil {
				data.verify()
			}
			testhelper.ProtoEqual(t, tc.expectedResponse, resp)
		})
	}
}
