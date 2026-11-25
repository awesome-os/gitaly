package commit

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math"

	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/log"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	// listFilesReadBufferSize is the initial size of the buffer that the ls-tree output is read
	// into. Most ListFiles responses on GitLab.com are a few hundred bytes, so it starts small and
	// only grows if an entry doesn't fit.
	listFilesReadBufferSize = 4 * 1024
	// listFilesMaxMessageSize matches the chunk package, so the paths are split across responses
	// as they were before.
	listFilesMaxMessageSize = 1024 * 1024
	// listFilesMinPathBufferSize and listFilesMaxPathBufferSize bound the size of the buffers that
	// paths are copied into. The first buffer is small so that small responses stay cheap, and
	// each following buffer doubles in size so that large responses need few allocations.
	listFilesMinPathBufferSize = 4 * 1024
	listFilesMaxPathBufferSize = 64 * 1024
)

var listFilesBlobPrefix = []byte("blob ")

func (s *server) ListFiles(in *gitalypb.ListFilesRequest, stream gitalypb.CommitService_ListFilesServer) error {
	ctx := stream.Context()

	s.logger.WithFields(log.Fields{
		"Revision": in.GetRevision(),
	}).DebugContext(ctx, "ListFiles")

	if err := validateListFilesRequest(ctx, s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localRepoFactory.Build(in.GetRepository())
	if _, err := repo.Path(ctx); err != nil {
		return err
	}

	revision := string(in.GetRevision())
	if len(revision) == 0 {
		defaultBranch, err := repo.GetDefaultBranch(ctx)
		if err != nil {
			return structerr.NewNotFound("revision not found %q", revision)
		}

		if len(defaultBranch) == 0 {
			return structerr.NewFailedPrecondition("repository does not have a default branch")
		}

		revision = defaultBranch.String()
	}

	contained, err := repo.HasRevision(ctx, git.Revision(revision))
	if err != nil {
		return structerr.NewInternal("%w", err)
	}

	if !contained {
		return stream.Send(&gitalypb.ListFilesResponse{})
	}

	if err := s.listFiles(repo, revision, stream); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func validateListFilesRequest(ctx context.Context, locator storage.Locator, in *gitalypb.ListFilesRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}
	if err := git.ValidateRevision(in.GetRevision(), git.AllowEmptyRevision()); err != nil {
		return err
	}
	return nil
}

func (s *server) listFiles(repo gitcmd.RepositoryExecutor, revision string, stream gitalypb.CommitService_ListFilesServer) error {
	ctx := stream.Context()

	// Only the type and the path of each entry are needed, so entries are scanned in place instead
	// of being parsed into tree entries. This keeps the work per entry down to a scan of the output
	// and a copy of the path, which matters for repositories with hundreds of thousands of files.
	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name: "ls-tree",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "-z"},
			gitcmd.Flag{Name: "-r"},
			gitcmd.Flag{Name: "--full-tree"},
			gitcmd.Flag{Name: "--full-name"},
		},
		Args: []string{revision},
	}, gitcmd.WithSetupStdout())
	if err != nil {
		return err
	}

	sender := &listFilesSender{stream: stream}
	entries := bufio.NewScanner(cmd)
	// Paths aren't limited in length, so the buffer grows to fit the longest entry and is reused
	// for all entries after it.
	entries.Buffer(make([]byte, listFilesReadBufferSize), math.MaxInt)
	entries.Split(splitNUL)

	for entries.Scan() {
		entry := entries.Bytes()

		// Each entry has the format `<mode> SP <type> SP <objectid> TAB <path>`. With `-z`, Git
		// doesn't quote the path.
		info, path, found := bytes.Cut(entry, []byte{'\t'})
		if !found {
			return fmt.Errorf("parsing ls-tree entry %q: %w", info, localrepo.ErrParse)
		}

		// Recursive ls-tree lists only blobs and submodule commits, and only blobs are files. `info`
		// is `<mode> SP <type> SP <objectid>`, so after cutting off the mode, `objectInfo` is
		// `<type> SP <objectid>` and its prefix tells the type of the entry.
		_, objectInfo, _ := bytes.Cut(info, []byte{' '})
		if !bytes.HasPrefix(objectInfo, listFilesBlobPrefix) {
			continue
		}

		if err := sender.queuePath(path); err != nil {
			return err
		}
	}

	if err := entries.Err(); err != nil {
		return fmt.Errorf("reading ls-tree output: %w", err)
	}

	return sender.flush()
}

// splitNUL is a bufio.SplitFunc that splits NUL-terminated entries, as written by Git with `-z`.
// The NUL terminators aren't part of the returned entries.
func splitNUL(data []byte, atEOF bool) (int, []byte, error) {
	if i := bytes.IndexByte(data, 0); i >= 0 {
		return i + 1, data[:i], nil
	}
	if atEOF && len(data) > 0 {
		return 0, nil, fmt.Errorf("unterminated entry: %w", localrepo.ErrParse)
	}

	return 0, nil, nil
}

// listFilesSender batches paths into responses of at most listFilesMaxMessageSize bytes.
//
// To avoid an allocation per path, paths are copied into buffers that hold many paths each, and
// the pending response refers to them through slices of those buffers. When a buffer is full,
// a new one is allocated instead of reusing it, because gRPC may still read a sent message after
// Send has returned. A buffer that the sender has moved on from stays alive as long as a response
// refers to it, and is garbage collected once all of them are gone.
type listFilesSender struct {
	stream gitalypb.CommitService_ListFilesServer
	// paths are the paths of the pending response.
	paths [][]byte
	// size is the encoded size of paths.
	size int
	// buf is the buffer that the next path is copied into.
	buf []byte
}

// queuePath adds the path to the pending response. If the path doesn't fit into the pending
// response, the pending response is sent first and the path starts a new one.
func (s *listFilesSender) queuePath(path []byte) error {
	pathSize := protowire.SizeTag(1) + protowire.SizeBytes(len(path))
	if s.size > 0 && s.size+pathSize >= listFilesMaxMessageSize {
		if err := s.flush(); err != nil {
			return err
		}
	}

	if cap(s.buf)-len(s.buf) < len(path) {
		// The path doesn't fit into the current buffer, so allocate a new one. Each new buffer is
		// twice the size of the previous one, starting at listFilesMinPathBufferSize and capped at
		// listFilesMaxPathBufferSize, but always big enough for the path itself.
		size := min(max(2*cap(s.buf), listFilesMinPathBufferSize), listFilesMaxPathBufferSize)
		s.buf = make([]byte, 0, max(size, len(path)))
	}
	start := len(s.buf)
	s.buf = append(s.buf, path...)

	// The capacity is capped so that appending to one path can never overwrite the path after it.
	s.paths = append(s.paths, s.buf[start:len(s.buf):len(s.buf)])
	s.size += pathSize

	return nil
}

func (s *listFilesSender) flush() error {
	if len(s.paths) == 0 {
		return nil
	}

	err := s.stream.Send(&gitalypb.ListFilesResponse{Paths: s.paths})
	s.paths = make([][]byte, 0, len(s.paths))
	s.size = 0

	return err
}
