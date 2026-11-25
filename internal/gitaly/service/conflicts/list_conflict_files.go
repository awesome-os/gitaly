package conflicts

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v19/streamio"
)

func (s *server) ListConflictFiles(request *gitalypb.ListConflictFilesRequest, stream gitalypb.ConflictsService_ListConflictFilesServer) error {
	ctx := stream.Context()

	if err := validateListConflictFilesRequest(ctx, s.locator, request); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	_, quarantineRepo, cleanup, err := s.quarantinedRepo(ctx, request.GetRepository())
	if err != nil {
		return err
	}
	defer cleanup()

	ours, err := quarantineRepo.ResolveRevision(ctx, git.Revision(request.GetOurCommitOid()+"^{commit}"))
	if err != nil {
		return structerr.NewFailedPrecondition("could not lookup 'our' OID: %w", err)
	}

	theirs, err := quarantineRepo.ResolveRevision(ctx, git.Revision(request.GetTheirCommitOid()+"^{commit}"))
	if err != nil {
		return structerr.NewFailedPrecondition("could not lookup 'their' OID: %w", err)
	}

	return s.conflictFilesWithGitMergeTree(ctx, request, stream, ours, theirs, quarantineRepo)
}

func (s *server) conflictFilesWithGitMergeTree(
	ctx context.Context,
	request *gitalypb.ListConflictFilesRequest,
	stream gitalypb.ConflictsService_ListConflictFilesServer,
	ours, theirs git.ObjectID,
	repo *localrepo.Repo,
) error {
	var mergeConflictErr *localrepo.MergeTreeConflictError

	oid, err := repo.MergeTree(ctx, ours.String(), theirs.String(), localrepo.WithAllowUnrelatedHistories())
	if err == nil {
		// When there are no errors, it denotes that there are no conflicts.
		return nil
	} else if !errors.As(err, &mergeConflictErr) {
		// If its not a conflict, we return the error to the user.
		return structerr.NewInternal("couldn't find conflict: %w", err)
	}

	type conflictHeader struct {
		theirPath    string
		ourPath      string
		ancestorPath string
		ourMode      int32
	}

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return err
	}
	defer cancel()

	// We need to combine data with same path, but we also want to retain
	// the ordering. We use a map to track the data with the same path, but
	// retain ordering by using a slice.
	pathToConflict := make(map[string]*conflictHeader)
	var conflicts []*conflictHeader

	for _, conflictFile := range mergeConflictErr.ConflictingFileInfo {
		val, ok := pathToConflict[conflictFile.FileName]
		if !ok {
			val = &conflictHeader{}
			conflicts = append(conflicts, val)
		}

		switch conflictFile.Stage {
		case localrepo.MergeStageAncestor:
			val.ancestorPath = conflictFile.FileName
		case localrepo.MergeStageOurs:
			val.ourPath = conflictFile.FileName
			val.ourMode = conflictFile.Mode
		case localrepo.MergeStageTheirs:
			val.theirPath = conflictFile.FileName
		}

		pathToConflict[conflictFile.FileName] = val
	}

	sender := newConflictFilesSender(stream)
	contentReader := bufio.NewReaderSize(nil, readBufferSize)

	for _, conflict := range conflicts {
		if !request.GetAllowTreeConflicts() && (conflict.theirPath == "" || conflict.ourPath == "") {
			return structerr.NewFailedPrecondition("conflict side missing")
		}

		sender.addHeader(&gitalypb.ConflictFileHeader{
			CommitOid:    request.GetOurCommitOid(),
			TheirPath:    []byte(conflict.theirPath),
			OurPath:      []byte(conflict.ourPath),
			AncestorPath: []byte(conflict.ancestorPath),
			OurMode:      conflict.ourMode,
		})

		// Clients do not want the contents of the conflicted files, so we skip this section.
		if request.GetSkipContent() {
			continue
		}

		path := conflict.ourPath
		if path == "" {
			path = conflict.theirPath
		}

		fileOID, err := repo.ResolveRevision(ctx, oid.Revision()+":"+git.Revision(path))
		if err != nil {
			return structerr.NewFailedPrecondition("getting file revision: %w", err)
		}

		object, err := objectReader.Object(ctx, fileOID.Revision())
		if err != nil {
			return structerr.NewFailedPrecondition("getting objectreader: %w", err)
		}

		// Git2Go conflict markers have filenames and git-merge-tree(1) has commit OIDs,
		// to keep the content the same, we replace commit OID with filenames.
		replacements := []oidReplacement{
			{oid: []byte(ours), path: []byte(conflict.ourPath)},
			{oid: []byte(theirs), path: []byte(conflict.theirPath)},
		}

		contentReader.Reset(object)
		if err := streamConflictContent(contentReader, replacements, sender.addContent); err != nil {
			return err
		}
	}

	return sender.flush()
}

// readBufferSize is the size of the buffer used to read conflicted objects from Git.
const readBufferSize = 32 * 1024

// oidReplacement describes a commit OID in the conflict markers written by git-merge-tree(1)
// that gets replaced with the path of the conflicting file.
type oidReplacement struct {
	oid  []byte
	path []byte
}

// streamConflictContent reads the conflicted file content from r, replaces commit OIDs in it
// as described by replacements and passes the result to emit in pieces. The content is
// processed in chunks of at most r.Size() bytes so that memory usage doesn't depend on the size
// of the file. All OIDs in replacements must be of the same length, which must be smaller than
// r.Size(). The slices passed to emit are only valid until emit returns.
//
// An error is returned if the content isn't valid UTF-8. As the content is streamed, emit may
// have been called with parts of the content before the invalid sequence was found.
func streamConflictContent(r *bufio.Reader, replacements []oidReplacement, emit func([]byte) error) error {
	oidLen := len(replacements[0].oid)

	// Bytes of the buffered data before validated have already been checked to be valid
	// UTF-8.
	validated := 0

	for {
		// Peek only returns fewer bytes than requested if it encountered an error, so
		// data is either a full buffer or the remainder of the content.
		data, err := r.Peek(r.Size())
		eof := errors.Is(err, io.EOF)
		if err != nil && !eof {
			return structerr.NewInternal("reading conflict object: %w", err)
		}

		// A multi-byte character may be split across reads, so we only validate up to the
		// last complete character and retain the rest until the next read.
		end := len(data)
		if !eof {
			end = completeRunesEnd(data)
		}

		if !utf8.Valid(data[validated:end]) {
			return structerr.NewFailedPrecondition("unsupported encoding")
		}

		// An OID may be split across reads, so we retain enough bytes to cover a partial
		// match unless we have reached the end of the content.
		safe := end
		if !eof {
			safe = max(0, end-(oidLen-1))
		}

		consumed, err := replaceOIDs(data[:end], safe, replacements, emit)
		if err != nil {
			return err
		}

		if eof {
			return nil
		}

		if _, err := r.Discard(consumed); err != nil {
			return structerr.NewInternal("discarding conflict object data: %w", err)
		}
		validated = end - consumed
	}
}

// replaceOIDs emits data up to safe with all OIDs that start before safe replaced by their
// paths. It returns the number of bytes of data it has consumed, which may exceed safe when an
// OID starts before but ends after it.
func replaceOIDs(data []byte, safe int, replacements []oidReplacement, emit func([]byte) error) (int, error) {
	consumed := 0

	// nextMatch caches the position of the next occurrence of each OID so that data is only
	// searched again for an OID once its previous occurrence has been consumed. A negative
	// value means that there are no further occurrences.
	nextMatch := make([]int, len(replacements))
	for i, replacement := range replacements {
		nextMatch[i] = bytes.Index(data, replacement.oid)
	}

	for consumed < safe {
		matchIdx, match := -1, oidReplacement{}
		for i, replacement := range replacements {
			if nextMatch[i] >= 0 && nextMatch[i] < consumed {
				nextMatch[i] = bytes.Index(data[consumed:], replacement.oid)
				if nextMatch[i] >= 0 {
					nextMatch[i] += consumed
				}
			}

			if nextMatch[i] >= 0 && (matchIdx < 0 || nextMatch[i] < matchIdx) {
				matchIdx, match = nextMatch[i], replacement
			}
		}

		if matchIdx < 0 || matchIdx >= safe {
			if err := emit(data[consumed:safe]); err != nil {
				return 0, err
			}

			return safe, nil
		}

		if err := emit(data[consumed:matchIdx]); err != nil {
			return 0, err
		}
		if err := emit(match.path); err != nil {
			return 0, err
		}

		consumed = matchIdx + len(match.oid)
	}

	return consumed, nil
}

// completeRunesEnd returns the length of the prefix of p that doesn't end with an incomplete
// UTF-8 encoded character.
func completeRunesEnd(p []byte) int {
	for i := len(p) - 1; i >= 0 && i >= len(p)-utf8.UTFMax; i-- {
		if utf8.RuneStart(p[i]) {
			if utf8.FullRune(p[i:]) {
				return len(p)
			}

			return i
		}
	}

	return len(p)
}

// conflictFilesSender batches conflict file headers and content into responses. Content is
// copied into a buffer of streamio.WriteBufferSize bytes that is reused across responses, and a
// response is sent whenever the buffer is full.
type conflictFilesSender struct {
	stream  gitalypb.ConflictsService_ListConflictFilesServer
	files   []*gitalypb.ConflictFile
	content []byte
}

func newConflictFilesSender(stream gitalypb.ConflictsService_ListConflictFilesServer) *conflictFilesSender {
	return &conflictFilesSender{
		stream:  stream,
		content: make([]byte, 0, streamio.WriteBufferSize),
	}
}

func (s *conflictFilesSender) addHeader(header *gitalypb.ConflictFileHeader) {
	s.files = append(s.files, &gitalypb.ConflictFile{
		ConflictFilePayload: &gitalypb.ConflictFile_Header{Header: header},
	})
}

func (s *conflictFilesSender) addContent(p []byte) error {
	for len(p) > 0 {
		start := len(s.content)
		n := min(len(p), cap(s.content)-start)

		// The content buffer never grows beyond its capacity, so the slices referenced by
		// pending files stay valid until the buffer is reset in flush.
		s.content = append(s.content, p[:n]...)
		p = p[n:]

		// Content is always appended to the end of the buffer, so if the last pending entry
		// is content it directly precedes the new data and can be extended. This avoids
		// creating an entry for each call.
		if prev := s.lastContent(); prev != nil {
			prev.Content = prev.Content[:len(prev.Content)+n]
		} else {
			s.files = append(s.files, &gitalypb.ConflictFile{
				ConflictFilePayload: &gitalypb.ConflictFile_Content{Content: s.content[start:len(s.content)]},
			})
		}

		if len(s.content) == cap(s.content) {
			if err := s.flush(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *conflictFilesSender) lastContent() *gitalypb.ConflictFile_Content {
	if len(s.files) == 0 {
		return nil
	}

	content, _ := s.files[len(s.files)-1].GetConflictFilePayload().(*gitalypb.ConflictFile_Content)
	return content
}

// flush sends all pending files, if any.
func (s *conflictFilesSender) flush() error {
	if len(s.files) == 0 {
		return nil
	}

	if err := s.stream.Send(&gitalypb.ListConflictFilesResponse{Files: s.files}); err != nil {
		return structerr.NewInternal("error streaming conflict files: %w", err)
	}

	s.files = s.files[:0]
	s.content = s.content[:0]

	return nil
}

func validateListConflictFilesRequest(ctx context.Context, locator storage.Locator, in *gitalypb.ListConflictFilesRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}
	if in.GetOurCommitOid() == "" {
		return fmt.Errorf("empty OurCommitOid")
	}
	if in.GetTheirCommitOid() == "" {
		return fmt.Errorf("empty TheirCommitOid")
	}

	return nil
}
