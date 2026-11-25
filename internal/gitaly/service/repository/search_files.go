package repository

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"unicode/utf8"

	"gitlab.com/gitlab-org/gitaly/v19/internal/command"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v19/streamio"
)

const (
	surroundContext = "2"

	// searchFilesFilterMaxLength controls the maximum length of the regular
	// expression to thwart excessive resource usage when filtering
	searchFilesFilterMaxLength = 1000

	// searchFilesPathspecMaxLength is the maximum length of a single
	// SearchFilesByContent pathspec.
	searchFilesPathspecMaxLength = 1000

	// searchFilesMaxPathspecs is the maximum number of pathspecs a
	// SearchFilesByContent request may pass.
	searchFilesMaxPathspecs = 32
)

var contentDelimiter = []byte("--\n")

func (s *server) SearchFilesByContent(req *gitalypb.SearchFilesByContentRequest, stream gitalypb.RepositoryService_SearchFilesByContentServer) error {
	// Cancelling the context kills git-grep(1) when we return before it has
	// finished, for example because the limit has been reached.
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	if err := validateSearchFilesRequest(ctx, s.locator, req); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	if err := validateSearchFilesPathspecs(req.GetPathspecs()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	pathspecs := make([]string, 0, len(req.GetPathspecs()))
	for _, pathspec := range req.GetPathspecs() {
		pathspecs = append(pathspecs, string(pathspec))
	}

	repo := s.localRepoFactory.Build(req.GetRepository())

	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name: "grep",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--ignore-case"},
			gitcmd.Flag{Name: "-I"},
			gitcmd.Flag{Name: "--line-number"},
			gitcmd.Flag{Name: "--null"},
			gitcmd.ValueFlag{Name: "--before-context", Value: surroundContext},
			gitcmd.ValueFlag{Name: "--after-context", Value: surroundContext},
			gitcmd.Flag{Name: "--perl-regexp"},
			gitcmd.ValueFlag{Name: "-e", Value: req.GetQuery()},
		},
		Args: []string{
			string(req.GetRef()),
		},
		PostSepArgs: pathspecs,
	}, gitcmd.WithSetupStdout())
	if err != nil {
		return structerr.NewInternal("cmd start failed: %w", err)
	}

	if err = sendSearchFilesResultChunked(cmd, stream, req.GetLimit(), req.GetMaxMatchBytes()); err != nil {
		return structerr.NewInternal("sending chunked response failed: %w", err)
	}

	return nil
}

// sendSearchFilesResultChunked streams the matches in git-grep(1)'s output to
// the client. Output is read and sent piece by piece, so neither a whole match
// nor a whole line is ever held in memory.
func sendSearchFilesResultChunked(cmd *command.Command, stream gitalypb.RepositoryService_SearchFilesByContentServer, limit, maxMatchBytes uint32) error {
	reader := bufio.NewReader(cmd)
	match := newSearchFilesMatchWriter(stream, int(maxMatchBytes))
	atLineStart := true
	var sent uint32

	for {
		// ReadSlice returns bufio.ErrBufferFull with a partial line when the
		// line is longer than the reader's buffer. We handle that like any
		// other piece of a line instead of allocating the whole line.
		piece, err := reader.ReadSlice('\n')
		if err != nil && !errors.Is(err, bufio.ErrBufferFull) && !errors.Is(err, io.EOF) {
			return fmt.Errorf("reading output: %w", err)
		}

		if len(piece) > 0 {
			lineEnd := piece[len(piece)-1] == '\n'

			// Only a whole line can be the delimiter. A piece of a longer line
			// that happens to read "--\n" is content.
			if atLineStart && bytes.Equal(piece, contentDelimiter) {
				if err := match.end(); err != nil {
					return err
				}

				sent++
				if limit > 0 && sent >= limit {
					return nil
				}
			} else if err := match.write(piece, lineEnd); err != nil {
				return err
			}

			atLineStart = lineEnd
		}

		if errors.Is(err, io.EOF) {
			break
		}
	}

	if match.started {
		return match.end()
	}

	return nil
}

// searchFilesMatchWriter writes a single match to the stream, applying
// maxBytes to it.
type searchFilesMatchWriter struct {
	stream gitalypb.RepositoryService_SearchFilesByContentServer
	writer *bufio.Writer

	// maxBytes is the most bytes sent for a match, or 0 for no limit.
	maxBytes int
	// written is the number of bytes sent for the current match.
	written int
	// line holds the current line until we know whether it fits. It is only
	// used when maxBytes is set, and never grows much beyond it.
	line []byte
	// discarding is set once a line didn't fit. The rest of the match is
	// dropped.
	discarding bool
	// started is set once the current match has any content.
	started bool
}

func newSearchFilesMatchWriter(stream gitalypb.RepositoryService_SearchFilesByContentServer, maxBytes int) *searchFilesMatchWriter {
	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.SearchFilesByContentResponse{MatchData: bytes.Clone(p)})
	})

	return &searchFilesMatchWriter{
		stream:   stream,
		writer:   bufio.NewWriterSize(sw, streamio.WriteBufferSize),
		maxBytes: maxBytes,
	}
}

// write adds a piece of a line to the current match. lineEnd says whether
// the piece completes the line.
func (w *searchFilesMatchWriter) write(piece []byte, lineEnd bool) error {
	w.started = true

	if w.discarding {
		return nil
	}

	if w.maxBytes == 0 {
		_, err := w.writer.Write(piece)
		return err
	}

	w.line = append(w.line, piece...)
	if len(w.line) > w.maxBytes-w.written {
		return w.overflow()
	}

	if lineEnd {
		return w.flushLine()
	}

	return nil
}

// overflow handles a line that doesn't fit into the remaining budget.
func (w *searchFilesMatchWriter) overflow() error {
	w.discarding = true
	defer func() { w.line = w.line[:0] }()

	if w.written > 0 {
		return nil
	}

	// Without its first line a match wouldn't say which file and line it
	// comes from, so we send as much of the line as fits instead.
	if _, err := w.writer.Write(truncateToRune(w.line, w.maxBytes-1)); err != nil {
		return err
	}

	return w.writer.WriteByte('\n')
}

func (w *searchFilesMatchWriter) flushLine() error {
	if _, err := w.writer.Write(w.line); err != nil {
		return err
	}

	w.written += len(w.line)
	w.line = w.line[:0]

	return nil
}

// end finishes the current match and sends end_of_match.
func (w *searchFilesMatchWriter) end() error {
	// The last line is only incomplete if the output didn't end with a
	// newline. write has already checked that it fits.
	if !w.discarding && len(w.line) > 0 {
		if err := w.flushLine(); err != nil {
			return err
		}
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}

	w.written = 0
	w.line = w.line[:0]
	w.discarding = false
	w.started = false

	return w.stream.Send(&gitalypb.SearchFilesByContentResponse{EndOfMatch: true})
}

// truncateToRune returns at most the first n bytes of b, without cutting a
// multi-byte UTF-8 character in half.
func truncateToRune(b []byte, n int) []byte {
	if n <= 0 {
		return nil
	}
	if len(b) <= n {
		return b
	}

	b = b[:n]
	for i := len(b) - 1; i >= 0 && i >= len(b)-utf8.UTFMax; i-- {
		if utf8.RuneStart(b[i]) {
			if !utf8.FullRune(b[i:]) {
				b = b[:i]
			}
			break
		}
	}

	return b
}

func (s *server) SearchFilesByName(req *gitalypb.SearchFilesByNameRequest, stream gitalypb.RepositoryService_SearchFilesByNameServer) error {
	ctx := stream.Context()

	if err := validateSearchFilesRequest(ctx, s.locator, req); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	var filter *regexp.Regexp
	if req.GetFilter() != "" {
		if len(req.GetFilter()) > searchFilesFilterMaxLength {
			return structerr.NewInvalidArgument("filter exceeds maximum length")
		}
		var err error
		filter, err = regexp.Compile(req.GetFilter())
		if err != nil {
			return structerr.NewInvalidArgument("filter did not compile: %w", err)
		}
	}

	repo := s.localRepoFactory.Build(req.GetRepository())

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name: "ls-tree",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--full-tree"},
			gitcmd.Flag{Name: "--name-status"},
			gitcmd.Flag{Name: "-r"},
			// We use -z to force NULL byte termination here to prevent git from
			// quoting and escaping unusual file names. Lstree parser would be a
			// more ideal solution. Unfortunately, it supports parsing full
			// output while we are interested in the filenames only.
			gitcmd.Flag{Name: "-z"},
		},
		Args: []string{
			string(req.GetRef()),
		},
		PostSepArgs: []string{
			req.GetQuery(),
		},
	}, gitcmd.WithSetupStdout())
	if err != nil {
		return structerr.NewInternal("cmd start failed: %w", err)
	}

	files, err := parseLsTree(objectHash, cmd, filter, int(req.GetOffset()), int(req.GetLimit()))
	if err != nil {
		return err
	}

	return stream.Send(&gitalypb.SearchFilesByNameResponse{Files: files})
}

type searchFilesRequest interface {
	GetRepository() *gitalypb.Repository
	GetRef() []byte
	GetQuery() string
}

func validateSearchFilesRequest(ctx context.Context, locator storage.Locator, req searchFilesRequest) error {
	if err := locator.ValidateRepository(ctx, req.GetRepository()); err != nil {
		return err
	}

	if len(req.GetQuery()) == 0 {
		return errors.New("no query given")
	}

	if len(req.GetRef()) == 0 {
		return errors.New("no ref given")
	}

	if bytes.HasPrefix(req.GetRef(), []byte("-")) {
		return errors.New("invalid ref argument")
	}

	return nil
}

// validateSearchFilesPathspecs checks that the pathspecs only use pathspec
// magic that is cheap to evaluate. Other magic, like "attr", makes git-grep(1)
// do more work per path, and "top" is meaningless when searching a tree.
func validateSearchFilesPathspecs(pathspecs [][]byte) error {
	if len(pathspecs) > searchFilesMaxPathspecs {
		return fmt.Errorf("too many pathspecs: %d, at most %d are allowed", len(pathspecs), searchFilesMaxPathspecs)
	}

	for _, pathspec := range pathspecs {
		if err := validateSearchFilesPathspec(pathspec); err != nil {
			return fmt.Errorf("invalid pathspec %q: %w", pathspec, err)
		}
	}

	return nil
}

func validateSearchFilesPathspec(pathspec []byte) error {
	if len(pathspec) > searchFilesPathspecMaxLength {
		return errors.New("pathspec exceeds maximum length")
	}

	if bytes.IndexByte(pathspec, 0) >= 0 {
		return errors.New("pathspec contains a NUL byte")
	}

	var pattern []byte
	switch {
	case bytes.HasPrefix(pathspec, []byte(":(")):
		end := bytes.IndexByte(pathspec, ')')
		if end < 0 {
			return errors.New("unterminated pathspec magic")
		}

		var glob, literal bool
		for _, magic := range bytes.Split(pathspec[len(":("):end], []byte(",")) {
			switch string(magic) {
			case "exclude", "icase":
			case "glob":
				glob = true
			case "literal":
				literal = true
			default:
				return fmt.Errorf("unsupported pathspec magic %q", magic)
			}
		}

		if glob && literal {
			return errors.New("pathspec magic \"glob\" and \"literal\" are incompatible")
		}

		pattern = pathspec[end+1:]
	case bytes.HasPrefix(pathspec, []byte(":")):
		// The short form is a run of magic signatures, optionally terminated
		// by a colon. The run ends at the first character that isn't a magic
		// signature, which starts the pattern.
		pattern = bytes.TrimLeft(pathspec[len(":"):], "!^")
		if bytes.HasPrefix(pattern, []byte(":")) {
			pattern = pattern[len(":"):]
		} else if len(pattern) > 0 && isShortPathspecMagic(pattern[0]) {
			return fmt.Errorf("unsupported pathspec magic %q", pattern[:1])
		}
	default:
		pattern = pathspec
	}

	if len(pattern) == 0 {
		return errors.New("empty pathspec pattern")
	}

	// git-grep(1) fails on patterns outside of the repository instead of
	// matching nothing, so reject them up front.
	if bytes.HasPrefix(pattern, []byte("/")) {
		return errors.New("pathspec is outside the repository")
	}
	if cleaned := path.Clean(string(pattern)); cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return errors.New("pathspec is outside the repository")
	}

	return nil
}

// isShortPathspecMagic reports whether Git treats c as a magic signature in
// the short form of a pathspec, mirroring GIT_PATHSPEC_MAGIC in Git's ctype.c.
// Other characters end the magic signatures and start the pattern.
func isShortPathspecMagic(c byte) bool {
	return strings.IndexByte("!\"#%&',-/:;<=>@_`~", c) >= 0
}

func parseLsTree(objectHash git.ObjectHash, cmd *command.Command, filter *regexp.Regexp, offset int, limit int) ([][]byte, error) {
	var files [][]byte
	var index int
	parser := localrepo.NewParser(cmd, objectHash)

	for {
		path, err := parser.NextEntryPath()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if filter != nil && !filter.Match(path) {
			continue
		}

		index++
		if index > offset {
			files = append(files, path)
		}
		if limit > 0 && len(files) >= limit {
			break
		}
	}

	return files, nil
}
