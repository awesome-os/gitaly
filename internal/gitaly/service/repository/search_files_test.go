package repository

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
)

func TestSearchFilesByContent(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "a", Mode: "100644", Content: "a-1\na-2\na-3\na-4\na-5\na-6\n"},
		{Path: "b", Mode: "100644", Content: "b-1\nb-2\nb-3\nb-4\nb-5\nb-6\n"},
		{Path: "dir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{Path: "c", Mode: "100644", Content: "c-1\nc-2\nc-3\nc-4\nc-5\nc-6\n"},
		})},
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"), gittest.WithTree(treeID))

	largeFilesCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "huge-file", Mode: "100644", Content: strings.Repeat("abcdefghi\n", 210000)},
		gittest.TreeEntry{Path: "huge-utf8", Mode: "100644", Content: strings.Repeat("你今天吃了什么东西?\n", 70000)},
	))

	largeLineCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "huge-line-file", Mode: "100644", Content: strings.Repeat("abcdefghi", 1024*64) + "\n"},
	))

	dashedCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "-dashed", Mode: "100644", Content: "-dashed\n"},
	))

	multiHunkCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "m", Mode: "100644", Content: "m-1\nm-2\nm-3\nm-4\nm-5\nm-6\nm-7\nm-8\nm-9\nm-10\nm-11\nm-12\n"},
	))

	// The first line of git-grep(1)'s output is the commit ID, the path, the
	// line number and the content. Size it so that bufio.Reader's default
	// 4096-byte buffer splits it right before a trailing "--\n".
	dashedLinePrefix := gittest.DefaultObjectHash.EncodedLen() + len(":dashes\x001\x00")
	dashedLine := strings.Repeat("-", 4096+len("--")-dashedLinePrefix)
	dashedLineCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "dashes", Mode: "100644", Content: dashedLine + "\n"},
	))

	pathsCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "README.MD", Mode: "100644", Content: "needle\n"},
		gittest.TreeEntry{Path: "main.go", Mode: "100644", Content: "needle\n"},
		gittest.TreeEntry{Path: "dir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{Path: "notes.txt", Mode: "100644", Content: "needle\n"},
			{Path: "util.GO", Mode: "100644", Content: "needle\n"},
		})},
	))

	// git-grep(1) only searches regular files, so a symlink is neither
	// followed nor searched for its target.
	symlinkCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "link", Mode: "120000", Content: "../../../"},
		gittest.TreeEntry{Path: "inside", Mode: "100644", Content: "needle\n"},
	))

	generateMatch := func(ref, file string, from, to int, generateLine func(line int) string) string {
		var builder strings.Builder
		for i := from; i <= to; i++ {
			_, err := builder.WriteString(fmt.Sprintf("%s:%s\x00%d\x00%s\n", ref, file, i, generateLine(i)))
			require.NoError(t, err)
		}
		return builder.String()
	}
	prefixedLine := func(prefix string) func(int) string {
		return func(line int) string { return fmt.Sprintf("%s-%d", prefix, line) }
	}
	staticLine := func(content string) func(int) string {
		return func(int) string { return content }
	}
	linePrefix := func(ref, file string, line int) string {
		return fmt.Sprintf("%s:%s\x00%d\x00", ref, file, line)
	}
	needleMatch := func(path string) string {
		return generateMatch(pathsCommit.String(), path, 1, 1, staticLine("needle"))
	}
	// The match lines for "a-6" on "branch" are all this long.
	const branchLineLength = uint32(len("branch:a\x004\x00a-4\n"))

	for _, tc := range []struct {
		desc            string
		request         *gitalypb.SearchFilesByContentRequest
		expectedErr     error
		expectedMatches []string
	}{
		{
			desc: "no repo",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "empty request",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
			},
			expectedErr: structerr.NewInvalidArgument("no query given"),
		},
		{
			desc: "only query given",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "foo",
			},
			expectedErr: structerr.NewInvalidArgument("no ref given"),
		},
		{
			desc: "invalid ref argument",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      ".",
				Ref:        []byte("--no-index"),
			},
			expectedErr: structerr.NewInvalidArgument("invalid ref argument"),
		},
		{
			desc: "single matching file",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "a-6",
				Ref:        []byte("branch"),
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
			},
		},
		{
			desc: "multiple matching files",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "6",
				Ref:        []byte("branch"),
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
				generateMatch("branch", "b", 4, 6, prefixedLine("b")),
				generateMatch("branch", "dir/c", 4, 6, prefixedLine("c")),
			},
		},
		{
			desc: "multiple matching files with multiple matching lines",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "[ac]-[16]",
				Ref:        []byte("branch"),
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 1, 6, prefixedLine("a")),
				generateMatch("branch", "dir/c", 1, 6, prefixedLine("c")),
			},
		},
		{
			desc: "no results",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "这个应该没有结果",
				Ref:        []byte("branch"),
			},
			expectedMatches: nil,
		},
		{
			desc: "with regexp limiter only recognized by pcre",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "(*LIMIT_MATCH=1)a",
				Ref:        []byte("branch"),
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 1, 6, prefixedLine("a")),
			},
		},
		{
			desc: "large file",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "abcdefg",
				Ref:        []byte(largeFilesCommit),
			},
			expectedMatches: []string{
				generateMatch(largeFilesCommit.String(), "huge-file", 1, 210000, staticLine("abcdefghi")),
			},
		},
		{
			desc: "large file with unicode",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "什么东西",
				Ref:        []byte(largeFilesCommit),
			},
			expectedMatches: []string{
				generateMatch(largeFilesCommit.String(), "huge-utf8", 1, 70000, staticLine("你今天吃了什么东西?")),
			},
		},
		{
			desc: "file with large line",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "abcdefg",
				Ref:        []byte(largeLineCommit),
			},
			expectedMatches: []string{
				generateMatch(largeLineCommit.String(), "huge-line-file", 1, 1, staticLine(strings.Repeat("abcdefghi", 1024*64))),
			},
		},
		{
			desc: "limit of zero returns all matches",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "6",
				Ref:        []byte("branch"),
				Limit:      0,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
				generateMatch("branch", "b", 4, 6, prefixedLine("b")),
				generateMatch("branch", "dir/c", 4, 6, prefixedLine("c")),
			},
		},
		{
			desc: "limit of one",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "6",
				Ref:        []byte("branch"),
				Limit:      1,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
			},
		},
		{
			desc: "limit smaller than the number of matches",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "6",
				Ref:        []byte("branch"),
				Limit:      2,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
				generateMatch("branch", "b", 4, 6, prefixedLine("b")),
			},
		},
		{
			desc: "limit equal to the number of matches",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "6",
				Ref:        []byte("branch"),
				Limit:      3,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
				generateMatch("branch", "b", 4, 6, prefixedLine("b")),
				generateMatch("branch", "dir/c", 4, 6, prefixedLine("c")),
			},
		},
		{
			desc: "limit larger than the number of matches",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "6",
				Ref:        []byte("branch"),
				Limit:      10,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
				generateMatch("branch", "b", 4, 6, prefixedLine("b")),
				generateMatch("branch", "dir/c", 4, 6, prefixedLine("c")),
			},
		},
		{
			desc: "multiple hunks in one file",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "m-(1|12)$",
				Ref:        []byte(multiHunkCommit),
			},
			expectedMatches: []string{
				generateMatch(multiHunkCommit.String(), "m", 1, 3, prefixedLine("m")),
				generateMatch(multiHunkCommit.String(), "m", 10, 12, prefixedLine("m")),
			},
		},
		{
			desc: "limit counts each hunk as a match",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "m-(1|12)$",
				Ref:        []byte(multiHunkCommit),
				Limit:      1,
			},
			expectedMatches: []string{
				generateMatch(multiHunkCommit.String(), "m", 1, 3, prefixedLine("m")),
			},
		},
		{
			desc: "limit with large files",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "abcdefg|什么东西",
				Ref:        []byte(largeFilesCommit),
				Limit:      1,
			},
			expectedMatches: []string{
				generateMatch(largeFilesCommit.String(), "huge-file", 1, 210000, staticLine("abcdefghi")),
			},
		},
		{
			desc: "line split right before a trailing delimiter",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "---",
				Ref:        []byte(dashedLineCommit),
			},
			expectedMatches: []string{
				generateMatch(dashedLineCommit.String(), "dashes", 1, 1, staticLine(dashedLine)),
			},
		},
		{
			desc: "max match bytes of zero returns whole matches",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:    repoProto,
				Query:         "a-6",
				Ref:           []byte("branch"),
				MaxMatchBytes: 0,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
			},
		},
		{
			desc: "max match bytes larger than the match",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:    repoProto,
				Query:         "a-6",
				Ref:           []byte("branch"),
				MaxMatchBytes: 3*branchLineLength + 1,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
			},
		},
		{
			desc: "max match bytes equal to the match",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:    repoProto,
				Query:         "a-6",
				Ref:           []byte("branch"),
				MaxMatchBytes: 3 * branchLineLength,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
			},
		},
		{
			desc: "max match bytes keeps the lines that fit",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:    repoProto,
				Query:         "a-6",
				Ref:           []byte("branch"),
				MaxMatchBytes: 3*branchLineLength - 1,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 5, prefixedLine("a")),
			},
		},
		{
			desc: "max match bytes applies to each match",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:    repoProto,
				Query:         "6",
				Ref:           []byte("branch"),
				MaxMatchBytes: branchLineLength,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 4, prefixedLine("a")),
				generateMatch("branch", "b", 4, 4, prefixedLine("b")),
				// "dir/c" makes the first line longer than the others, so it is cut.
				generateMatch("branch", "dir/c", 4, 4, prefixedLine("c"))[:branchLineLength-1] + "\n",
			},
		},
		{
			desc: "max match bytes cuts a first line that doesn't fit",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:    repoProto,
				Query:         "a-6",
				Ref:           []byte("branch"),
				MaxMatchBytes: branchLineLength - 1,
			},
			expectedMatches: []string{
				linePrefix("branch", "a", 4) + "a-\n",
			},
		},
		{
			desc: "max match bytes with a large line",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:    repoProto,
				Query:         "abcdefg",
				Ref:           []byte(largeLineCommit),
				MaxMatchBytes: 1024,
			},
			expectedMatches: []string{
				(linePrefix(largeLineCommit.String(), "huge-line-file", 1) + strings.Repeat("abcdefghi", 1024))[:1023] + "\n",
			},
		},
		{
			desc: "max match bytes with a large file",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:    repoProto,
				Query:         "abcdefg",
				Ref:           []byte(largeFilesCommit),
				MaxMatchBytes: uint32(len(generateMatch(largeFilesCommit.String(), "huge-file", 1, 100, staticLine("abcdefghi")))),
			},
			expectedMatches: []string{
				generateMatch(largeFilesCommit.String(), "huge-file", 1, 100, staticLine("abcdefghi")),
			},
		},
		{
			desc: "max match bytes doesn't cut a character in half",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "什么东西",
				Ref:        []byte(largeFilesCommit),
				// Leaves room for the prefix, "你" and the first byte of "今",
				// plus the newline.
				MaxMatchBytes: uint32(len(linePrefix(largeFilesCommit.String(), "huge-utf8", 1)) + len("你") + 2),
			},
			expectedMatches: []string{
				linePrefix(largeFilesCommit.String(), "huge-utf8", 1) + "你\n",
			},
		},
		{
			desc: "max match bytes with limit",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:    repoProto,
				Query:         "6",
				Ref:           []byte("branch"),
				Limit:         2,
				MaxMatchBytes: branchLineLength,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 4, prefixedLine("a")),
				generateMatch("branch", "b", 4, 4, prefixedLine("b")),
			},
		},
		{
			desc: "without pathspecs",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
			},
			expectedMatches: []string{
				needleMatch("README.MD"),
				needleMatch("dir/notes.txt"),
				needleMatch("dir/util.GO"),
				needleMatch("main.go"),
			},
		},
		{
			desc: "pathspec without magic",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs("*.go"),
			},
			expectedMatches: []string{
				needleMatch("main.go"),
			},
		},
		{
			desc: "pathspec with icase",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":(icase)*.go"),
			},
			expectedMatches: []string{
				needleMatch("dir/util.GO"),
				needleMatch("main.go"),
			},
		},
		{
			desc: "pathspec matching a directory",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":(icase)*DIR/*"),
			},
			expectedMatches: []string{
				needleMatch("dir/notes.txt"),
				needleMatch("dir/util.GO"),
			},
		},
		{
			desc: "pathspec with glob",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":(glob)*"),
			},
			expectedMatches: []string{
				needleMatch("README.MD"),
				needleMatch("main.go"),
			},
		},
		{
			desc: "pathspec with literal",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":(literal)*.go"),
			},
			expectedMatches: nil,
		},
		{
			desc: "positive pathspecs are OR-ed",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs("README.MD", "main.go"),
			},
			expectedMatches: []string{
				needleMatch("README.MD"),
				needleMatch("main.go"),
			},
		},
		{
			desc: "exclude pathspec only",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":(exclude)main.go"),
			},
			expectedMatches: []string{
				needleMatch("README.MD"),
				needleMatch("dir/notes.txt"),
				needleMatch("dir/util.GO"),
			},
		},
		{
			desc: "short exclude pathspec",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":!main.go"),
			},
			expectedMatches: []string{
				needleMatch("README.MD"),
				needleMatch("dir/notes.txt"),
				needleMatch("dir/util.GO"),
			},
		},
		{
			desc: "short exclude pathspec with caret and colon",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":^:*.go"),
			},
			expectedMatches: []string{
				needleMatch("README.MD"),
				needleMatch("dir/notes.txt"),
				needleMatch("dir/util.GO"),
			},
		},
		{
			desc: "positive and exclude pathspecs",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":(icase)*.go", ":(exclude,icase)DIR/*"),
			},
			expectedMatches: []string{
				needleMatch("main.go"),
			},
		},
		{
			desc: "pathspecs with limit",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":(icase)*.go"),
				Limit:      1,
			},
			expectedMatches: []string{
				needleMatch("dir/util.GO"),
			},
		},
		{
			desc: "pathspecs with max match bytes",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:    repoProto,
				Query:         "needle",
				Ref:           []byte(pathsCommit),
				Pathspecs:     pathspecs("main.go"),
				MaxMatchBytes: uint32(len(needleMatch("main.go")) - 1),
			},
			expectedMatches: []string{
				needleMatch("main.go")[:len(needleMatch("main.go"))-2] + "\n",
			},
		},
		{
			desc: "symlinks are not searched",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      `needle|\.\.`,
				Ref:        []byte(symlinkCommit),
			},
			expectedMatches: []string{
				generateMatch(symlinkCommit.String(), "inside", 1, 1, staticLine("needle")),
			},
		},
		{
			desc: "pathspecs don't descend into symlinks",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      `needle|\.\.`,
				Ref:        []byte(symlinkCommit),
				Pathspecs:  pathspecs("link", "link/*", ":(glob)link/**"),
			},
			expectedMatches: nil,
		},
		{
			desc: "pathspec with unsupported magic",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":(attr:foo)a"),
			},
			expectedErr: structerr.NewInvalidArgument(`invalid pathspec ":(attr:foo)a": unsupported pathspec magic "attr:foo"`),
		},
		{
			desc: "pathspec with top magic",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":(top)a"),
			},
			expectedErr: structerr.NewInvalidArgument(`invalid pathspec ":(top)a": unsupported pathspec magic "top"`),
		},
		{
			desc: "pathspec with short top magic",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":/a"),
			},
			expectedErr: structerr.NewInvalidArgument(`invalid pathspec ":/a": unsupported pathspec magic "/"`),
		},
		{
			desc: "pathspec with glob and literal",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":(glob,literal)a"),
			},
			expectedErr: structerr.NewInvalidArgument(`invalid pathspec ":(glob,literal)a": pathspec magic "glob" and "literal" are incompatible`),
		},
		{
			desc: "pathspec with unterminated magic",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":(icase"),
			},
			expectedErr: structerr.NewInvalidArgument(`invalid pathspec ":(icase": unterminated pathspec magic`),
		},
		{
			desc: "empty pathspec",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(""),
			},
			expectedErr: structerr.NewInvalidArgument(`invalid pathspec "": empty pathspec pattern`),
		},
		{
			desc: "pathspec without pattern",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":(exclude)"),
			},
			expectedErr: structerr.NewInvalidArgument(`invalid pathspec ":(exclude)": empty pathspec pattern`),
		},
		{
			desc: "too many pathspecs",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(slices.Repeat([]string{"a"}, searchFilesMaxPathspecs+1)...),
			},
			expectedErr: structerr.NewInvalidArgument("too many pathspecs: 33, at most 32 are allowed"),
		},
		{
			desc: "pathspec too long",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(strings.Repeat("a", searchFilesPathspecMaxLength+1)),
			},
			expectedErr: structerr.NewInvalidArgument(`invalid pathspec "%s": pathspec exceeds maximum length`, strings.Repeat("a", searchFilesPathspecMaxLength+1)),
		},
		{
			desc: "pathspec outside the repository",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs("../a"),
			},
			expectedErr: structerr.NewInvalidArgument(`invalid pathspec "../a": pathspec is outside the repository`),
		},
		{
			desc: "short pathspec with non-magic punctuation",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "needle",
				Ref:        []byte(pathsCommit),
				Pathspecs:  pathspecs(":*.go"),
			},
			expectedMatches: []string{
				needleMatch("main.go"),
			},
		},
		{
			desc: "query with leading dash",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "-dashed",
				Ref:        []byte(dashedCommit),
			},
			expectedMatches: []string{
				generateMatch(dashedCommit.String(), "-dashed", 1, 1, staticLine("-dashed")),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.SearchFilesByContent(ctx, tc.request)
			require.NoError(t, err)

			matches, err := consumeFilenameByContentChunked(stream)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedMatches, matches)
		})
	}
}

func TestSearchFilesByName(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "a", Mode: "100644", Content: "a"},
		{Path: "b", Mode: "100644", Content: "b"},
		{Path: "dir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{Path: "c.svg", Mode: "100644", Content: "c"},
			{Path: "d.txt", Mode: "100644", Content: "d"},
		})},
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"), gittest.WithTree(treeID))

	unusualNamesCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "\"file with quote.txt", Mode: "100644", Content: "something"},
		gittest.TreeEntry{Path: ".vimrc", Mode: "100644", Content: "something"},
		gittest.TreeEntry{Path: "cuộc đời là những chuyến đi.md", Mode: "100644", Content: "something"},
		gittest.TreeEntry{Path: "编码 'foo'.md", Mode: "100644", Content: "something"},
	))

	paginationCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file1.md", Mode: "100644", Content: "file1"},
		gittest.TreeEntry{Path: "file2.md", Mode: "100644", Content: "file2"},
		gittest.TreeEntry{Path: "file3.md", Mode: "100644", Content: "file3"},
		gittest.TreeEntry{Path: "file4.md", Mode: "100644", Content: "file4"},
		gittest.TreeEntry{Path: "file5.md", Mode: "100644", Content: "file5"},
		gittest.TreeEntry{Path: "new_file1.md", Mode: "100644", Content: "new_file1"},
		gittest.TreeEntry{Path: "new_file2.md", Mode: "100644", Content: "new_file2"},
		gittest.TreeEntry{Path: "new_file3.md", Mode: "100644", Content: "new_file3"},
	))

	dashedCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "-dashed", Mode: "100644", Content: "-dashed\n"},
	))

	for _, tc := range []struct {
		desc          string
		request       *gitalypb.SearchFilesByNameRequest
		expectedErr   error
		expectedFiles []string
	}{
		{
			desc: "repository not initialized",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: &gitalypb.Repository{},
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "empty request",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
			},
			expectedErr: structerr.NewInvalidArgument("no query given"),
		},
		{
			desc: "only query given",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Query:      "foo",
			},
			expectedErr: structerr.NewInvalidArgument("no ref given"),
		},
		{
			desc: "no repo",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: nil,
				Query:      "foo",
				Ref:        []byte("branch"),
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "invalid filter",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Query:      "foo",
				Ref:        []byte("branch"),
				Filter:     "+*.",
			},
			expectedErr: structerr.NewInvalidArgument("filter did not compile: error parsing regexp: missing argument to repetition operator: `+`"),
		},
		{
			desc: "filter longer than max",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Query:      "foo",
				Ref:        []byte("branch"),
				Filter:     strings.Repeat(".", searchFilesFilterMaxLength+1),
			},
			expectedErr: structerr.NewInvalidArgument("filter exceeds maximum length"),
		},
		{
			desc: "one file",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte("branch"),
				Query:      "dir/c.svg",
			},
			expectedFiles: []string{
				"dir/c.svg",
			},
		},
		{
			desc: "many files",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte("branch"),
				Query:      "dir",
			},
			expectedFiles: []string{
				"dir/c.svg",
				"dir/d.txt",
			},
		},
		{
			desc: "filtered",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte("branch"),
				Query:      "dir",
				Filter:     `\.svg$`,
			},
			expectedFiles: []string{
				"dir/c.svg",
			},
		},
		{
			desc: "non-existent ref",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Query:      ".",
				Ref:        []byte("non_existent_ref"),
			},
		},
		{
			desc: "file with quote",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      "\"file with quote.txt",
			},
			expectedFiles: []string{"\"file with quote.txt"},
		},
		{
			desc: "dotfiles",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      ".vimrc",
			},
			expectedFiles: []string{".vimrc"},
		},
		{
			desc: "latin-base language",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      "cuộc đời là những chuyến đi.md",
			},
			expectedFiles: []string{"cuộc đời là những chuyến đi.md"},
		},
		{
			desc: "non-latin language",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      "编码 'foo'.md",
			},
			expectedFiles: []string{"编码 'foo'.md"},
		},
		{
			desc: "filter file with quote",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      ".",
				Filter:     "^\"file.*",
			},
			expectedFiles: []string{"\"file with quote.txt"},
		},
		{
			desc: "filter dotfiles",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      ".",
				Filter:     "^\\..*",
			},
			expectedFiles: []string{".vimrc"},
		},
		{
			desc: "filter latin-base language",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      ".",
				Filter:     "cuộc đời .*\\.md",
			},
			expectedFiles: []string{"cuộc đời là những chuyến đi.md"},
		},
		{
			desc: "filter non-latin language",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      ".",
				Filter:     "编码 'foo'\\.(md|txt|rdoc)",
			},
			expectedFiles: []string{"编码 'foo'.md"},
		},
		{
			desc: "wildcard filter",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      ".",
				Filter:     ".*",
			},
			expectedFiles: []string{
				"\"file with quote.txt",
				".vimrc",
				"cuộc đời là những chuyến đi.md",
				"编码 'foo'.md",
			},
		},
		{
			desc: "only limit is set",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Limit:      3,
			},
			expectedFiles: []string{"file1.md", "file2.md", "file3.md"},
		},
		{
			desc: "only offset is set",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Offset:     2,
			},
			expectedFiles: []string{"file3.md", "file4.md", "file5.md", "new_file1.md", "new_file2.md", "new_file3.md"},
		},
		{
			desc: "both limit and offset are set",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Offset:     2,
				Limit:      2,
			},
			expectedFiles: []string{"file3.md", "file4.md"},
		},
		{
			desc: "offset exceeds the total files",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Offset:     8,
			},
			expectedFiles: nil,
		},
		{
			desc: "offset + limit exceeds the total files",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Offset:     6,
				Limit:      5,
			},
			expectedFiles: []string{"new_file2.md", "new_file3.md"},
		},
		{
			desc: "limit and offset combine with filter",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Filter:     "new.*",
				Offset:     1,
				Limit:      2,
			},
			expectedFiles: []string{"new_file2.md", "new_file3.md"},
		},
		{
			desc: "limit and offset combine with unmatched filter",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Filter:     "not_matched.*",
				Offset:     1,
				Limit:      2,
			},
			expectedFiles: nil,
		},
		{
			desc: "limit and offset combine with matched query",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      "new_file2.md",
				Offset:     0,
				Limit:      2,
			},
			expectedFiles: []string{"new_file2.md"},
		},
		{
			desc: "query with leading dash",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(dashedCommit),
				Query:      "-dashed",
			},
			expectedFiles: []string{"-dashed"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.SearchFilesByName(ctx, tc.request)
			require.NoError(t, err)

			files, err := consumeFilenameByName(stream)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedFiles, files)
		})
	}
}

func TestValidateSearchFilesPathspecs(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		pathspec    string
		expectedErr string
	}{
		{pathspec: "dir/file"},
		{pathspec: "*.go"},
		{pathspec: "编码.md"},
		{pathspec: ":(exclude)a"},
		{pathspec: ":(exclude,icase,glob)a"},
		{pathspec: ":(icase,literal)a"},
		{pathspec: ":!a"},
		{pathspec: ":^a"},
		{pathspec: ":!:a"},
		{pathspec: ":!:*.go"},
		{pathspec: "::a"},
		{pathspec: ":a"},
		{pathspec: ":(icase):a"},
		{pathspec: ":*.go"},
		{pathspec: ":.hidden"},
		{pathspec: ":!*.go"},
		{pathspec: "./a"},
		{pathspec: "dir/../a"},
		{pathspec: "..a"},
		{pathspec: ":_a", expectedErr: `invalid pathspec ":_a": unsupported pathspec magic "_"`},
		{pathspec: ":-a", expectedErr: `invalid pathspec ":-a": unsupported pathspec magic "-"`},
		{pathspec: ":!/a", expectedErr: `invalid pathspec ":!/a": unsupported pathspec magic "/"`},
		{pathspec: "/a", expectedErr: `invalid pathspec "/a": pathspec is outside the repository`},
		{pathspec: "..", expectedErr: `invalid pathspec "..": pathspec is outside the repository`},
		{pathspec: "dir/../../a", expectedErr: `invalid pathspec "dir/../../a": pathspec is outside the repository`},
		{pathspec: ":(exclude)../a", expectedErr: `invalid pathspec ":(exclude)../a": pathspec is outside the repository`},
		{pathspec: ":!:/a", expectedErr: `invalid pathspec ":!:/a": pathspec is outside the repository`},
		{pathspec: ":()a", expectedErr: `invalid pathspec ":()a": unsupported pathspec magic ""`},
		{pathspec: ":(icase,)a", expectedErr: `invalid pathspec ":(icase,)a": unsupported pathspec magic ""`},
		{pathspec: ":(ICASE)a", expectedErr: `invalid pathspec ":(ICASE)a": unsupported pathspec magic "ICASE"`},
		{pathspec: ":(prefix:1)a", expectedErr: `invalid pathspec ":(prefix:1)a": unsupported pathspec magic "prefix:1"`},
		{pathspec: ":", expectedErr: `invalid pathspec ":": empty pathspec pattern`},
		{pathspec: ":!", expectedErr: `invalid pathspec ":!": empty pathspec pattern`},
		{pathspec: "a\x00b", expectedErr: `invalid pathspec "a\x00b": pathspec contains a NUL byte`},
	} {
		t.Run(tc.pathspec, func(t *testing.T) {
			t.Parallel()

			err := validateSearchFilesPathspecs([][]byte{[]byte(tc.pathspec)})
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
		})
	}
}

func pathspecs(pathspecs ...string) [][]byte {
	result := make([][]byte, 0, len(pathspecs))
	for _, pathspec := range pathspecs {
		result = append(result, []byte(pathspec))
	}
	return result
}

func consumeFilenameByContentChunked(stream gitalypb.RepositoryService_SearchFilesByContentClient) ([]string, error) {
	var matches []string
	var match []byte

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		match = append(match, resp.GetMatchData()...)
		if resp.GetEndOfMatch() {
			matches = append(matches, string(match))
			match = nil
		}
	}

	return matches, nil
}

func consumeFilenameByName(stream gitalypb.RepositoryService_SearchFilesByNameClient) ([]string, error) {
	var filenames []string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		for _, file := range resp.GetFiles() {
			filenames = append(filenames, string(file))
		}
	}

	return filenames, nil
}
