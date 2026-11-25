package reftable

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testcfg"
)

func TestPutVarInt(t *testing.T) {
	t.Parallel()

	var table Table
	for _, value := range []uint64{0, 1, 127, 128, 255, 16511, 16512, 1 << 32, math.MaxUint64} {
		encoded := putVarInt(nil, value)
		// getVarInt's end bound is exclusive: the varint must lie within
		// [start, end), so the whole encoding is len(encoded) bytes.
		next, decoded, err := table.getVarInt(encoded, 0, uint(len(encoded)))
		require.NoError(t, err)
		require.Equal(t, uint(len(encoded)), next, "value %d", value)
		require.Equal(t, uint(value), decoded, "value %d", value)
	}
}

func TestWriteTableAt_matchesGit(t *testing.T) {
	t.Parallel()

	if !testhelper.IsReftableEnabled() {
		t.Skip("tests are reftable specific")
	}

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	tagID := gittest.WriteTag(t, cfg, repoPath, "v1", commitID.Revision(), gittest.WriteTagConfig{Message: "annotated"})
	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/doomed", commitID.String())

	// One transaction, so Git writes all of it into one table at a single
	// update index: direct refs, a peeled tag, a symbolic ref and a
	// deletion. More than restartInterval records, and ORIG_HEAD followed
	// by refs/ gives a record with no common prefix in the middle of an
	// interval.
	var stdin strings.Builder
	fmt.Fprintf(&stdin, "create ORIG_HEAD %s\n", commitID)
	for i := 0; i < 40; i++ {
		fmt.Fprintf(&stdin, "create refs/heads/branch-%02d %s\n", i, commitID)
	}
	fmt.Fprintf(&stdin, "create refs/tags/annotated %s\n", tagID)
	fmt.Fprintf(&stdin, "delete refs/heads/doomed\n")
	fmt.Fprintf(&stdin, "symref-create refs/heads/sym refs/heads/main\n")
	// Without auto-compaction the transaction stays in a table of its own.
	gittest.ExecOpts(t, cfg, gittest.ExecConfig{
		Stdin: strings.NewReader(stdin.String()),
		Env:   []string{"GIT_TEST_REFTABLE_AUTOCOMPACTION=false"},
	}, "-C", repoPath, "-c", "core.logAllRefUpdates=false", "update-ref", "--stdin")

	tables, err := ReadTablesList(repoPath)
	require.NoError(t, err)
	gitTablePath := filepath.Join(repoPath, "reftable", tables[len(tables)-1].String())

	gitTable, err := ParseTable(gitTablePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, gitTable)
	require.Equal(t, gitTable.MinUpdateIndex(), gitTable.MaxUpdateIndex())

	records, err := gitTable.GetRefRecords()
	require.NoError(t, err)
	require.Len(t, records, 44)
	require.Contains(t, records, RefRecord{Name: "refs/tags/annotated", Target: tagID.String(), Peeled: commitID.String()})
	require.Contains(t, records, RefRecord{Name: "refs/heads/sym", Target: "refs/heads/main", IsSymbolic: true})
	require.Contains(t, records, RefRecord{Name: "refs/heads/doomed", IsDeletion: true})

	ourTablePath := filepath.Join(testhelper.TempDir(t), "ours.ref")
	require.NoError(t, WriteTableAt(ourTablePath, gitTable.MinUpdateIndex(), gittest.DefaultObjectHash, records))

	expected := testhelper.MustReadFile(t, gitTablePath)
	actual := testhelper.MustReadFile(t, ourTablePath)
	require.Equal(t, expected, actual, "the table must be laid out byte for byte as Git writes it")
}

func TestWriteTableAt_gitReadsLargeTable(t *testing.T) {
	t.Parallel()

	if !testhelper.IsReftableEnabled() {
		t.Skip("tests are reftable specific")
	}

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	tagID := gittest.WriteTag(t, cfg, repoPath, "v1", commitID.Revision(), gittest.WriteTagConfig{Message: "annotated"})

	records := []RefRecord{
		{Name: "HEAD", Target: "refs/heads/main", IsSymbolic: true},
	}
	for i := 0; i < 3000; i++ {
		records = append(records, RefRecord{Name: fmt.Sprintf("refs/heads/branch-%04d", i), Target: commitID.String()})
	}
	records = append(records,
		RefRecord{Name: "refs/heads/main", Target: commitID.String()},
		RefRecord{Name: "refs/heads/zz-deleted", IsDeletion: true},
		RefRecord{Name: "refs/tags/v1", Target: tagID.String(), Peeled: commitID.String()},
	)

	// Replace the repository's stack with our table alone.
	name := Name{MinUpdateIndex: 1, MaxUpdateIndex: 1, Suffix: "a1b2c3d4"}
	tablePath := filepath.Join(repoPath, "reftable", name.String())
	require.NoError(t, WriteTableAt(tablePath, 1, gittest.DefaultObjectHash, records))
	require.NoError(t, os.WriteFile(filepath.Join(repoPath, "reftable", "tables.list"), []byte(name.String()+"\n"), 0o644))

	table, err := ParseTable(tablePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, table)
	require.Greater(t, table.blockSize, uint(defaultBlockSize), "expected one block larger than Git's")

	readBack, err := ReadRefRecordsAt(tablePath)
	require.NoError(t, err)
	require.Equal(t, records, readBack)

	refs := strings.Split(strings.TrimSpace(string(gittest.Exec(t, cfg, "-C", repoPath,
		"for-each-ref", "--format=%(refname) %(objectname)"))), "\n")
	require.Len(t, refs, 3002, "branches, main and v1; not the deleted ref")
	require.Contains(t, refs, "refs/heads/branch-2999 "+commitID.String())
	require.Contains(t, refs, "refs/tags/v1 "+tagID.String())

	require.Equal(t, "refs/heads/main", text(gittest.Exec(t, cfg, "-C", repoPath, "symbolic-ref", "HEAD")))
	require.Equal(t,
		tagID.String()+" refs/tags/v1\n"+commitID.String()+" refs/tags/v1^{}",
		text(gittest.Exec(t, cfg, "-C", repoPath, "show-ref", "--dereference", "refs/tags/v1")))
}

func TestWriteTableAt_gitCompactsOverTable(t *testing.T) {
	t.Parallel()

	if !testhelper.IsReftableEnabled() {
		t.Skip("tests are reftable specific")
	}

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	tagID := gittest.WriteTag(t, cfg, repoPath, "v1", commitID.Revision(), gittest.WriteTagConfig{Message: "annotated"})

	// Our table seeds the stack with a symref, direct refs, a peeled tag and a
	// deletion tombstone.
	records := []RefRecord{
		{Name: "HEAD", Target: "refs/heads/main", IsSymbolic: true},
		{Name: "refs/heads/main", Target: commitID.String()},
		{Name: "refs/heads/seed", Target: commitID.String()},
		{Name: "refs/heads/zz-deleted", IsDeletion: true},
		{Name: "refs/tags/v1", Target: tagID.String(), Peeled: commitID.String()},
	}
	name := Name{MinUpdateIndex: 1, MaxUpdateIndex: 1, Suffix: "a1b2c3d4"}
	tablePath := filepath.Join(repoPath, "reftable", name.String())
	require.NoError(t, WriteTableAt(tablePath, 1, gittest.DefaultObjectHash, records))
	require.NoError(t, os.WriteFile(filepath.Join(repoPath, "reftable", "tables.list"), []byte(name.String()+"\n"), 0o644))

	// Let Git write many more references over our table with auto-compaction
	// on, so it has to merge our table with the ones it writes.
	for i := 0; i < 10; i++ {
		gittest.Exec(t, cfg, "-C", repoPath, "update-ref",
			fmt.Sprintf("refs/heads/added-%02d", i), commitID.String())
	}

	tables, err := ReadTablesList(repoPath)
	require.NoError(t, err)
	require.Less(t, len(tables), 11, "Git should have compacted tables rather than keeping one per update")

	// Every value our table seeded must survive compaction alongside Git's.
	refs := strings.Split(strings.TrimSpace(string(gittest.Exec(t, cfg, "-C", repoPath,
		"for-each-ref", "--format=%(refname) %(objectname)"))), "\n")
	require.Contains(t, refs, "refs/heads/seed "+commitID.String())
	require.Contains(t, refs, "refs/heads/added-09 "+commitID.String())
	require.Contains(t, refs, "refs/tags/v1 "+tagID.String())
	require.NotContains(t, refs, "refs/heads/zz-deleted "+commitID.String())

	require.Equal(t, "refs/heads/main", text(gittest.Exec(t, cfg, "-C", repoPath, "symbolic-ref", "HEAD")))
	require.Equal(t,
		tagID.String()+" refs/tags/v1\n"+commitID.String()+" refs/tags/v1^{}",
		text(gittest.Exec(t, cfg, "-C", repoPath, "show-ref", "--dereference", "refs/tags/v1")))
}

func TestWriteTableAt_deletionThroughReaders(t *testing.T) {
	t.Parallel()

	oid := gittest.DefaultObjectHash.EmptyTreeOID.String()
	zero := gittest.DefaultObjectHash.ZeroOID.String()
	records := []RefRecord{
		{Name: "refs/heads/kept", Target: oid},
		{Name: "refs/heads/zz-doomed", IsDeletion: true},
	}
	tablePath := filepath.Join(testhelper.TempDir(t), "table.ref")
	require.NoError(t, WriteTableAt(tablePath, 1, gittest.DefaultObjectHash, records))

	// RefRecord keeps a tombstone's Target empty.
	readBack, err := ReadRefRecordsAt(tablePath)
	require.NoError(t, err)
	require.Equal(t, records, readBack)

	// The refname->target and git.Reference views both map a deletion to the
	// zero OID, which ReadRefsAt and the MVCC backend rely on.
	refs, err := ReadRefsAt(tablePath)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"refs/heads/kept":      oid,
		"refs/heads/zz-doomed": zero,
	}, refs)

	table, err := ParseTable(tablePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, table)

	references, err := table.GetReferences()
	require.NoError(t, err)
	require.Contains(t, references, git.Reference{Name: git.ReferenceName("refs/heads/zz-doomed"), Target: zero})
}

func TestWriteTableAt_invalidRecords(t *testing.T) {
	t.Parallel()

	oid := gittest.DefaultObjectHash.EmptyTreeOID.String()
	for _, tc := range []struct {
		desc        string
		records     []RefRecord
		expectedErr string
	}{
		{desc: "no records", expectedErr: "no records to write"},
		{
			desc:        "unsorted",
			records:     []RefRecord{{Name: "refs/heads/b", Target: oid}, {Name: "refs/heads/a", Target: oid}},
			expectedErr: "records not sorted",
		},
		{
			desc:        "duplicate",
			records:     []RefRecord{{Name: "refs/heads/a", Target: oid}, {Name: "refs/heads/a", Target: oid}},
			expectedErr: "records not sorted",
		},
		{
			desc:        "short object ID",
			records:     []RefRecord{{Name: "refs/heads/a", Target: "abcd"}},
			expectedErr: "has 2 bytes",
		},
		{
			desc:        "symbolic ref without target",
			records:     []RefRecord{{Name: "HEAD", IsSymbolic: true}},
			expectedErr: "symbolic ref without a target",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(testhelper.TempDir(t), "table.ref")
			require.ErrorContains(t, WriteTableAt(path, 1, gittest.DefaultObjectHash, tc.records), tc.expectedErr)
			require.NoFileExists(t, path)
		})
	}
}

func text(output []byte) string {
	return strings.TrimSpace(string(output))
}
