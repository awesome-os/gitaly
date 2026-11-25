package reftable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
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

func getReftables(repoPath string) []string {
	tables := []string{}

	reftablePath := filepath.Join(repoPath, "reftable")

	files, err := os.ReadDir(reftablePath)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if filepath.Base(file.Name()) == "tables.list" {
			continue
		}

		tables = append(tables, filepath.Join(reftablePath, file.Name()))
	}

	return tables
}

func TestParseTable(t *testing.T) {
	t.Parallel()

	if !testhelper.IsReftableEnabled() {
		t.Skip("tests are reftable specific")
	}

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	type setupData struct {
		repoPath   string
		references []git.Reference
		// multipleBlocks asserts that the table spans more than a single
		// block, so the test keeps covering the multi-block code path even
		// if the format's compression gets better.
		multipleBlocks bool
	}

	for _, tc := range []struct {
		name        string
		setup       func() setupData
		expectedErr error
	}{
		{
			name: "single ref",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

				return setupData{
					repoPath: repoPath,
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
					},
				}
			},
		},
		{
			name: "single ref + annotated tag",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				annotatedTag := gittest.WriteTag(t, cfg, repoPath, "v2.0.0", mainCommit.Revision(), gittest.WriteTagConfig{
					Message: "annotated tag",
				})

				return setupData{
					repoPath: repoPath,
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
						{
							Name:   "refs/tags/v2.0.0",
							Target: annotatedTag.String(),
						},
					},
				}
			},
		},
		{
			name: "two refs without prefix compression",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				rootRefCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("ROOTREF"))

				return setupData{
					repoPath: repoPath,
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "ROOTREF",
							Target: rootRefCommit.String(),
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
					},
				}
			},
		},
		{
			name: "two refs with prefix compression",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				masterCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

				return setupData{
					repoPath: repoPath,
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
						{
							Name:   "refs/heads/master",
							Target: masterCommit.String(),
						},
					},
				}
			},
		},
		{
			name: "multiple refs with different commit IDs",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				masterCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(mainCommit), gittest.WithBranch("master"))

				return setupData{
					repoPath: repoPath,
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
						{
							Name:   "refs/heads/master",
							Target: masterCommit.String(),
						},
					},
				}
			},
		},
		{
			name: "many prefix compressed references",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				var references []git.Reference

				references = append(references, git.Reference{
					Name:       "HEAD",
					Target:     "refs/heads/main",
					IsSymbolic: true,
				})

				for i := 0; i < 100; i++ {
					branch := fmt.Sprintf("branch%02d", i)
					references = append(references, git.Reference{
						Name:   git.ReferenceName(fmt.Sprintf("refs/heads/%s", branch)),
						Target: gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branch)).String(),
					})
				}

				return setupData{
					repoPath:   repoPath,
					references: references,
				}
			},
		},
		{
			name: "multiple blocks in table",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				commit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

				references := []git.Reference{
					{
						Name:       "HEAD",
						Target:     "refs/heads/main",
						IsSymbolic: true,
					},
					{
						Name:   "refs/heads/main",
						Target: commit.String(),
					},
				}

				// The reftable format prefix compresses reference names, so short
				// and similar names pack into a single block. Use long names with
				// an early mismatch to make each record big enough that the
				// references are spread over multiple blocks.
				var stdin bytes.Buffer
				for i := 0; i < 500; i++ {
					name := git.ReferenceName(fmt.Sprintf("refs/heads/zz%04d-%s", i, strings.Repeat("x", 40)))
					fmt.Fprintf(&stdin, "create %s %s\n", name, commit)

					references = append(references, git.Reference{
						Name:   name,
						Target: commit.String(),
					})
				}

				gittest.ExecOpts(t, cfg, gittest.ExecConfig{Stdin: &stdin},
					"-C", repoPath, "update-ref", "--stdin")

				return setupData{
					repoPath:       repoPath,
					references:     references,
					multipleBlocks: true,
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup()

			repoPath := setup.repoPath

			// pack-refs so there is only one table
			gittest.Exec(t, cfg, "-C", repoPath, "pack-refs")
			reftablePath := getReftables(repoPath)[0]

			table, err := ParseTable(reftablePath)
			require.NoError(t, err)
			defer testhelper.MustClose(t, table)

			if setup.multipleBlocks {
				require.Greater(t, table.footerOffset, table.blockSize,
					"expected the table to span multiple blocks")
			}

			references, err := table.GetReferences()
			require.NoError(t, err)

			require.Equal(t, setup.references, references)
		})
	}
}

func TestParseTable_validation(t *testing.T) {
	if !testhelper.IsReftableEnabled() {
		t.Skip("This test is reftable specific.")
	}

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	patchHeader := func(t *testing.T, f *os.File, hdr header) {
		buf := bytes.NewBuffer(nil)
		require.NoError(t, binary.Write(buf, binary.BigEndian, hdr.headerV1))

		if hdr.headerV1.Version >= 2 {
			require.NoError(t, binary.Write(buf, binary.BigEndian, hdr.HashID))
		}

		_, err := f.WriteAt(buf.Bytes(), 0)
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		desc                 string
		patchTable           func(*testing.T, *os.File, footer)
		expectedErrorMessage string
	}{
		{
			desc: "unexpected magic",
			patchTable: func(t *testing.T, file *os.File, f footer) {
				f.header.Magic = [...]byte{'I', 'V', 'A', 'L'}
				patchHeader(t, file, f.header)
			},
			expectedErrorMessage: `parse header: unexpected magic bytes: "IVAL"`,
		},
		{
			desc: "unsupported version",
			patchTable: func(t *testing.T, file *os.File, f footer) {
				f.header.Version = 3
				patchHeader(t, file, f.header)
			},
			expectedErrorMessage: `parse header: unsupported version: 3`,
		},
		{
			desc: "unsupported hash",
			patchTable: func(t *testing.T, file *os.File, f footer) {
				if f.Version < 2 {
					t.Skip("Hash ID is only present on reftable version 2.")
				}

				f.header.HashID = [...]byte{'I', 'V', 'A', 'L'}
				patchHeader(t, file, f.header)
			},
			expectedErrorMessage: `parse header: unsupported hash id: "IVAL"`,
		},
		{
			desc: "mismatching header and footer",
			patchTable: func(t *testing.T, file *os.File, f footer) {
				f.header.MaxUpdateIndex++
				patchHeader(t, file, f.header)
			},
			expectedErrorMessage: `footer doesn't match header`,
		},
		{
			desc: "invalid checksum",
			patchTable: func(t *testing.T, file *os.File, f footer) {
				// The checksum is at the end of the file. Modify the preceding byte without updating the
				// checksum to trigger a checksumming failure.
				info, err := file.Stat()
				require.NoError(t, err)

				_, err = file.WriteAt([]byte{255}, info.Size()-crc32.Size-1)
				require.NoError(t, err)
			},
			expectedErrorMessage: "parse footer: checksum mismatch",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})

			tables, err := ReadTablesList(repoPath)
			require.NoError(t, err)

			tablePath := filepath.Join(repoPath, "reftable", tables[0].String())
			table, err := ParseTable(tablePath)
			require.NoError(t, err)
			defer testhelper.MustClose(t, table)

			file, err := os.OpenFile(tablePath, os.O_RDWR, 0)
			require.NoError(t, err)
			defer testhelper.MustClose(t, file)

			tc.patchTable(t, file, table.footer)

			table, err = ParseTable(tablePath)
			require.EqualError(t, err, tc.expectedErrorMessage)
			require.Nil(t, table)
		})
	}
}

func TestTable_GetReferences_corrupt(t *testing.T) {
	if !testhelper.IsReftableEnabled() {
		t.Skip("This test is reftable specific.")
	}

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	// The table Git writes for the repository below holds a single ref block
	// right after the file header: HEAD as a symref record, followed by
	// refs/heads/main as a regular record. The cases corrupt bytes inside that
	// block, which ParseTable does not checksum, so only GetReferences sees
	// them.
	blockLen := func(data []byte, headerSize int) int {
		return int(parseUint24([3]byte(data[headerSize+1 : headerSize+4])))
	}

	for _, tc := range []struct {
		desc                 string
		corrupt              func(data []byte, headerSize, footerOffset int) []byte
		expectedErrorMessage string
	}{
		{
			desc: "varint runs into the restart table",
			corrupt: func(data []byte, headerSize, _ int) []byte {
				end := blockLen(data, headerSize)
				restartCount := int(binary.BigEndian.Uint16(data[end-2:]))
				for i := headerSize + 4; i < end-2-3*restartCount; i++ {
					data[i] = 0xff
				}
				return data
			},
			expectedErrorMessage: "parsing block: getting prefix length: exceeded block length",
		},
		{
			desc: "prefix length exceeds previous refname",
			corrupt: func(data []byte, headerSize, _ int) []byte {
				data[headerSize+4] = 0x05
				return data
			},
			expectedErrorMessage: "parsing block: prefix length 5 exceeds previous refname",
		},
		{
			desc: "suffix length exceeds block",
			corrupt: func(data []byte, headerSize, _ int) []byte {
				copy(data[headerSize+5:], []byte{0xff, 0x7f})
				return data
			},
			expectedErrorMessage: "parsing block: suffix length 2063 exceeded block length",
		},
		{
			desc: "symref size exceeds block",
			corrupt: func(data []byte, headerSize, _ int) []byte {
				// prefix length, suffix length, "HEAD", update index delta.
				data[headerSize+4+1+1+len("HEAD")+1] = 0x7f
				return data
			},
			expectedErrorMessage: "parsing block: symref size 127 exceeded block length",
		},
		{
			desc: "object ID exceeds block",
			corrupt: func(data []byte, headerSize, _ int) []byte {
				// Grow the restart table so that it begins inside the object ID
				// of refs/heads/main.
				binary.BigEndian.PutUint16(data[blockLen(data, headerSize)-2:], 4)
				return data
			},
			expectedErrorMessage: "parsing block: object ID exceeded block length",
		},
		{
			desc: "restart count exceeds block",
			corrupt: func(data []byte, headerSize, _ int) []byte {
				binary.BigEndian.PutUint16(data[blockLen(data, headerSize)-2:], 0xffff)
				return data
			},
			expectedErrorMessage: "parsing block: restart count 65535 exceeds block length",
		},
		{
			desc: "block length too short",
			corrupt: func(data []byte, headerSize, _ int) []byte {
				// Too short to hold the block header and the restart count.
				copy(data[headerSize+1:], []byte{0, 0, 5})
				return data
			},
			expectedErrorMessage: "parsing block: block length 5 out of bounds",
		},
		{
			desc: "block length exceeds table",
			corrupt: func(data []byte, headerSize, _ int) []byte {
				copy(data[headerSize+1:], []byte{0xff, 0xff, 0xff})
				return data
			},
			expectedErrorMessage: "parsing block: block length 16777215 out of bounds",
		},
		{
			desc: "truncated ref block",
			corrupt: func(data []byte, headerSize, footerOffset int) []byte {
				// Drop the restart table but keep the footer intact, so the
				// block length points past the end of the ref block.
				return append(data[:footerOffset-10:footerOffset-10], data[footerOffset:]...)
			},
			expectedErrorMessage: gittest.ObjectHashDependent(t, map[string]string{
				git.ObjectHashSHA1.Format:   "parsing block: block length 97 out of bounds",
				git.ObjectHashSHA256.Format: "parsing block: block length 113 out of bounds",
			}),
		},
		{
			desc: "unknown reference value type",
			corrupt: func(data []byte, headerSize, _ int) []byte {
				// The first record is HEAD, a symref (value type 3). Set the
				// value-type bits -- the low 3 of the suffix-length byte -- to
				// 7, which the format does not define, while leaving the suffix
				// length intact so parsing reaches the value-type switch.
				data[headerSize+5] |= 0x07
				return data
			},
			expectedErrorMessage: "parsing block: unknown reference value type 7",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
			gittest.Exec(t, cfg, "-C", repoPath, "pack-refs")

			original, err := ParseTable(getReftables(repoPath)[0])
			require.NoError(t, err)
			defer testhelper.MustClose(t, original)

			data, err := os.ReadFile(original.absolutePath)
			require.NoError(t, err)

			tablePath := filepath.Join(t.TempDir(), "corrupt.ref")
			require.NoError(t, os.WriteFile(tablePath,
				tc.corrupt(data, original.footer.Version.HeaderSize(), int(original.footerOffset)), 0o644))

			table, err := ParseTable(tablePath)
			require.NoError(t, err)
			defer testhelper.MustClose(t, table)

			references, err := table.GetReferences()
			require.EqualError(t, err, tc.expectedErrorMessage)
			require.Nil(t, references)
		})
	}
}

func TestTable_updateIndex(t *testing.T) {
	if !testhelper.IsReftableEnabled() {
		t.Skip("This test is reftable specific.")
	}

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	// Create a new branch. This creates a table that contains the original
	// HEAD at update index 1 and the new branch at updated index 2.
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch-1"))

	tables, err := ReadTablesList(repoPath)
	require.NoError(t, err)

	table, err := ParseTable(filepath.Join(repoPath, "reftable", tables[0].String()))
	require.NoError(t, err)
	defer testhelper.MustClose(t, table)

	require.Equal(t, uint64(1), table.MinUpdateIndex())
	require.Equal(t, uint64(2), table.MaxUpdateIndex())
}

func TestParseName(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc             string
		reftableName     string
		expectedName     Name
		expectedErrorStr string
	}{
		{
			desc:             "invalid table name",
			reftableName:     "spy-from-files-backend.ref",
			expectedErrorStr: "reftable name \"spy-from-files-backend.ref\" malformed",
		},
		{
			desc:             "non hex values in min index",
			reftableName:     "0xzz0000000001-0x00000000000a-b54f3b59.ref",
			expectedErrorStr: "reftable name \"0xzz0000000001-0x00000000000a-b54f3b59.ref\" malformed",
		},
		{
			desc:             "non hex values in max index",
			reftableName:     "0x000000000001-0x0000000000za-b54f3b59.ref",
			expectedErrorStr: "reftable name \"0x000000000001-0x0000000000za-b54f3b59.ref\" malformed",
		},
		{
			desc:         "small values in table name",
			reftableName: "0x000000000001-0x000000000005-b54f3b59.ref",
			expectedName: Name{
				MinUpdateIndex: 1,
				MaxUpdateIndex: 5,
				Suffix:         "b54f3b59",
			},
		},
		{
			desc:         "medium values in table name",
			reftableName: "0x0000000000af-0x0000000000ee-ab413b60.ref",
			expectedName: Name{
				MinUpdateIndex: 175,
				MaxUpdateIndex: 238,
				Suffix:         "ab413b60",
			},
		},
		{
			desc:         "upper case hex in table name",
			reftableName: "0x0000000000DE-0x0000000000AD-c23a3459.ref",
			expectedName: Name{
				MinUpdateIndex: 222,
				MaxUpdateIndex: 173,
				Suffix:         "c23a3459",
			},
		},
		{
			desc:         "max values in table name",
			reftableName: "0xeeeeeeeeeeee-0xeeeeeeeeeeee-ddff2b19.ref",
			expectedName: Name{
				MinUpdateIndex: 262709978263278,
				MaxUpdateIndex: 262709978263278,
				Suffix:         "ddff2b19",
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			actualName, err := ParseName(tc.reftableName)
			if tc.expectedErrorStr != "" {
				require.EqualError(t, err, tc.expectedErrorStr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedName, actualName)
		})
	}
}

func TestName_string(t *testing.T) {
	require.Equal(t,
		"0x000000000001-0x0000000005f6-b54f3b59.ref",
		Name{
			MinUpdateIndex: 1,
			MaxUpdateIndex: 1526,
			Suffix:         "b54f3b59",
		}.String(),
	)
}

func TestTable_PatchUpdateIndexes(t *testing.T) {
	if !testhelper.IsReftableEnabled() {
		t.Skipf("This test is specific to reftables.")
	}

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	tables, err := ReadTablesList(repoPath)
	require.NoError(t, err)
	// In a new repository, there should only be a single table that contains HEAD.
	require.Len(t, tables, 1)

	tablePath := filepath.Join(repoPath, "reftable", tables[0].String())

	originalData, err := os.ReadFile(tablePath)
	require.NoError(t, err)

	// First check the table as Git wrote it matches our expectations.
	originalTable, err := ParseTable(tablePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, originalTable)

	expectedVersion := gittest.ObjectHashDependent(t, map[string]version{
		git.ObjectHashSHA1.Format:   1,
		git.ObjectHashSHA256.Format: 2,
	})

	require.Equal(t, footer{
		header: header{
			headerV1: headerV1{
				Magic:          magic,
				Version:        expectedVersion,
				BlockSize:      [...]byte{0, 16, 0},
				MinUpdateIndex: 1,
				MaxUpdateIndex: 1,
			},
			HashID: testhelper.DependingOn(expectedVersion == 2,
				gittest.ObjectHashDependent(t, map[string][4]byte{
					git.ObjectHashSHA1.Format:   hashIDSHA1,
					git.ObjectHashSHA256.Format: hashIDSHA256,
				}),
				// Version 1 doesn't have a hash ID field.
				[4]byte{},
			),
		},
		footerEnd: footerEnd{
			CRC32: gittest.ObjectHashDependent(t, map[string]uint32{
				git.ObjectHashSHA1.Format:   3066034058,
				git.ObjectHashSHA256.Format: 1113112077,
			}),
		},
	}, originalTable.footer)

	originalFooter := originalTable.footer

	// Patch the table and assert that all of the relevant fields in both Table instance
	// and the file have been updated.
	require.NoError(t, originalTable.PatchUpdateIndexes(2, 3))

	patchedData, err := os.ReadFile(tablePath)
	require.NoError(t, err)
	require.NotEqual(t, patchedData, originalData)

	patchedTable, err := ParseTable(tablePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, patchedTable)

	expectedPatchedFooter := originalFooter
	expectedPatchedFooter.MinUpdateIndex = 2
	expectedPatchedFooter.MaxUpdateIndex = 3
	expectedPatchedFooter.CRC32 = gittest.ObjectHashDependent(t, map[string]uint32{
		git.ObjectHashSHA1.Format:   4151879677,
		git.ObjectHashSHA256.Format: 1567373335,
	})

	require.Equal(t, expectedPatchedFooter, patchedTable.footer)
	require.Equal(t, originalTable.footer, patchedTable.footer,
		"expected the internal fields on the patched table to be updated along the file",
	)

	// Finally, check that we arrive back at the original if we patch the file with
	// the same update indexes again.
	require.NoError(t, patchedTable.PatchUpdateIndexes(1, 1))
	roundtrippedData, err := os.ReadFile(tablePath)
	require.NoError(t, err)
	require.Equal(t, originalData, roundtrippedData)
}

// TestReftableHelpersAt covers the three exported, path-based helpers
// (ReadUpdateRangeAt, PatchUpdateRangeAt, ReadRefsAt) directly against a real
// reftable, rather than only through the mvcc package's fakes. They are the
// callbacks the MVCC three-way merge injects, so their happy path, their
// error-wrapping, and the range-patch round-trip are worth pinning down here.
func TestReftableHelpersAt(t *testing.T) {
	t.Parallel()

	if !testhelper.IsReftableEnabled() {
		t.Skip("tests are reftable specific")
	}

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

	tables, err := ReadTablesList(repoPath)
	require.NoError(t, err)
	require.Len(t, tables, 1, "a fresh repository has a single reftable")
	tablePath := filepath.Join(repoPath, "reftable", tables[0].String())

	t.Run("ReadRefsAt returns refname -> target", func(t *testing.T) {
		refs, err := ReadRefsAt(tablePath)
		require.NoError(t, err)
		require.Equal(t, map[string]string{
			"HEAD":            "refs/heads/main",
			"refs/heads/main": mainCommit.String(),
		}, refs)
	})

	t.Run("ReadUpdateRangeAt returns a sane range", func(t *testing.T) {
		// The exact indices depend on how many ref transactions Git wrote
		// setting the repository up, so assert the range is well-formed
		// rather than hard-coding a value. The deterministic round-trip is
		// covered by the PatchUpdateRangeAt subtest below.
		minIdx, maxIdx, err := ReadUpdateRangeAt(tablePath)
		require.NoError(t, err)
		require.NotZero(t, minIdx, "update indexes are 1-based")
		require.GreaterOrEqual(t, maxIdx, minIdx, "max must not be below min")
	})

	t.Run("PatchUpdateRangeAt rewrites the range, leaving refs intact", func(t *testing.T) {
		// Patch a copy so the repository's own table stays pristine for the
		// other subtests; PatchUpdateRangeAt edits in place.
		copyPath := filepath.Join(t.TempDir(), "patched.ref")
		data, err := os.ReadFile(tablePath)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(copyPath, data, 0o644))

		require.NoError(t, PatchUpdateRangeAt(copyPath, 5, 7))

		minIdx, maxIdx, err := ReadUpdateRangeAt(copyPath)
		require.NoError(t, err)
		require.Equal(t, uint64(5), minIdx, "patched min is read back")
		require.Equal(t, uint64(7), maxIdx, "patched max is read back")

		refs, err := ReadRefsAt(copyPath)
		require.NoError(t, err)
		require.Equal(t, mainCommit.String(), refs["refs/heads/main"],
			"a range patch does not touch the refs")
	})

	t.Run("a missing file is an error, not a panic", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "does-not-exist.ref")

		_, _, err := ReadUpdateRangeAt(missing)
		require.Error(t, err)

		_, err = ReadRefsAt(missing)
		require.Error(t, err)

		require.Error(t, PatchUpdateRangeAt(missing, 1, 1))
	})
}
