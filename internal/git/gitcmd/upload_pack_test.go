package gitcmd

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsUploadPackMissingObject(t *testing.T) {
	t.Parallel()

	const notOurRef = "fatal: git upload-pack: not our ref 1111111111111111111111111111111111111111\n"

	for _, tc := range []struct {
		desc     string
		filter   string
		stderr   string
		expected bool
	}{
		{
			desc:     "lazy fetch of missing object",
			filter:   "blob:none",
			stderr:   notOurRef,
			expected: true,
		},
		{
			desc:     "lazy fetch of missing SHA-256 object",
			filter:   "blob:none",
			stderr:   "fatal: git upload-pack: not our ref " + strings.Repeat("1", 64) + "\n",
			expected: true,
		},
		{
			desc:   "missing object without filter",
			stderr: notOurRef,
		},
		{
			desc:   "missing object with other filter",
			filter: "blob:limit=200",
			stderr: notOurRef,
		},
		{
			desc:   "unreachable object",
			filter: "blob:none",
			stderr: "error: git upload-pack: not our ref 1111111111111111111111111111111111111111\n",
		},
		{
			desc:   "broken alternates",
			filter: "blob:none",
			stderr: "error: unable to normalize alternate object path: /pool/objects\n" + notOurRef,
		},
		{
			desc:   "corrupt object",
			filter: "blob:none",
			stderr: "error: inflate: data stream error (incorrect header check)\n" +
				"error: unable to unpack 1111111111111111111111111111111111111111 header\n" + notOurRef,
		},
		{
			desc:   "missing object ID",
			filter: "blob:none",
			stderr: "fatal: git upload-pack: not our ref \n",
		},
		{
			desc:   "short object ID",
			filter: "blob:none",
			stderr: "fatal: git upload-pack: not our ref 1111111\n",
		},
		{
			desc:   "non-hex object ID",
			filter: "blob:none",
			stderr: "fatal: git upload-pack: not our ref " + strings.Repeat("z", 40) + "\n",
		},
		{
			desc:   "missing trailing newline",
			filter: "blob:none",
			stderr: "fatal: git upload-pack: not our ref 1111111111111111111111111111111111111111",
		},
		{
			desc:   "trailing output",
			filter: "blob:none",
			stderr: notOurRef + "error: something else\n",
		},
		{
			desc:   "empty stderr",
			filter: "blob:none",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.expected, IsUploadPackMissingObject(tc.filter, tc.stderr))
		})
	}
}
