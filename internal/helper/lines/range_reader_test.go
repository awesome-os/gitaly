package lines

import (
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

func TestRangeReader(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc       string
		input      string
		start, end uint64
		expected   string
	}{
		{
			desc:     "empty input",
			input:    "",
			start:    1,
			end:      1,
			expected: "",
		},
		{
			desc:     "first line",
			input:    "a\nb\nc\n",
			start:    1,
			end:      1,
			expected: "a\n",
		},
		{
			desc:     "middle lines",
			input:    "a\nb\nc\nd\n",
			start:    2,
			end:      3,
			expected: "b\nc\n",
		},
		{
			desc:     "last line without trailing newline",
			input:    "a\nb\nc",
			start:    3,
			end:      3,
			expected: "c",
		},
		{
			desc:     "end beyond last line",
			input:    "a\nb\nc\n",
			start:    2,
			end:      10,
			expected: "b\nc\n",
		},
		{
			desc:     "start beyond last line",
			input:    "a\nb\nc\n",
			start:    4,
			end:      10,
			expected: "",
		},
		{
			desc:     "start beyond last line without trailing newline",
			input:    "a\nb\nc",
			start:    4,
			end:      4,
			expected: "",
		},
		{
			desc:     "empty lines",
			input:    "\n\n\n\n",
			start:    2,
			end:      3,
			expected: "\n\n",
		},
		{
			desc:     "carriage returns are kept",
			input:    "a\r\nb\r\nc\r\n",
			start:    2,
			end:      2,
			expected: "b\r\n",
		},
		{
			desc:     "whole input",
			input:    "a\nb\nc",
			start:    1,
			end:      3,
			expected: "a\nb\nc",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			for _, reader := range []struct {
				desc string
				wrap func(io.Reader) io.Reader
			}{
				{desc: "full reads", wrap: func(r io.Reader) io.Reader { return r }},
				{desc: "one byte reads", wrap: iotest.OneByteReader},
				{desc: "half reads", wrap: iotest.HalfReader},
			} {
				t.Run(reader.desc, func(t *testing.T) {
					t.Parallel()

					data, err := io.ReadAll(NewRangeReader(reader.wrap(strings.NewReader(tc.input)), tc.start, tc.end))
					require.NoError(t, err)
					require.Equal(t, tc.expected, string(data))

					data, err = io.ReadAll(iotest.OneByteReader(NewRangeReader(reader.wrap(strings.NewReader(tc.input)), tc.start, tc.end)))
					require.NoError(t, err)
					require.Equal(t, tc.expected, string(data))
				})
			}
		})
	}
}

func TestRangeReader_stopsAfterEnd(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("a\nb\nc\nd\n")

	data, err := io.ReadAll(NewRangeReader(iotest.OneByteReader(input), 2, 2))
	require.NoError(t, err)
	require.Equal(t, "b\n", string(data))

	remaining, err := io.ReadAll(input)
	require.NoError(t, err)
	require.Equal(t, "c\nd\n", string(remaining))
}

type countingReader struct {
	io.Reader
	reads int
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.reads++
	return r.Reader.Read(p)
}

func TestRangeReader_smallReadsDoNotShrinkUnderlyingReads(t *testing.T) {
	t.Parallel()

	input := &countingReader{Reader: strings.NewReader(strings.Repeat("line\n", 10000) + "last\n")}

	data, err := io.ReadAll(io.LimitReader(NewRangeReader(input, 10001, 10001), 2))
	require.NoError(t, err)
	require.Equal(t, "la", string(data))
	require.Equal(t, 2, input.reads)
}

func TestRangeReader_propagatesErrors(t *testing.T) {
	t.Parallel()

	expectedErr := io.ErrUnexpectedEOF
	reader := io.MultiReader(strings.NewReader("a\nb"), iotest.ErrReader(expectedErr))

	data, err := io.ReadAll(NewRangeReader(reader, 2, 3))
	require.Equal(t, expectedErr, err)
	require.Equal(t, "b", string(data))
}

func TestValidateRange(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc        string
		start, end  uint64
		expectedErr error
	}{
		{
			desc:  "single line",
			start: 1,
			end:   1,
		},
		{
			desc:  "multiple lines",
			start: 2,
			end:   5,
		},
		{
			desc:        "start at zero",
			start:       0,
			end:         1,
			expectedErr: errors.New("line range start must be at least 1"),
		},
		{
			desc:        "end before start",
			start:       3,
			end:         2,
			expectedErr: errors.New("line range end must not be smaller than start"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.expectedErr, ValidateRange(tc.start, tc.end))
		})
	}
}
