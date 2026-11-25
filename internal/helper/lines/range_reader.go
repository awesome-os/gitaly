package lines

import (
	"bytes"
	"errors"
	"io"
)

// rangeReaderBufferSize is the size of the chunks read from the underlying reader. It's independent
// of the size of the buffers passed to Read so that skipping lines stays cheap even when callers
// read in small chunks, for example because the output is limited by an io.LimitReader.
const rangeReaderBufferSize = 32 * 1024

// rangeReader is an io.Reader that only yields a range of lines of the underlying reader.
type rangeReader struct {
	r          io.Reader
	start, end uint64
	// line is the 1-based number of the line that the next byte read from r belongs to.
	line    uint64
	buf     []byte
	pending []byte
	err     error
}

// NewRangeReader returns a reader that yields lines start through end of r, both 1-based and
// inclusive. Lines are terminated by '\n', which is kept in the returned data. The reader stops
// reading from r as soon as the last requested line has been read. If r has fewer than start lines,
// no data is returned.
func NewRangeReader(r io.Reader, start, end uint64) io.Reader {
	return &rangeReader{
		r:     r,
		start: start,
		end:   end,
		line:  1,
		buf:   make([]byte, rangeReaderBufferSize),
	}
}

// ValidateRange checks that start and end describe a valid range of lines for NewRangeReader.
func ValidateRange(start, end uint64) error {
	if start == 0 {
		return errors.New("line range start must be at least 1")
	}
	if end < start {
		return errors.New("line range end must not be smaller than start")
	}

	return nil
}

func (rr *rangeReader) Read(p []byte) (int, error) {
	for len(rr.pending) == 0 {
		if rr.err != nil {
			return 0, rr.err
		}
		if rr.line > rr.end {
			return 0, io.EOF
		}

		rr.fill()
	}

	n := copy(p, rr.pending)
	rr.pending = rr.pending[n:]

	return n, nil
}

// fill reads the next chunk from the underlying reader and stores the part of it that belongs to
// the selected lines in pending.
func (rr *rangeReader) fill() {
	n, err := rr.r.Read(rr.buf)
	rr.err = err
	chunk := rr.buf[:n]

	begin := 0
	for begin < n && rr.line < rr.start {
		i := bytes.IndexByte(chunk[begin:], '\n')
		if i < 0 {
			begin = n
			break
		}

		begin += i + 1
		rr.line++
	}

	stop := begin
	for stop < n && rr.line <= rr.end {
		i := bytes.IndexByte(chunk[stop:], '\n')
		if i < 0 {
			stop = n
			break
		}

		stop += i + 1
		rr.line++
	}

	rr.pending = chunk[begin:stop]
}
