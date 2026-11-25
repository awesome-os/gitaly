package reftable

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
)

const (
	// defaultBlockSize is the block size Git writes reftables with.
	defaultBlockSize = 4096
	// restartInterval is how often Git starts a record with an empty
	// prefix, so readers can binary search a block.
	restartInterval = 16
	// maxRestarts is the most restart points a block can have, as the
	// count is a uint16.
	maxRestarts = 1<<16 - 1
	// maxBlockLen is the longest block a uint24 block_len can describe.
	maxBlockLen = 1<<24 - 1
)

// ErrBlockTooLarge is returned by WriteTableAt when the records don't fit
// in a single block.
var ErrBlockTooLarge = errors.New("records don't fit in a single block")

// Value types of a ref record, stored in the low 3 bits of the suffix length.
const (
	valueTypeDeletion = 0
	valueTypeVal1     = 1
	valueTypeVal2     = 2
	valueTypeSymref   = 3
)

// WriteTableAt writes a reftable with the given records to absolutePath. All
// records are written at updateIndex, and they must be sorted by name with no
// duplicates. The records go into a single ref block, so the table has no
// index, object or log sections. A table whose block fits in Git's default
// block size is laid out byte for byte as Git writes it; a larger one gets a
// block size equal to its block length.
func WriteTableAt(absolutePath string, updateIndex uint64, objectHash git.ObjectHash, records []RefRecord) (returnedErr error) {
	data, err := encodeTable(updateIndex, objectHash, records)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(absolutePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close: %w", err))
		}
	}()

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	return nil
}

func encodeTable(updateIndex uint64, objectHash git.ObjectHash, records []RefRecord) ([]byte, error) {
	if len(records) == 0 {
		// Git doesn't write tables without records either.
		return nil, errors.New("no records to write")
	}

	// Git writes version 1 for SHA-1 and version 2, which names the hash,
	// for SHA-256.
	v, hashID := version(1), hashIDSHA1
	if objectHash.Format == git.ObjectHashSHA256.Format {
		v, hashID = 2, hashIDSHA256
	}
	headerSize := v.HeaderSize()
	hashSize := objectHash.Hash().Size()

	// The first block starts with the file header. Its block_len, like the
	// restart offsets, counts from the start of the file.
	data := make([]byte, headerSize, defaultBlockSize)
	data = append(data, 'r', 0, 0, 0)

	var restarts []int
	for i, record := range records {
		if record.Name == "" {
			return nil, errors.New("record without a name")
		}
		if i > 0 && record.Name <= records[i-1].Name {
			return nil, fmt.Errorf("records not sorted: %q after %q", record.Name, records[i-1].Name)
		}

		valueType, value, err := encodeValue(record, hashSize)
		if err != nil {
			return nil, fmt.Errorf("record %q: %w", record.Name, err)
		}

		prefixLen := 0
		if i%restartInterval != 0 {
			prefixLen = commonPrefixLen(records[i-1].Name, record.Name)
		}
		// Git starts a restart point at every record without a prefix, not
		// only at every restartInterval records.
		if prefixLen == 0 && len(restarts) < maxRestarts {
			restarts = append(restarts, len(data))
		}

		suffix := record.Name[prefixLen:]
		data = putVarInt(data, uint64(prefixLen))
		data = putVarInt(data, uint64(len(suffix))<<3|valueType)
		data = append(data, suffix...)
		// The update index is stored relative to the table's minimum, and
		// every record is at that minimum.
		data = putVarInt(data, 0)
		data = append(data, value...)
	}

	for _, restart := range restarts {
		data = putUint24(data, uint(restart))
	}
	data = binary.BigEndian.AppendUint16(data, uint16(len(restarts)))

	blockLen := len(data)
	if blockLen > maxBlockLen {
		return nil, fmt.Errorf("%w: %d bytes", ErrBlockTooLarge, blockLen)
	}
	copy(data[headerSize+1:], putUint24(nil, uint(blockLen)))

	blockSize := defaultBlockSize
	if blockLen > blockSize {
		// Readers expect a block to be no larger than the block size.
		blockSize = blockLen
	}

	hdr := encodeHeader(v, hashID, uint(blockSize), updateIndex)
	copy(data, hdr)

	// The footer repeats the header, then the offsets of the ref index,
	// object, object index, log and log index sections, all absent here.
	footerStart := len(data)
	data = append(data, hdr...)
	data = append(data, make([]byte, 5*8)...)
	data = binary.BigEndian.AppendUint32(data, crc32.ChecksumIEEE(data[footerStart:]))

	return data, nil
}

func encodeValue(record RefRecord, hashSize int) (uint64, []byte, error) {
	switch {
	case record.IsDeletion:
		return valueTypeDeletion, nil, nil
	case record.IsSymbolic:
		if record.Target == "" {
			return 0, nil, errors.New("symbolic ref without a target")
		}
		value := putVarInt(nil, uint64(len(record.Target)))
		return valueTypeSymref, append(value, record.Target...), nil
	}

	value, err := decodeOID(record.Target, hashSize)
	if err != nil {
		return 0, nil, fmt.Errorf("target: %w", err)
	}
	if record.Peeled == "" {
		return valueTypeVal1, value, nil
	}

	peeled, err := decodeOID(record.Peeled, hashSize)
	if err != nil {
		return 0, nil, fmt.Errorf("peeled: %w", err)
	}
	return valueTypeVal2, append(value, peeled...), nil
}

func decodeOID(oid string, hashSize int) ([]byte, error) {
	raw, err := hex.DecodeString(oid)
	if err != nil {
		return nil, fmt.Errorf("decode %q: %w", oid, err)
	}
	if len(raw) != hashSize {
		return nil, fmt.Errorf("object ID %q has %d bytes, want %d", oid, len(raw), hashSize)
	}
	return raw, nil
}

func encodeHeader(v version, hashID [4]byte, blockSize uint, updateIndex uint64) []byte {
	hdr := make([]byte, 0, v.HeaderSize())
	hdr = append(hdr, magic[:]...)
	hdr = append(hdr, byte(v))
	hdr = putUint24(hdr, blockSize)
	hdr = binary.BigEndian.AppendUint64(hdr, updateIndex)
	hdr = binary.BigEndian.AppendUint64(hdr, updateIndex)
	if v == 2 {
		hdr = append(hdr, hashID[:]...)
	}
	return hdr
}

// putVarInt appends value in the varint encoding reftables use, the inverse
// of Table.getVarInt.
func putVarInt(dst []byte, value uint64) []byte {
	var buf [10]byte
	pos := len(buf) - 1
	buf[pos] = byte(value & 0x7f)
	for value >>= 7; value != 0; value >>= 7 {
		value--
		pos--
		buf[pos] = 0x80 | byte(value&0x7f)
	}
	return append(dst, buf[pos:]...)
}

func putUint24(dst []byte, value uint) []byte {
	return append(dst, byte(value>>16), byte(value>>8), byte(value))
}

func commonPrefixLen(a, b string) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
