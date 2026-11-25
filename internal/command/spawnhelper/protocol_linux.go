package spawnhelper

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"syscall"
)

// Every message is a single datagram made of a fixed header followed by a body. The child's file
// descriptors travel with the spawn request as SCM_RIGHTS. Spawn requests are encoded in binary as
// JSON would mangle arguments and environment variables that aren't valid UTF-8, all other bodies
// are JSON.
//
// The protocol isn't versioned as both ends are always the same binary: Gitaly starts the helper
// by executing /proc/self/exe.
const (
	headerSize = 10
	// MaxRequestSize is the maximum size of an encoded spawn request. Larger requests don't fit
	// into the socket's send buffer, so they must be spawned directly.
	MaxRequestSize = 128 * 1024
	// MaxFiles is the maximum number of file descriptors a spawn request can carry, including
	// the cgroup directory.
	MaxFiles = 32
	// maxReplySize bounds the size of replies. Only error messages are of variable length.
	maxReplySize = 4096
)

type messageType byte

const (
	msgSpawn messageType = iota + 1
	msgSignal
	// msgWait asks the helper to reap the child once it has exited. The helper doesn't reap
	// children before so that their PID and process group stay reserved, like they do for
	// children of Gitaly until exec.Cmd.Wait is called.
	msgWait
	msgStarted
	msgError
	msgExited
)

// flagCgroupFD is set on a spawn request if its last file descriptor is the cgroup directory the
// child should be cloned into.
const flagCgroupFD = 1 << 0

type spawnBody struct {
	Path string
	Argv []string
	Env  []string
	Dir  string
}

func (b spawnBody) encode() []byte {
	var buf []byte
	appendString := func(s string) {
		buf = binary.AppendUvarint(buf, uint64(len(s)))
		buf = append(buf, s...)
	}
	appendStrings := func(ss []string) {
		buf = binary.AppendUvarint(buf, uint64(len(ss)))
		for _, s := range ss {
			appendString(s)
		}
	}

	appendString(b.Path)
	appendStrings(b.Argv)
	appendStrings(b.Env)
	appendString(b.Dir)

	return buf
}

func decodeSpawnBody(buf []byte) (spawnBody, error) {
	errMalformed := errors.New("malformed spawn request")

	readUvarint := func() (uint64, error) {
		n, size := binary.Uvarint(buf)
		if size <= 0 {
			return 0, errMalformed
		}
		buf = buf[size:]
		return n, nil
	}
	readString := func() (string, error) {
		n, err := readUvarint()
		if err != nil {
			return "", err
		}
		if n > uint64(len(buf)) {
			return "", errMalformed
		}
		s := string(buf[:n])
		buf = buf[n:]
		return s, nil
	}
	readStrings := func() ([]string, error) {
		n, err := readUvarint()
		if err != nil {
			return nil, err
		}
		// Every string takes at least one byte, which bounds the allocation.
		if n > uint64(len(buf)) {
			return nil, errMalformed
		}
		ss := make([]string, n)
		for i := range ss {
			if ss[i], err = readString(); err != nil {
				return nil, err
			}
		}
		return ss, nil
	}

	var b spawnBody
	var err error
	if b.Path, err = readString(); err != nil {
		return spawnBody{}, err
	}
	if b.Argv, err = readStrings(); err != nil {
		return spawnBody{}, err
	}
	if b.Env, err = readStrings(); err != nil {
		return spawnBody{}, err
	}
	if b.Dir, err = readString(); err != nil {
		return spawnBody{}, err
	}
	if len(buf) != 0 {
		return spawnBody{}, errMalformed
	}

	return b, nil
}

type signalBody struct {
	Signal syscall.Signal `json:"signal"`
	// Group sends the signal to the child's process group instead of only the child.
	Group bool `json:"group"`
}

type startedBody struct {
	Pid int `json:"pid"`
}

type errorBody struct {
	Errno   syscall.Errno `json:"errno"`
	Message string        `json:"message"`
}

func (b errorBody) err() error {
	switch {
	case b.Message == "":
		return b.Errno
	case b.Errno == 0:
		return errors.New(b.Message)
	default:
		return fmt.Errorf("%s: %w", b.Message, b.Errno)
	}
}

type exitedBody struct {
	Status syscall.WaitStatus `json:"status"`
	Rusage syscall.Rusage     `json:"rusage"`
}

func encodeMessage(typ messageType, id uint64, body any) ([]byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal body: %w", err)
	}

	return appendHeader(typ, 0, id, payload), nil
}

func appendHeader(typ messageType, flags byte, id uint64, payload []byte) []byte {
	msg := make([]byte, headerSize, headerSize+len(payload))
	msg[0] = byte(typ)
	msg[1] = flags
	binary.LittleEndian.PutUint64(msg[2:], id)
	return append(msg, payload...)
}

func decodeHeader(msg []byte) (messageType, byte, uint64, []byte, error) {
	if len(msg) < headerSize {
		return 0, 0, 0, nil, errors.New("short message")
	}

	return messageType(msg[0]), msg[1], binary.LittleEndian.Uint64(msg[2:]), msg[headerSize:], nil
}
