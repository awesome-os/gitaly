package spawnhelper

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testTimeout bounds how long the tests wait for the helper, so that a broken helper fails them
// instead of hanging them.
const testTimeout = 10 * time.Second

// socketPair returns the two ends of a socket pair: Gitaly's as a raw file descriptor, which is
// closed on cleanup, and the helper's as a connection.
func socketPair(t *testing.T) (int, *net.UnixConn) {
	t.Helper()

	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	gitalyFD := fds[0]
	t.Cleanup(func() { _ = syscall.Close(gitalyFD) })
	setReceiveTimeout(t, gitalyFD)

	helperFile := os.NewFile(uintptr(fds[1]), "helper")
	conn, err := net.FileConn(helperFile)
	require.NoError(t, err)
	mustClose(t, helperFile)

	return gitalyFD, conn.(*net.UnixConn)
}

// setReceiveTimeout makes reads from fd fail after testTimeout. Such reads aren't restarted when a
// signal interrupts them, so read with retryOnEINTR.
func setReceiveTimeout(t *testing.T, fd int) {
	t.Helper()

	timeout := syscall.NsecToTimeval(testTimeout.Nanoseconds())
	require.NoError(t, syscall.SetsockoptTimeval(fd, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &timeout))
}

// retryOnEINTR calls read until it isn't interrupted by a signal, which the Go runtime sends
// frequently.
func retryOnEINTR(read func() error) error {
	for {
		if err := read(); !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

// startServer serves on a socket pair and returns Gitaly's end and the result of serving.
func startServer(t *testing.T) (int, *net.UnixConn, <-chan error) {
	t.Helper()

	gitalyFD, helperConn := socketPair(t)

	served := make(chan error, 1)
	go func() { served <- serveConn(helperConn) }()

	return gitalyFD, helperConn, served
}

// waitServed waits for serving to stop and returns its result.
func waitServed(t *testing.T, served <-chan error) error {
	t.Helper()

	select {
	case err := <-served:
		return err
	case <-time.After(testTimeout):
		require.FailNow(t, "serving didn't stop")
		return nil
	}
}

// requireServing checks that serving hasn't stopped yet.
func requireServing(t *testing.T, served <-chan error) {
	t.Helper()

	select {
	case err := <-served:
		require.FailNow(t, "serving stopped early", "error: %v", err)
	default:
	}
}

func readMessage(t *testing.T, fd int) (messageType, uint64, []byte) {
	t.Helper()

	msg := make([]byte, headerSize+MaxRequestSize)
	var n int
	require.NoError(t, retryOnEINTR(func() (err error) {
		n, err = syscall.Read(fd, msg)
		return err
	}))
	typ, _, id, body, err := decodeHeader(msg[:n])
	require.NoError(t, err)

	return typ, id, body
}

func TestServeConn_errorKillsChildren(t *testing.T) {
	t.Parallel()

	gitalyFD, helperConn, served := startServer(t)

	devNull := devNullFiles(t)[0]
	msg := appendHeader(msgSpawn, 0, 1, spawnBody{Path: "/bin/sleep", Argv: []string{"sleep", "60"}}.encode())
	fd := int(devNull.Fd())
	require.NoError(t, syscall.Sendmsg(gitalyFD, msg, syscall.UnixRights(fd, fd, fd), nil, 0))

	typ, id, body := readMessage(t, gitalyFD)
	require.Equal(t, msgStarted, typ)
	require.Equal(t, uint64(1), id)
	var started startedBody
	require.NoError(t, json.Unmarshal(body, &started))

	// Credentials can't be parsed as file descriptors, which fails serving.
	rawConn, err := helperConn.SyscallConn()
	require.NoError(t, err)
	require.NoError(t, rawConn.Control(func(fd uintptr) {
		require.NoError(t, syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_PASSCRED, 1))
	}))
	_, err = syscall.Write(gitalyFD, appendHeader(msgWait, 0, 2, []byte("{}")))
	require.NoError(t, err)

	require.ErrorContains(t, waitServed(t, served), "parse rights")

	// The child has been killed and reaped rather than waited for.
	require.ErrorIs(t, syscall.Kill(started.Pid, 0), syscall.ESRCH)
}

func TestServeConn_gitalyExitsWithUnreadReplies(t *testing.T) {
	t.Parallel()

	// Gitaly's end is closed by the test, so don't use socketPair, which closes it again on
	// cleanup.
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	gitalyFD := fds[0]
	setReceiveTimeout(t, gitalyFD)
	helperFile := os.NewFile(uintptr(fds[1]), "helper")
	helperConn, err := net.FileConn(helperFile)
	require.NoError(t, err)
	mustClose(t, helperFile)

	served := make(chan error, 1)
	go func() { served <- serveConn(helperConn.(*net.UnixConn)) }()

	stdinReader, stdinWriter, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = stdinWriter.Close() })
	stdoutReader, stdoutWriter, err := os.Pipe()
	require.NoError(t, err)
	defer mustClose(t, stdoutReader)
	require.NoError(t, stdoutReader.SetReadDeadline(time.Now().Add(testTimeout)))

	// The child prints "done" only after its stdin is closed, so the output shows it wasn't
	// killed when the connection was reset.
	msg := appendHeader(msgSpawn, 0, 1, spawnBody{Path: "/bin/sh", Argv: []string{"sh", "-c", "cat; echo done"}}.encode())
	rights := syscall.UnixRights(int(stdinReader.Fd()), int(stdoutWriter.Fd()), int(devNullFiles(t)[2].Fd()))
	require.NoError(t, syscall.Sendmsg(gitalyFD, msg, rights, nil, 0))
	require.NoError(t, stdinReader.Close())
	require.NoError(t, stdoutWriter.Close())

	// Wait for the started reply but leave it unread, so that closing Gitaly's end resets the
	// connection rather than closing it cleanly.
	require.NoError(t, retryOnEINTR(func() error {
		_, _, err := syscall.Recvfrom(gitalyFD, make([]byte, maxReplySize), syscall.MSG_PEEK)
		return err
	}))
	require.NoError(t, syscall.Close(gitalyFD))

	// The helper keeps running until its child exits.
	requireServing(t, served)
	require.NoError(t, stdinWriter.Close())
	output, err := io.ReadAll(stdoutReader)
	require.NoError(t, err)
	require.Equal(t, "done\n", string(output))
	require.NoError(t, waitServed(t, served))
}

func TestServeConn_waitForRunningChildReapsOnSIGCHLD(t *testing.T) {
	t.Parallel()

	gitalyFD, _, served := startServer(t)

	stdinReader, stdinWriter, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = stdinWriter.Close() })
	devNull := devNullFiles(t)
	msg := appendHeader(msgSpawn, 0, 1, spawnBody{Path: "/bin/cat", Argv: []string{"cat"}}.encode())
	rights := syscall.UnixRights(int(stdinReader.Fd()), int(devNull[1].Fd()), int(devNull[2].Fd()))
	require.NoError(t, syscall.Sendmsg(gitalyFD, msg, rights, nil, 0))
	require.NoError(t, stdinReader.Close())
	typ, id, _ := readMessage(t, gitalyFD)
	require.Equal(t, msgStarted, typ)
	require.Equal(t, uint64(1), id)

	_, err = syscall.Write(gitalyFD, appendHeader(msgWait, 0, 1, []byte("{}")))
	require.NoError(t, err)

	// Send an unknown request as a barrier: it always gets an error reply. Requests are handled
	// in order, so once the reply arrives, the wait has been handled while the child was still
	// running, as it can't exit before its stdin is closed. It is then only reaped on SIGCHLD.
	_, err = syscall.Write(gitalyFD, appendHeader(messageType(0xff), 0, 2, []byte("{}")))
	require.NoError(t, err)
	typ, id, _ = readMessage(t, gitalyFD)
	require.Equal(t, msgError, typ)
	require.Equal(t, uint64(2), id)

	require.NoError(t, stdinWriter.Close())
	typ, id, body := readMessage(t, gitalyFD)
	require.Equal(t, msgExited, typ)
	require.Equal(t, uint64(1), id)
	var exited exitedBody
	require.NoError(t, json.Unmarshal(body, &exited))
	require.True(t, exited.Status.Exited())
	require.Zero(t, exited.Status.ExitStatus())

	require.NoError(t, syscall.Shutdown(gitalyFD, syscall.SHUT_WR))
	require.NoError(t, waitServed(t, served))
}

func TestServeConn_errorReplies(t *testing.T) {
	t.Parallel()

	devNull := devNullFiles(t)[0]
	fd := int(devNull.Fd())

	for _, tc := range []struct {
		desc          string
		msg           []byte
		rights        []byte
		expectedErrno syscall.Errno
		expectedMsg   string
	}{
		{
			desc:          "truncated request",
			msg:           appendHeader(msgSpawn, 0, 1, make([]byte, MaxRequestSize+1)),
			expectedErrno: syscall.EMSGSIZE,
			expectedMsg:   "request truncated",
		},
		{
			desc:          "malformed spawn request",
			msg:           appendHeader(msgSpawn, 0, 1, []byte{0xff}),
			rights:        syscall.UnixRights(fd, fd, fd),
			expectedErrno: syscall.EINVAL,
			expectedMsg:   "malformed spawn request",
		},
		{
			desc:          "missing cgroup file descriptor",
			msg:           appendHeader(msgSpawn, flagCgroupFD, 1, spawnBody{Path: "/bin/true", Argv: []string{"true"}}.encode()),
			expectedErrno: syscall.EINVAL,
			expectedMsg:   "missing cgroup file descriptor",
		},
		{
			desc:          "unknown message type",
			msg:           appendHeader(messageType(0xff), 0, 1, []byte("{}")),
			expectedErrno: syscall.EINVAL,
			expectedMsg:   "unknown message type 255",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			gitalyFD, _, served := startServer(t)

			require.NoError(t, syscall.Sendmsg(gitalyFD, tc.msg, tc.rights, nil, 0))

			typ, id, body := readMessage(t, gitalyFD)
			require.Equal(t, msgError, typ)
			require.Equal(t, uint64(1), id)
			var reply errorBody
			require.NoError(t, json.Unmarshal(body, &reply))
			require.Equal(t, errorBody{Errno: tc.expectedErrno, Message: tc.expectedMsg}, reply)

			// The helper keeps serving after replying with an error.
			msg := appendHeader(msgSpawn, 0, 2, spawnBody{Path: "/bin/true", Argv: []string{"true"}}.encode())
			require.NoError(t, syscall.Sendmsg(gitalyFD, msg, syscall.UnixRights(fd, fd, fd), nil, 0))
			typ, id, _ = readMessage(t, gitalyFD)
			require.Equal(t, msgStarted, typ)
			require.Equal(t, uint64(2), id)

			_, err := syscall.Write(gitalyFD, appendHeader(msgWait, 0, 2, []byte("{}")))
			require.NoError(t, err)
			typ, id, _ = readMessage(t, gitalyFD)
			require.Equal(t, msgExited, typ)
			require.Equal(t, uint64(2), id)

			require.NoError(t, syscall.Shutdown(gitalyFD, syscall.SHUT_WR))
			require.NoError(t, waitServed(t, served))
		})
	}
}

func TestErrorBody_err(t *testing.T) {
	t.Parallel()

	require.Equal(t, syscall.ENOENT, errorBody{Errno: syscall.ENOENT}.err())
	require.EqualError(t, errorBody{Message: "failed"}.err(), "failed")

	err := errorBody{Errno: syscall.EINVAL, Message: "missing cgroup file descriptor"}.err()
	require.EqualError(t, err, "missing cgroup file descriptor: invalid argument")
	require.ErrorIs(t, err, syscall.EINVAL)
}

func TestClient_corruptStartedReplyKillsChild(t *testing.T) {
	t.Parallel()

	helperFD, gitalyConn := socketPair(t)
	client := &Client{
		conn:         gitalyConn,
		write:        gitalyConn.Write,
		pending:      map[uint64]*Process{},
		disconnected: make(chan struct{}),
	}
	go client.readLoop()
	defer func() {
		mustClose(t, gitalyConn)
		<-client.disconnected
	}()

	req, err := NewRequest("/bin/sleep", []string{"sleep", "60"}, nil, "")
	require.NoError(t, err)

	files := devNullFiles(t)
	spawned := make(chan error, 1)
	go func() {
		_, err := client.Spawn(t.Context(), req, files, -1)
		spawned <- err
	}()

	// Play the helper: accept the spawn request and reply with a corrupt started message.
	msg := make([]byte, headerSize+MaxRequestSize)
	oob := make([]byte, syscall.CmsgSpace(MaxFiles*4))
	var n, oobn int
	require.NoError(t, retryOnEINTR(func() (err error) {
		n, oobn, _, _, err = syscall.Recvmsg(helperFD, msg, oob, 0)
		return err
	}))
	fds, err := parseRights(oob[:oobn])
	require.NoError(t, err)
	closeFDs(fds)
	typ, _, id, _, err := decodeHeader(msg[:n])
	require.NoError(t, err)
	require.Equal(t, msgSpawn, typ)

	_, err = syscall.Write(helperFD, appendHeader(msgStarted, 0, id, []byte("corrupt")))
	require.NoError(t, err)
	require.ErrorContains(t, <-spawned, "unmarshal started")

	// The child is running regardless, so the client kills its process group and reaps it.
	typ, signalID, body := readMessage(t, helperFD)
	require.Equal(t, msgSignal, typ)
	require.Equal(t, id, signalID)
	var signal signalBody
	require.NoError(t, json.Unmarshal(body, &signal))
	require.Equal(t, signalBody{Signal: syscall.SIGKILL, Group: true}, signal)

	typ, waitID, _ := readMessage(t, helperFD)
	require.Equal(t, msgWait, typ)
	require.Equal(t, id, waitID)
}
