package spawnhelper

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func mustClose(tb testing.TB, closer io.Closer) {
	tb.Helper()
	require.NoError(tb, closer.Close())
}

func startClient(tb testing.TB) *Client {
	tb.Helper()

	client, err := Start()
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, client.Close()) })

	return client
}

func devNullFiles(tb testing.TB) []*os.File {
	tb.Helper()

	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	require.NoError(tb, err)
	tb.Cleanup(func() { mustClose(tb, devNull) })

	return []*os.File{devNull, devNull, devNull}
}

func spawn(tb testing.TB, client *Client, files []*os.File, argv ...string) *Process {
	tb.Helper()

	req, err := NewRequest(argv[0], argv, nil, "")
	require.NoError(tb, err)

	process, err := client.Spawn(tb.Context(), req, files, -1)
	require.NoError(tb, err)

	return process
}

// procState returns the state of pid from /proc, or "" if there is no such process.
func procState(tb testing.TB, pid int) string {
	tb.Helper()

	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return ""
	}
	require.NoError(tb, err)

	return strings.Fields(string(stat[bytes.LastIndexByte(stat, ')')+1:]))[0]
}

// requireEventuallyGone waits until pid has exited. It may linger as a zombie of whoever adopted it.
func requireEventuallyGone(tb testing.TB, pid int) {
	tb.Helper()

	require.Eventually(tb, func() bool {
		state := procState(tb, pid)
		return state == "" || state == "Z"
	}, 10*time.Second, 10*time.Millisecond)
}

// childrenOf returns the PIDs of the children of pid.
func childrenOf(tb testing.TB, pid int) []int {
	tb.Helper()

	entries, err := os.ReadDir("/proc")
	require.NoError(tb, err)

	var children []int
	for _, entry := range entries {
		child, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}

		stat, err := os.ReadFile("/proc/" + entry.Name() + "/stat")
		if err != nil {
			continue
		}

		fields := strings.Fields(string(stat[bytes.LastIndexByte(stat, ')')+1:]))
		if fields[1] == strconv.Itoa(pid) {
			children = append(children, child)
		}
	}

	return children
}

func TestClient_spawn(t *testing.T) {
	t.Parallel()

	client := startClient(t)

	for _, tc := range []struct {
		desc             string
		script           string
		expectedExitCode int
		expectedSignal   syscall.Signal
	}{
		{desc: "success", script: "exit 0"},
		{desc: "failure", script: "exit 3", expectedExitCode: 3},
		{desc: "signal", script: "kill -TERM $$", expectedExitCode: -1, expectedSignal: syscall.SIGTERM},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			process := spawn(t, client, devNullFiles(t), "/bin/sh", "-c", tc.script)
			require.NotZero(t, process.Pid())

			status, rusage, err := process.Wait()
			require.NoError(t, err)
			require.Equal(t, tc.expectedExitCode, status.ExitStatus())
			require.Equal(t, tc.expectedSignal != 0, status.Signaled())
			if tc.expectedSignal != 0 {
				require.Equal(t, tc.expectedSignal, status.Signal())
			}
			require.NotNil(t, rusage)
			require.NotZero(t, rusage.Maxrss)
		})
	}
}

func TestClient_spawnRusage(t *testing.T) {
	t.Parallel()

	client := startClient(t)

	// Burn some CPU in a grandchild, which is accounted to the child once it has reaped it.
	process := spawn(t, client, devNullFiles(t), "/bin/sh", "-c", `sh -c 'i=0; while [ $i -lt 300000 ]; do i=$((i+1)); done'`)

	status, rusage, err := process.Wait()
	require.NoError(t, err)
	require.Zero(t, status.ExitStatus())
	require.Greater(t, time.Duration(rusage.Utime.Nano()+rusage.Stime.Nano()), 10*time.Millisecond)
}

func TestClient_spawnEnvironmentAndFiles(t *testing.T) {
	t.Parallel()

	client := startClient(t)
	dir := t.TempDir()

	stdinReader, stdinWriter, err := os.Pipe()
	require.NoError(t, err)
	defer mustClose(t, stdinWriter)
	stdoutReader, stdoutWriter, err := os.Pipe()
	require.NoError(t, err)
	defer mustClose(t, stdoutReader)
	extraReader, extraWriter, err := os.Pipe()
	require.NoError(t, err)
	defer mustClose(t, extraReader)

	req, err := NewRequest("/bin/sh", []string{"sh", "-c", `read line; echo "$line $FOO $(pwd)"; echo extra >&3`}, []string{"FOO=bar"}, dir)
	require.NoError(t, err)

	process, err := client.Spawn(t.Context(), req, []*os.File{stdinReader, stdoutWriter, stdoutWriter, extraWriter}, -1)
	require.NoError(t, err)
	for _, f := range []*os.File{stdinReader, stdoutWriter, extraWriter} {
		require.NoError(t, f.Close())
	}

	_, err = stdinWriter.WriteString("hello\n")
	require.NoError(t, err)

	status, _, err := process.Wait()
	require.NoError(t, err)
	require.Zero(t, status.ExitStatus())

	stdout, err := io.ReadAll(stdoutReader)
	require.NoError(t, err)
	require.Equal(t, "hello bar "+dir+"\n", string(stdout))

	extra, err := io.ReadAll(extraReader)
	require.NoError(t, err)
	require.Equal(t, "extra\n", string(extra))
}

func TestSpawnBody_roundTrip(t *testing.T) {
	t.Parallel()

	body := spawnBody{
		Path: "/bin/sh",
		Argv: []string{"sh", "-c", "invalid UTF-8: \xff\xfe", ""},
		Env:  []string{"A=\xff"},
		Dir:  "/tmp",
	}

	decoded, err := decodeSpawnBody(body.encode())
	require.NoError(t, err)
	require.Equal(t, body, decoded)

	for i := range len(body.encode()) {
		_, err := decodeSpawnBody(body.encode()[:i])
		require.Error(t, err)
	}
}

func TestClient_spawnInvalidUTF8(t *testing.T) {
	t.Parallel()

	client := startClient(t)

	stdoutReader, stdoutWriter, err := os.Pipe()
	require.NoError(t, err)
	defer mustClose(t, stdoutReader)

	arg := "\xff\xfe"
	process := spawn(t, client, []*os.File{devNullFiles(t)[0], stdoutWriter, stdoutWriter}, "/bin/printf", "%s", arg)
	require.NoError(t, stdoutWriter.Close())

	_, _, err = process.Wait()
	require.NoError(t, err)

	stdout, err := io.ReadAll(stdoutReader)
	require.NoError(t, err)
	require.Equal(t, arg, string(stdout))
}

func TestClient_spawnError(t *testing.T) {
	t.Parallel()

	client := startClient(t)

	req, err := NewRequest("/bin/true", []string{"true"}, nil, "/does/not/exist")
	require.NoError(t, err)

	_, err = client.Spawn(t.Context(), req, devNullFiles(t), -1)
	var pathErr *os.PathError
	require.ErrorAs(t, err, &pathErr)
	require.Equal(t, "fork/exec", pathErr.Op)
	require.ErrorIs(t, err, syscall.ENOENT)
	require.NotErrorIs(t, err, ErrUnavailable)

	// The helper keeps working after a failed spawn.
	status, _, err := spawn(t, client, devNullFiles(t), "/bin/true").Wait()
	require.NoError(t, err)
	require.Zero(t, status.ExitStatus())
}

func TestClient_spawnConcurrently(t *testing.T) {
	t.Parallel()

	client := startClient(t)
	files := devNullFiles(t)

	req, err := NewRequest("/bin/true", []string{"true"}, nil, "")
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for range cap(errs) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			process, err := client.Spawn(t.Context(), req, files, -1)
			if err == nil {
				_, _, err = process.Wait()
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

func TestClient_spawnCanceledContext(t *testing.T) {
	t.Parallel()

	client := startClient(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	req, err := NewRequest("/bin/sleep", []string{"sleep", "60"}, nil, "")
	require.NoError(t, err)

	_, err = client.Spawn(ctx, req, devNullFiles(t), -1)
	require.Equal(t, context.Canceled, err)
}

func TestClient_spawnAbandoned(t *testing.T) {
	t.Parallel()

	client := startClient(t)

	// Stop the helper so that the spawn request is only handled after Spawn has given up.
	require.NoError(t, syscall.Kill(client.Pid(), syscall.SIGSTOP))

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	req, err := NewRequest("/bin/sleep", []string{"sleep", "60"}, nil, "")
	require.NoError(t, err)

	_, err = client.Spawn(ctx, req, devNullFiles(t), -1)
	require.Equal(t, context.DeadlineExceeded, err)

	require.NoError(t, syscall.Kill(client.Pid(), syscall.SIGCONT))

	// The helper spawns the process once it continues, after which it is killed and reaped.
	require.Eventually(t, func() bool {
		return len(childrenOf(t, client.Pid())) == 0
	}, 10*time.Second, 10*time.Millisecond)

	status, _, err := spawn(t, client, devNullFiles(t), "/bin/true").Wait()
	require.NoError(t, err)
	require.Zero(t, status.ExitStatus())
}

func TestNewRequest_tooLarge(t *testing.T) {
	t.Parallel()

	_, err := NewRequest("/bin/true", []string{"true", strings.Repeat("x", MaxRequestSize)}, nil, "")
	require.Equal(t, ErrRequestTooLarge, err)
}

func TestProcess_signal(t *testing.T) {
	t.Parallel()

	client := startClient(t)

	process := spawn(t, client, devNullFiles(t), "/bin/sleep", "60")
	require.NoError(t, process.Signal(syscall.SIGTERM, true))

	status, _, err := process.Wait()
	require.NoError(t, err)
	require.True(t, status.Signaled())
	require.Equal(t, syscall.SIGTERM, status.Signal())

	// Signalling a reaped process is a no-op, and the helper keeps working.
	require.NoError(t, process.Signal(syscall.SIGKILL, true))
	status, _, err = spawn(t, client, devNullFiles(t), "/bin/true").Wait()
	require.NoError(t, err)
	require.Zero(t, status.ExitStatus())
}

// spawnWithBackgroundSleep spawns a shell that starts a sleep in the background, and returns the
// shell and the PID of the sleep, which is in the shell's process group.
func spawnWithBackgroundSleep(t *testing.T, client *Client, script string) (*Process, int) {
	t.Helper()

	stdoutReader, stdoutWriter, err := os.Pipe()
	require.NoError(t, err)
	defer mustClose(t, stdoutReader)

	process := spawn(t, client, []*os.File{devNullFiles(t)[0], stdoutWriter, stdoutWriter},
		"/bin/sh", "-c", "sleep 60 >/dev/null & echo $!; "+script)
	require.NoError(t, stdoutWriter.Close())

	line, err := bufio.NewReader(stdoutReader).ReadString('\n')
	require.NoError(t, err)
	sleepPid, err := strconv.Atoi(strings.TrimSpace(line))
	require.NoError(t, err)
	t.Cleanup(func() { _ = syscall.Kill(sleepPid, syscall.SIGKILL) })

	return process, sleepPid
}

func TestProcess_signalProcessOnly(t *testing.T) {
	t.Parallel()

	client := startClient(t)
	process, sleepPid := spawnWithBackgroundSleep(t, client, "wait")

	require.NoError(t, process.Signal(syscall.SIGKILL, false))

	status, _, err := process.Wait()
	require.NoError(t, err)
	require.Equal(t, syscall.SIGKILL, status.Signal())
	require.NotContains(t, []string{"", "Z"}, procState(t, sleepPid))
}

func TestProcess_signalGroupAfterLeaderExited(t *testing.T) {
	t.Parallel()

	client := startClient(t)
	// The shell exits right away, leaving the sleep running in its process group.
	process, sleepPid := spawnWithBackgroundSleep(t, client, "exit 0")

	// The exited shell stays a zombie until it is waited for, which keeps its process group
	// reachable.
	require.Eventually(t, func() bool { return procState(t, process.Pid()) == "Z" }, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, process.Signal(syscall.SIGTERM, true))
	requireEventuallyGone(t, sleepPid)

	status, _, err := process.Wait()
	require.NoError(t, err)
	require.Zero(t, status.ExitStatus())
	require.Empty(t, procState(t, process.Pid()))
}

func TestClient_helperDeathKillsChildren(t *testing.T) {
	t.Parallel()

	client := startClient(t)

	process := spawn(t, client, devNullFiles(t), "/bin/sleep", "60")
	require.NoError(t, syscall.Kill(client.Pid(), syscall.SIGKILL))

	_, _, err := process.Wait()
	require.Equal(t, errHelperDisconnected, err)
	require.True(t, client.Disconnected())
	requireEventuallyGone(t, process.Pid())

	req, err := NewRequest("/bin/true", []string{"true"}, nil, "")
	require.NoError(t, err)
	_, err = client.Spawn(t.Context(), req, devNullFiles(t), -1)
	require.ErrorIs(t, err, ErrUnavailable)
}

func TestClient_helperOutlivesGitalyUntilChildrenExit(t *testing.T) {
	t.Parallel()

	client := startClient(t)

	stdinReader, stdinWriter, err := os.Pipe()
	require.NoError(t, err)
	// Registered after startClient, so that the child exits before cleanup waits for the helper.
	t.Cleanup(func() { _ = stdinWriter.Close() })
	stdoutReader, stdoutWriter, err := os.Pipe()
	require.NoError(t, err)
	defer mustClose(t, stdoutReader)
	require.NoError(t, stdoutReader.SetReadDeadline(time.Now().Add(testTimeout)))

	// The child prints "done" only after its stdin is closed. Pdeathsig kills it if the helper
	// exits first, so the output shows that the helper outlived Gitaly.
	process := spawn(t, client, []*os.File{stdinReader, stdoutWriter, devNullFiles(t)[2]}, "/bin/sh", "-c", "cat; echo done")
	require.NoError(t, stdinReader.Close())
	require.NoError(t, stdoutWriter.Close())

	// Gitaly going away makes the helper stop serving. Only shut down writing, so that the client
	// is disconnected once the helper has stopped serving and closed its end.
	require.NoError(t, client.conn.CloseWrite())
	select {
	case <-client.disconnected:
	case <-time.After(testTimeout):
		require.FailNow(t, "helper didn't stop serving after Gitaly shut down writing")
	}

	_, _, err = process.Wait()
	require.Equal(t, errHelperDisconnected, err)

	require.NoError(t, stdinWriter.Close())
	output, err := io.ReadAll(stdoutReader)
	require.NoError(t, err)
	require.Equal(t, "done\n", string(output))

	select {
	case <-client.helperReaped:
	case <-time.After(testTimeout):
		require.FailNow(t, "helper didn't exit after its child")
	}
	require.Empty(t, procState(t, process.Pid()))
}

// TestGet_restartsCrashedHelperAfterInterval isn't parallel as it kills the shared default helper.
func TestGet_restartsCrashedHelperAfterInterval(t *testing.T) {
	client := Get(testLogger)
	require.NotNil(t, client)
	require.Same(t, client, Get(testLogger))

	require.NoError(t, syscall.Kill(client.Pid(), syscall.SIGKILL))
	<-client.disconnected

	// Commands are spawned directly until the retry interval has passed.
	require.Nil(t, Get(testLogger))
	require.Nil(t, Get(testLogger))

	defaultHelper.mu.Lock()
	defaultHelper.lastFailure = time.Now().Add(-startRetryInterval)
	defaultHelper.mu.Unlock()

	restarted := Get(testLogger)
	require.NotNil(t, restarted)
	require.NotSame(t, client, restarted)

	status, _, err := spawn(t, restarted, devNullFiles(t), "/bin/true").Wait()
	require.NoError(t, err)
	require.Zero(t, status.ExitStatus())
}

// TestGet_doesntWaitForExitedHelper isn't parallel as it stops the shared default helper.
func TestGet_doesntWaitForExitedHelper(t *testing.T) {
	defer func() {
		defaultHelper.mu.Lock()
		defaultHelper.lastFailure = time.Time{}
		defaultHelper.mu.Unlock()
	}()

	client := Get(testLogger)
	require.NotNil(t, client)

	stdinReader, stdinWriter, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = stdinWriter.Close() })
	devNull := devNullFiles(t)
	process := spawn(t, client, []*os.File{stdinReader, devNull[1], devNull[2]}, "/bin/cat")
	require.NoError(t, stdinReader.Close())

	// The helper stops serving but keeps running until its child exits.
	require.NoError(t, client.conn.Close())
	<-client.disconnected

	done := make(chan struct{})
	go func() {
		defer close(done)
		require.Nil(t, Get(testLogger))
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "Get waited for the exited helper")
	}

	require.NoError(t, stdinWriter.Close())
	_, _, err = process.Wait()
	require.Equal(t, errHelperDisconnected, err)
	<-client.helperReaped
}

// TestGet_retriesStartAfterInterval isn't parallel as it modifies the shared default helper.
func TestGet_retriesStartAfterInterval(t *testing.T) {
	Shutdown()
	defer Enable()

	Enable()
	defaultHelper.mu.Lock()
	defaultHelper.lastFailure = time.Now()
	defaultHelper.mu.Unlock()
	require.Nil(t, Get(testLogger))

	defaultHelper.mu.Lock()
	defaultHelper.lastFailure = time.Now().Add(-startRetryInterval)
	defaultHelper.mu.Unlock()
	require.NotNil(t, Get(testLogger))
}

// TestGet_concurrentStart isn't parallel as it modifies the shared default helper.
func TestGet_concurrentStart(t *testing.T) {
	Shutdown()
	defer Enable()

	Enable()
	defaultHelper.mu.Lock()
	defaultHelper.starting = true
	defaultHelper.mu.Unlock()

	// Spawns arriving while the helper is being started are spawned directly.
	require.Nil(t, Get(testLogger))

	defaultHelper.mu.Lock()
	defaultHelper.starting = false
	defaultHelper.mu.Unlock()
	require.NotNil(t, Get(testLogger))
}

// TestShutdown isn't parallel as it modifies the shared default helper.
func TestShutdown(t *testing.T) {
	defer Enable()

	require.NotNil(t, Get(testLogger))
	Shutdown()
	require.Zero(t, DefaultPid())
	require.Nil(t, Get(testLogger))
}

func TestClient_fileDescriptorsDontGrowWithChildren(t *testing.T) {
	t.Parallel()

	client := startClient(t)
	files := devNullFiles(t)

	countFDs := func() int {
		entries, err := os.ReadDir("/proc/" + strconv.Itoa(client.Pid()) + "/fd")
		require.NoError(t, err)
		return len(entries)
	}

	// Let the helper's runtime open whatever it opens lazily before taking the baseline.
	status, _, err := spawn(t, client, files, "/bin/true").Wait()
	require.NoError(t, err)
	require.Zero(t, status.ExitStatus())
	baseline := countFDs()

	var processes []*Process
	for range 50 {
		processes = append(processes, spawn(t, client, files, "/bin/sleep", "60"))
	}

	require.Equal(t, baseline, countFDs())
	require.Less(t, baseline, 16)

	for _, process := range processes {
		require.NoError(t, process.Signal(syscall.SIGKILL, true))
		_, _, err := process.Wait()
		require.NoError(t, err)
	}
}

func TestProcess_sendFailureDisconnects(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc string
		send func(*Process) error
	}{
		{
			desc: "wait",
			send: func(p *Process) error {
				_, _, err := p.Wait()
				return err
			},
		},
		{
			desc: "signal",
			send: func(p *Process) error {
				return p.Signal(syscall.SIGTERM, true)
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			client := startClient(t)
			process := spawn(t, client, devNullFiles(t), "/bin/sleep", "60")
			// The helper keeps running until its children have exited once disconnected.
			t.Cleanup(func() { _ = syscall.Kill(process.Pid(), syscall.SIGKILL) })

			sendErr := errors.New("injected send error")
			client.write = func([]byte) (int, error) { return 0, sendErr }

			if err := tc.send(process); !errors.Is(err, sendErr) {
				require.Equal(t, errHelperDisconnected, err)
			}
			require.True(t, client.Disconnected())

			// The process fails instead of waiting for a reply that never comes.
			_, _, err := process.Wait()
			require.Equal(t, errHelperDisconnected, err)
		})
	}
}

// writableCgroup creates a cgroup below the cgroup the test runs in and returns its directory. It
// skips the test if that isn't possible, for example without cgroup v2 or without permission.
func writableCgroup(t *testing.T) (string, string) {
	t.Helper()

	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err != nil {
		t.Skipf("cgroup v2 unavailable: %v", err)
	}

	self, err := os.ReadFile("/proc/self/cgroup")
	require.NoError(t, err)
	var current string
	for _, line := range strings.Split(strings.TrimSpace(string(self)), "\n") {
		if path, ok := strings.CutPrefix(line, "0::"); ok {
			current = path
		}
	}
	if current == "" {
		t.Skip("not in a cgroup v2 hierarchy")
	}

	dir, err := os.MkdirTemp(filepath.Join("/sys/fs/cgroup", current), "spawnhelper-")
	if err != nil {
		t.Skipf("creating cgroup: %v", err)
	}
	t.Cleanup(func() { require.NoError(t, os.Remove(dir)) })

	return dir, filepath.Join(current, filepath.Base(dir))
}

func TestClient_spawnIntoCgroup(t *testing.T) {
	t.Parallel()

	cgroupDir, cgroupPath := writableCgroup(t)
	dir, err := os.Open(cgroupDir)
	require.NoError(t, err)
	defer mustClose(t, dir)

	// Cloning into a cgroup may need more permissions than creating it.
	probe := exec.CommandContext(t.Context(), "/bin/true")
	probe.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(dir.Fd())}
	if err := probe.Run(); err != nil {
		t.Skipf("cloning into cgroup: %v", err)
	}

	client := startClient(t)

	stdoutReader, stdoutWriter, err := os.Pipe()
	require.NoError(t, err)
	defer mustClose(t, stdoutReader)

	req, err := NewRequest("/bin/cat", []string{"cat", "/proc/self/cgroup"}, nil, "")
	require.NoError(t, err)
	process, err := client.Spawn(t.Context(), req, []*os.File{devNullFiles(t)[0], stdoutWriter, stdoutWriter}, int(dir.Fd()))
	require.NoError(t, err)
	mustClose(t, stdoutWriter)

	status, _, err := process.Wait()
	require.NoError(t, err)
	require.Zero(t, status.ExitStatus())

	stdout, err := io.ReadAll(stdoutReader)
	require.NoError(t, err)
	require.Equal(t, "0::"+cgroupPath+"\n", string(stdout))
}
