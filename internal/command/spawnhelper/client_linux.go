package spawnhelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
)

// ErrRequestTooLarge is returned if a spawn request doesn't fit into a single message.
var ErrRequestTooLarge = errors.New("spawn request too large")

// errHelperDisconnected is returned for processes the helper can no longer report on because the
// connection to it has been lost.
var errHelperDisconnected = errors.New("spawn helper disconnected")

// Client is a running spawn helper.
type Client struct {
	conn *net.UnixConn
	cmd  *exec.Cmd
	// write writes a message to conn. Tests replace it to inject errors.
	write func([]byte) (int, error)

	mu sync.Mutex
	// pending is nil once the client has been disconnected, after which no further spawns are
	// possible.
	pending map[uint64]*Process
	nextID  uint64

	disconnectOnce sync.Once
	disconnected   chan struct{}
	// helperReaped is closed once the helper process has been reaped.
	helperReaped chan struct{}
}

// Start starts a spawn helper by executing the running binary as the Subcommand.
func Start() (*Client, error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("socketpair: %w", err)
	}
	local := os.NewFile(uintptr(fds[0]), "spawn-helper")
	remote := os.NewFile(uintptr(fds[1]), "spawn-helper")

	// The kernel rejects datagrams larger than the send buffer minus 32 bytes.
	if sndbuf, err := syscall.GetsockoptInt(fds[0], syscall.SOL_SOCKET, syscall.SO_SNDBUF); err != nil {
		_ = local.Close()
		_ = remote.Close()
		return nil, fmt.Errorf("get send buffer size: %w", err)
	} else if sndbuf-32 < headerSize+MaxRequestSize {
		_ = local.Close()
		_ = remote.Close()
		return nil, fmt.Errorf("send buffer of %d bytes too small for requests of %d bytes", sndbuf, MaxRequestSize)
	}

	conn, err := net.FileConn(local)
	if closeErr := local.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("close local socket: %w", closeErr))
	}
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		_ = remote.Close()
		return nil, fmt.Errorf("socket: %w", err)
	}

	// /proc/self/exe keeps referring to the running binary even if it has been replaced on disk,
	// for example during a graceful upgrade.
	cmd := &exec.Cmd{
		Path:       "/proc/self/exe",
		Args:       []string{os.Args[0], Subcommand},
		ExtraFiles: []*os.File{remote},
		Stderr:     os.Stderr,
		// Keep signals sent to Gitaly's process group, for example by a terminal, away from
		// the helper.
		SysProcAttr: &syscall.SysProcAttr{Setpgid: true},
	}
	err = cmd.Start()
	// The helper holds its own copy of its end once started. An error closing ours leaves
	// nothing to clean up, so it is ignored.
	_ = remote.Close()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("start helper: %w", err)
	}

	unixConn := conn.(*net.UnixConn)
	c := &Client{
		conn:         unixConn,
		cmd:          cmd,
		write:        unixConn.Write,
		pending:      map[uint64]*Process{},
		disconnected: make(chan struct{}),
		helperReaped: make(chan struct{}),
	}

	go func() {
		defer close(c.helperReaped)
		_ = cmd.Wait()
	}()
	go c.readLoop()

	return c, nil
}

// Pid returns the PID of the helper process.
func (c *Client) Pid() int {
	return c.cmd.Process.Pid
}

// Disconnected returns whether the connection to the helper has been lost, for example because it
// exited. No further spawns are possible afterwards.
func (c *Client) Disconnected() bool {
	select {
	case <-c.disconnected:
		return true
	default:
		return false
	}
}

// Close closes the connection to the helper and waits for it to exit. Processes that haven't been
// reaped yet fail. The helper only exits once all of its children have exited.
func (c *Client) Close() error {
	err := c.conn.Close()
	if errors.Is(err, net.ErrClosed) {
		err = nil
	}
	<-c.disconnected
	<-c.helperReaped
	return err
}

func (c *Client) readLoop() {
	buf := make([]byte, maxReplySize)
	for {
		// Replies never carry file descriptors. Should any arrive regardless, the kernel
		// closes them as there is no room for the control message.
		n, err := c.conn.Read(buf)
		if err != nil || n == 0 {
			c.disconnect()
			return
		}

		typ, _, id, body, err := decodeHeader(buf[:n])
		if err != nil {
			continue
		}

		c.mu.Lock()
		p, ok := c.pending[id]
		if ok {
			switch {
			case typ == msgStarted:
			case typ == msgError && p.hasStarted():
				// Only a spawn can fail, so don't let a stray error fail a running process.
				ok = false
			default:
				delete(c.pending, id)
			}
		}
		c.mu.Unlock()
		if !ok {
			continue
		}

		p.handleReply(typ, body)
	}
}

// disconnect closes the connection to the helper and fails all processes it hasn't reported on.
func (c *Client) disconnect() {
	c.disconnectOnce.Do(func() {
		c.mu.Lock()
		pending := c.pending
		c.pending = nil
		close(c.disconnected)
		c.mu.Unlock()

		// Closing the socket makes the helper stop serving requests if it is still running.
		_ = c.conn.Close()

		for _, p := range pending {
			p.fail(errHelperDisconnected)
		}
	})
}

// Request is an encoded spawn request.
type Request struct {
	path string
	body []byte
}

// NewRequest encodes a request to spawn path with the given arguments, environment and working
// directory. It returns ErrRequestTooLarge if the request doesn't fit into a single message.
func NewRequest(path string, argv, env []string, dir string) (*Request, error) {
	body := spawnBody{Path: path, Argv: argv, Env: env, Dir: dir}.encode()
	if len(body) > MaxRequestSize {
		return nil, ErrRequestTooLarge
	}

	return &Request{path: path, body: body}, nil
}

// Spawn spawns the process described by req. files become the child's file descriptors 0, 1, 2
// and so on. If cgroupFD is not negative, the child is cloned into the cgroup whose directory it
// refers to. The caller may close the files once Spawn has returned.
//
// Spawn returns an error wrapping ErrUnavailable if the request couldn't be sent, and ctx.Err() if
// the context is done before the process has been started. In the latter case the process is
// killed and reaped once the helper has started it.
func (c *Client) Spawn(ctx context.Context, req *Request, files []*os.File, cgroupFD int) (*Process, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	fds := make([]int, 0, len(files)+1)
	for _, f := range files {
		fds = append(fds, int(f.Fd()))
	}

	var flags byte
	if cgroupFD >= 0 {
		flags |= flagCgroupFD
		fds = append(fds, cgroupFD)
	}

	if len(fds) > MaxFiles {
		return nil, fmt.Errorf("too many files: %d", len(fds))
	}

	c.mu.Lock()
	if c.pending == nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, errHelperDisconnected)
	}
	c.nextID++
	p := &Process{
		client:  c,
		id:      c.nextID,
		path:    req.path,
		started: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	c.pending[p.id] = p
	c.mu.Unlock()

	// The kernel takes its own references to the file descriptors when sending them.
	_, _, err := c.conn.WriteMsgUnix(appendHeader(msgSpawn, flags, p.id, req.body), syscall.UnixRights(fds...), nil)
	runtime.KeepAlive(files)
	if err != nil {
		c.mu.Lock()
		delete(c.pending, p.id)
		c.mu.Unlock()
		// The helper never received the request, so nothing has been spawned.
		return nil, fmt.Errorf("%w: send spawn request: %w", ErrUnavailable, err)
	}

	select {
	case <-p.started:
	case <-ctx.Done():
		c.mu.Lock()
		select {
		case <-p.started:
		default:
			p.abandoned = true
		}
		abandoned := p.abandoned
		c.mu.Unlock()

		if abandoned {
			return nil, ctx.Err()
		}
	}

	if p.startErr != nil {
		return nil, p.startErr
	}

	return p, nil
}

// send sends a request concerning a process the helper has accepted a spawn request for. If
// sending fails, the client is disconnected so that the process fails instead of waiting forever
// for a reply to a request the helper never received.
func (c *Client) send(typ messageType, id uint64, body any) error {
	msg, err := encodeMessage(typ, id, body)
	if err != nil {
		return err
	}

	if _, err := c.write(msg); err != nil {
		c.disconnect()
		return fmt.Errorf("send: %w", err)
	}

	return nil
}

// Process is a process spawned by the helper.
type Process struct {
	client *Client
	id     uint64
	path   string

	// started is closed once the helper has replied to the spawn request. pid or startErr
	// are set before it is closed.
	started  chan struct{}
	pid      int
	startErr error
	// abandoned is set if Spawn returned before the process was started. It is guarded by the
	// client's mutex.
	abandoned bool

	waitOnce sync.Once
	// exited is closed once the helper has reaped the process or the client has been
	// disconnected. status and rusage, or waitErr, are set before it is closed.
	exited  chan struct{}
	status  syscall.WaitStatus
	rusage  syscall.Rusage
	waitErr error
}

func (p *Process) handleReply(typ messageType, body []byte) {
	switch typ {
	case msgStarted:
		var started startedBody
		if err := json.Unmarshal(body, &started); err != nil {
			// The process is running regardless, so don't leave it behind.
			p.fail(fmt.Errorf("unmarshal started: %w", err))
			p.killAndReap()
			return
		}

		p.client.mu.Lock()
		select {
		case <-p.started:
			// The client has been disconnected concurrently, which failed the process.
			p.client.mu.Unlock()
			return
		default:
		}
		p.pid = started.Pid
		close(p.started)
		abandoned := p.abandoned
		p.client.mu.Unlock()

		if abandoned {
			p.killAndReap()
		}
	case msgError:
		var reply errorBody
		if err := json.Unmarshal(body, &reply); err != nil {
			p.fail(fmt.Errorf("unmarshal error: %w", err))
			return
		}
		// Match the error os.StartProcess returns.
		p.fail(&os.PathError{Op: "fork/exec", Path: p.path, Err: reply.err()})
	case msgExited:
		var exited exitedBody
		if err := json.Unmarshal(body, &exited); err != nil {
			p.fail(fmt.Errorf("unmarshal exited: %w", err))
			return
		}
		p.status = exited.Status
		p.rusage = exited.Rusage
		close(p.exited)
	default:
		p.fail(fmt.Errorf("unexpected reply type %d", typ))
	}
}

// hasStarted returns whether the helper has replied to the spawn request. The caller must hold the
// client's mutex.
func (p *Process) hasStarted() bool {
	select {
	case <-p.started:
		return true
	default:
		return false
	}
}

// fail fails the process. If it hasn't been started yet, Spawn returns err, otherwise Wait does.
func (p *Process) fail(err error) {
	p.client.mu.Lock()
	defer p.client.mu.Unlock()

	select {
	case <-p.started:
		p.waitErr = err
		close(p.exited)
	default:
		p.startErr = err
		close(p.started)
	}
}

// killAndReap kills the process group of a process nobody is going to wait for, and reaps it. The
// read loop must never write to the socket itself: the write may block until the helper reads, and
// the helper may itself be blocked writing replies the read loop has to consume.
func (p *Process) killAndReap() {
	go func() {
		_ = p.Signal(syscall.SIGKILL, true)
		p.requestWait()
	}()
}

// Pid returns the PID of the process.
func (p *Process) Pid() int {
	return p.pid
}

// Signal sends sig to the process, or to its process group if group is set. Signals sent after
// the process has been reaped are ignored.
func (p *Process) Signal(sig syscall.Signal, group bool) error {
	return p.client.send(msgSignal, p.id, signalBody{Signal: sig, Group: group})
}

// requestWait asks the helper to reap the process once it has exited.
func (p *Process) requestWait() {
	p.waitOnce.Do(func() {
		// If sending fails, the client is disconnected, which fails the process.
		_ = p.client.send(msgWait, p.id, struct{}{})
	})
}

// Wait waits for the process to exit, reaps it and returns its wait status and resource usage.
// Until Wait is called, the process stays a zombie once it has exited so that its PID and process
// group can't be reused. Wait returns an error if the client has been disconnected before the
// helper reaped the process.
func (p *Process) Wait() (syscall.WaitStatus, *syscall.Rusage, error) {
	p.requestWait()

	<-p.exited
	if p.waitErr != nil {
		return 0, nil, p.waitErr
	}

	return p.status, &p.rusage, nil
}
