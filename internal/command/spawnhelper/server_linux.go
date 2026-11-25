package spawnhelper

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// socketFD is the file descriptor the helper's end of the socket pair is passed as.
const socketFD = 3

// Serve runs the spawn helper. It serves requests on the socket passed as file descriptor 3. Once
// the other end of the socket has been closed, it stops serving requests and returns when all of
// its children have exited.
//
// The helper deliberately doesn't use os/exec or os.StartProcess: those hold a pidfd per child
// until it has been waited for, so the helper's file descriptor table would grow with the number of
// running children.
func Serve() error {
	// The helper exits once Gitaly closes its end of the socket. Discard signals instead of
	// dying on them, for example when a supervisor signals the whole cgroup. Unlike
	// signal.Ignore, this doesn't make the children inherit SIG_IGN.
	signal.Notify(make(chan os.Signal, 1), syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	f := os.NewFile(socketFD, "spawn-helper")
	// net.FileConn dups the socket with O_CLOEXEC, so closing the inherited descriptor keeps it
	// from leaking into the children.
	conn, err := net.FileConn(f)
	if closeErr := f.Close(); closeErr != nil {
		return errors.Join(err, fmt.Errorf("close socket: %w", closeErr))
	}
	if err != nil {
		return fmt.Errorf("socket: %w", err)
	}
	defer conn.Close()

	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("unexpected socket type %T", conn)
	}

	return serveConn(unixConn)
}

// serveConn serves requests on conn until Gitaly closes its end, and returns once all children
// have exited. If serving fails otherwise, it kills all children and returns right away.
func serveConn(conn *net.UnixConn) error {
	sigchld := make(chan os.Signal, 1)
	signal.Notify(sigchld, syscall.SIGCHLD)
	defer signal.Stop(sigchld)

	s := &server{
		conn:     conn,
		children: map[uint64]*child{},
		allGone:  make(chan struct{}),
	}

	stopReaper := make(chan struct{})
	defer close(stopReaper)
	go func() {
		for {
			select {
			case <-sigchld:
				s.reap()
			case <-stopReaper:
				return
			}
		}
	}()

	serveErr := s.serve()
	// Make Gitaly notice right away that the helper stopped serving.
	_ = conn.Close()
	s.spawns.Wait()

	if serveErr != nil {
		// Gitaly has already failed the commands and can't signal their processes anymore,
		// so don't leave them running untracked.
		s.killAll()
		return serveErr
	}

	// Children keep running when Gitaly goes away, like children Gitaly spawns directly do.
	// Wait for them so that they are reaped and aren't killed by their parent death signal.
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	s.reap()
	<-s.allGone

	return nil
}

type child struct {
	pid int
	// waitRequested is set once Gitaly has asked to reap the child.
	waitRequested bool
}

type server struct {
	conn *net.UnixConn

	// spawns tracks requests that are being spawned.
	spawns sync.WaitGroup

	// mu guards children. A child is registered until it has been reaped, so its PID and
	// process group can be signalled safely while holding mu. mu must never be held while
	// writing to the socket: the write may block until Gitaly reads, and the serve loop needs mu
	// to keep reading Gitaly's requests.
	mu       sync.Mutex
	children map[uint64]*child
	// closing is set once Gitaly has gone away. All children are reaped as soon as they exit
	// afterwards, and allGone is closed once there are none left.
	closing bool
	allGone chan struct{}
}

func (s *server) serve() error {
	buf := make([]byte, headerSize+MaxRequestSize)
	oob := make([]byte, syscall.CmsgSpace(MaxFiles*4))

	for {
		n, oobn, flags, _, err := s.conn.ReadMsgUnix(buf, oob)
		// Gitaly exiting with replies it hasn't read resets the connection rather than
		// closing it.
		if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || (err == nil && n == 0) {
			return nil
		} else if err != nil {
			return fmt.Errorf("receive: %w", err)
		}

		fds, err := parseRights(oob[:oobn])
		if err != nil {
			return fmt.Errorf("parse rights: %w", err)
		}

		if flags&(syscall.MSG_TRUNC|syscall.MSG_CTRUNC) != 0 {
			closeFDs(fds)
			if _, _, id, _, err := decodeHeader(buf[:n]); err == nil {
				s.reply(msgError, id, errorBody{Errno: syscall.EMSGSIZE, Message: "request truncated"})
			}
			continue
		}

		typ, msgFlags, id, body, err := decodeHeader(buf[:n])
		if err != nil {
			closeFDs(fds)
			continue
		}

		switch typ {
		case msgSpawn:
			// Spawn concurrently so that a slow exec doesn't hold up other spawns, signals
			// or reaping.
			req, err := decodeSpawnBody(body)
			if err != nil {
				closeFDs(fds)
				s.reply(msgError, id, errorBody{Errno: syscall.EINVAL, Message: err.Error()})
				continue
			}

			s.spawns.Add(1)
			go func() {
				defer s.spawns.Done()
				s.spawn(id, msgFlags, req, fds)
			}()
		case msgSignal:
			closeFDs(fds)
			s.signal(id, body)
		case msgWait:
			closeFDs(fds)
			s.wait(id)
		default:
			closeFDs(fds)
			// Don't leave Gitaly waiting for a reply to a request it expects to be handled.
			s.reply(msgError, id, errorBody{Errno: syscall.EINVAL, Message: fmt.Sprintf("unknown message type %d", typ)})
		}
	}
}

func (s *server) spawn(id uint64, flags byte, req spawnBody, fds []int) {
	// Close the child's file descriptors as soon as it has been started so that the helper's
	// file descriptor table doesn't grow with the number of running children.
	received := fds
	closed := false
	closeReceived := func() {
		if !closed {
			closeFDs(received)
			closed = true
		}
	}
	defer closeReceived()

	sys := &syscall.SysProcAttr{
		Setpgid: true,
		// Kill the child if the helper dies, as Gitaly can't track or signal it anymore. The
		// signal fires when the forking thread exits, and the helper never locks goroutines
		// to threads, so its threads live as long as the helper.
		Pdeathsig: syscall.SIGKILL,
	}
	if flags&flagCgroupFD != 0 {
		if len(fds) == 0 {
			s.reply(msgError, id, errorBody{Errno: syscall.EINVAL, Message: "missing cgroup file descriptor"})
			return
		}

		sys.UseCgroupFD = true
		sys.CgroupFD = fds[len(fds)-1]
		fds = fds[:len(fds)-1]
	}

	files := make([]uintptr, len(fds))
	for i, fd := range fds {
		files[i] = uintptr(fd)
	}

	// syscall.ForkExec reaps the child itself if exec fails. Children that are not registered
	// are never reaped by the helper, so the two can't race.
	pid, err := syscall.ForkExec(req.Path, req.Argv, &syscall.ProcAttr{
		Dir:   req.Dir,
		Env:   req.Env,
		Files: files,
		Sys:   sys,
	})
	closeReceived()
	if err != nil {
		reply := errorBody{Message: err.Error()}
		if errors.As(err, &reply.Errno) {
			reply.Message = ""
		}
		s.reply(msgError, id, reply)
		return
	}

	s.mu.Lock()
	s.children[id] = &child{pid: pid}
	s.mu.Unlock()

	// Gitaly only asks to reap the child once it has received this reply, so the reply can't
	// race with the exit being reported.
	s.reply(msgStarted, id, startedBody{Pid: pid})
}

func (s *server) signal(id uint64, body []byte) {
	var req signalBody
	if err := json.Unmarshal(body, &req); err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	c, ok := s.children[id]
	if !ok {
		// The child has already been reaped, so its PID may have been reused.
		return
	}

	pid := c.pid
	if req.Group {
		pid = -pid
	}

	_ = syscall.Kill(pid, req.Signal)
}

func (s *server) wait(id uint64) {
	s.mu.Lock()
	c, ok := s.children[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	c.waitRequested = true

	// The child may have exited already, in which case there won't be another SIGCHLD. Only
	// try this child so that the serve loop doesn't scan all of them on every request.
	exited, reaped := s.tryReap(id, c)
	s.mu.Unlock()

	if reaped {
		s.reply(msgExited, id, exited)
	}
}

// reap reaps all children that have exited and that Gitaly has asked to reap, or all exited
// children once Gitaly has gone away. A child that has exited but hasn't been asked for stays a
// zombie, which keeps its PID and process group reserved.
func (s *server) reap() {
	type exit struct {
		id   uint64
		body exitedBody
	}
	var exits []exit
	defer func() {
		for _, e := range exits {
			s.reply(msgExited, e.id, e.body)
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	for id, c := range s.children {
		if !c.waitRequested && !s.closing {
			continue
		}

		body, reaped := s.tryReap(id, c)
		if reaped && !s.closing {
			exits = append(exits, exit{id: id, body: body})
		}
	}

	if s.closing && len(s.children) == 0 {
		select {
		case <-s.allGone:
		default:
			close(s.allGone)
		}
	}
}

// tryReap reaps the child if it has exited and unregisters it. It must be called with mu held.
func (s *server) tryReap(id uint64, c *child) (exitedBody, bool) {
	var body exitedBody
	if wpid, err := syscall.Wait4(c.pid, &body.Status, syscall.WNOHANG, &body.Rusage); err != nil || wpid != c.pid {
		return exitedBody{}, false
	}
	delete(s.children, id)

	return body, true
}

// killAll kills the process groups of all children and reaps them.
func (s *server) killAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, c := range s.children {
		_ = syscall.Kill(-c.pid, syscall.SIGKILL)
		var status syscall.WaitStatus
		for {
			if _, err := syscall.Wait4(c.pid, &status, 0, nil); !errors.Is(err, syscall.EINTR) {
				break
			}
		}
		delete(s.children, id)
	}
}

func (s *server) reply(typ messageType, id uint64, body any) {
	msg, err := encodeMessage(typ, id, body)
	if err != nil {
		return
	}

	if len(msg) > maxReplySize {
		msg, err = encodeMessage(msgError, id, errorBody{Errno: syscall.E2BIG, Message: "error message too long"})
		if err != nil {
			return
		}
	}

	// A failed write means Gitaly has gone away, which the read loop notices as well.
	_, _ = s.conn.Write(msg)
}

func parseRights(oob []byte) ([]int, error) {
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, err
	}

	var fds []int
	for _, msg := range msgs {
		rights, err := syscall.ParseUnixRights(&msg)
		if err != nil {
			closeFDs(fds)
			return nil, err
		}
		fds = append(fds, rights...)
	}

	return fds, nil
}

func closeFDs(fds []int) {
	for _, fd := range fds {
		_ = syscall.Close(fd)
	}
}
