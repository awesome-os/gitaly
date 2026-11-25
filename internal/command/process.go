package command

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// process is a started child process. It decouples Command from exec.Cmd's Process and
// ProcessState so that processes not spawned by Gitaly itself can be tracked the same way.
type process interface {
	// Pid returns the process ID.
	Pid() int
	// Terminate sends SIGTERM to the process group of the process.
	Terminate() error
	// Wait waits for the process to exit and returns its state. The state is never nil, even
	// if waiting failed, as Command reads it unconditionally once the process has been started.
	// If the process didn't exit successfully, the error is an *ExitError. Other errors, like
	// failing to wait for the process, are returned as they are.
	Wait() (*processState, error)
}

// processState is the state of a reaped process.
type processState struct {
	pid int
	// exitCode is -1 if the process was terminated by a signal.
	exitCode   int
	signal     syscall.Signal
	userTime   time.Duration
	systemTime time.Duration
	// rusage is nil if resource usage is unavailable.
	rusage *syscall.Rusage
}

func newProcessState(state *os.ProcessState) *processState {
	ps := &processState{
		pid:        state.Pid(),
		exitCode:   state.ExitCode(),
		userTime:   state.UserTime(),
		systemTime: state.SystemTime(),
	}

	if waitStatus, ok := state.Sys().(syscall.WaitStatus); ok && waitStatus.Signaled() {
		ps.signal = waitStatus.Signal()
	}

	if rusage, ok := state.SysUsage().(*syscall.Rusage); ok {
		ps.rusage = rusage
	}

	return ps
}

// execProcess is a process spawned directly by Gitaly via exec.Cmd.
type execProcess struct {
	cmd *exec.Cmd
}

func (p execProcess) Pid() int {
	return p.cmd.Process.Pid
}

func (p execProcess) Terminate() error {
	if p.cmd.Process.Pid > 0 {
		return syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
	}

	return nil
}

func (p execProcess) Wait() (*processState, error) {
	err := p.cmd.Wait()
	if p.cmd.ProcessState == nil {
		// Waiting failed without reaping the process, so there is no state to report.
		return &processState{pid: p.Pid(), exitCode: -1}, err
	}

	state := newProcessState(p.cmd.ProcessState)

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		err = &ExitError{exitCode: state.exitCode, signal: state.signal, err: err}
	}

	return state, err
}
