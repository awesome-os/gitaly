package command

import (
	"errors"
	"os/exec"
	"strconv"
	"syscall"
)

// ExitError is returned by Wait if the process didn't exit successfully. Inspect it with
// ExitStatus and ExitSignal instead of *exec.ExitError, which only exists for processes that are
// direct children of Gitaly.
type ExitError struct {
	exitCode int
	signal   syscall.Signal
	// err is the error wrapping *exec.ExitError the process' exit was reported with, if any.
	err error
}

// Error returns the same message as exec.ExitError.
func (e *ExitError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}

	if e.signal != 0 {
		return "signal: " + e.signal.String()
	}

	return "exit status " + strconv.Itoa(e.exitCode)
}

// Unwrap returns the error wrapping *exec.ExitError the process' exit was reported with, if any.
func (e *ExitError) Unwrap() error {
	return e.err
}

// ExitCode returns the exit code of the process, or -1 if it was terminated by a signal.
func (e *ExitError) ExitCode() int {
	return e.exitCode
}

// Signal returns the signal that terminated the process, or zero if it exited.
func (e *ExitError) Signal() syscall.Signal {
	return e.signal
}

// ExitStatus will return the exit-code from an error returned by Wait().
func ExitStatus(err error) (int, bool) {
	var exitErr *ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), true
	}

	var execExitErr *exec.ExitError
	if errors.As(err, &execExitErr) {
		return execExitErr.ExitCode(), true
	}

	return 0, false
}

// ExitSignal returns the signal that terminated the process from an error returned by Wait(). It
// returns false if the error doesn't stem from a process terminated by a signal.
func ExitSignal(err error) (syscall.Signal, bool) {
	var exitErr *ExitError
	if errors.As(err, &exitErr) {
		return exitErr.Signal(), exitErr.Signal() != 0
	}

	var execExitErr *exec.ExitError
	if errors.As(err, &execExitErr) {
		if status, ok := execExitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			return status.Signal(), true
		}
	}

	return 0, false
}
