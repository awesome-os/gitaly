package command

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
)

func TestCommand_Wait_exitError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc             string
		script           string
		expectedMessage  string
		expectedExitCode int
		expectedSignal   syscall.Signal
	}{
		{
			desc:             "non-zero exit code",
			script:           "exit 3",
			expectedMessage:  "exit status 3",
			expectedExitCode: 3,
		},
		{
			desc:             "terminated by signal",
			script:           "kill -TERM $$",
			expectedMessage:  "signal: terminated",
			expectedExitCode: -1,
			expectedSignal:   syscall.SIGTERM,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			cmd, err := New(testhelper.Context(t), testhelper.SharedLogger(t), []string{"sh", "-c", tc.script})
			require.NoError(t, err)

			err = cmd.Wait()
			require.EqualError(t, err, tc.expectedMessage)

			var exitErr *ExitError
			require.ErrorAs(t, err, &exitErr)
			require.Equal(t, tc.expectedExitCode, exitErr.ExitCode())
			require.Equal(t, tc.expectedSignal, exitErr.Signal())

			exitCode, ok := ExitStatus(err)
			require.True(t, ok)
			require.Equal(t, tc.expectedExitCode, exitCode)

			signal, ok := ExitSignal(err)
			require.Equal(t, tc.expectedSignal != 0, ok)
			require.Equal(t, tc.expectedSignal, signal)

			// Callers inspecting the error returned by exec.Cmd keep working.
			var execExitErr *exec.ExitError
			require.ErrorAs(t, err, &execExitErr)
			require.Equal(t, tc.expectedExitCode, execExitErr.ExitCode())
		})
	}
}

func TestExitError_Error(t *testing.T) {
	t.Parallel()

	require.Equal(t, "exit status 3", (&ExitError{exitCode: 3}).Error())
	require.Equal(t, "signal: killed", (&ExitError{exitCode: -1, signal: syscall.SIGKILL}).Error())
}

func TestExitStatusAndSignal_otherErrors(t *testing.T) {
	t.Parallel()

	for _, err := range []error{nil, errors.New("other"), fmt.Errorf("wrapped: %w", errors.New("other"))} {
		_, ok := ExitStatus(err)
		require.False(t, ok)
		_, ok = ExitSignal(err)
		require.False(t, ok)
	}

	wrapped := fmt.Errorf("wrapped: %w", &ExitError{exitCode: -1, signal: syscall.SIGPIPE})
	exitCode, ok := ExitStatus(wrapped)
	require.True(t, ok)
	require.Equal(t, -1, exitCode)
	signal, ok := ExitSignal(wrapped)
	require.True(t, ok)
	require.Equal(t, syscall.SIGPIPE, signal)
}
