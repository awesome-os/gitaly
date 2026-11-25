package command

import (
	"os/exec"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
)

func TestExecProcess_Wait(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc             string
		script           string
		expectedExitCode int
		expectedSignal   syscall.Signal
	}{
		{
			desc:             "success",
			script:           "exit 0",
			expectedExitCode: 0,
		},
		{
			desc:             "non-zero exit code",
			script:           "exit 3",
			expectedExitCode: 3,
		},
		{
			desc:             "terminated by signal",
			script:           "kill -TERM $$",
			expectedExitCode: -1,
			expectedSignal:   syscall.SIGTERM,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			cmd := exec.CommandContext(testhelper.Context(t), "sh", "-c", tc.script)
			require.NoError(t, cmd.Start())

			state, err := execProcess{cmd: cmd}.Wait()
			if tc.expectedExitCode == 0 {
				require.NoError(t, err)
			} else {
				var exitErr *ExitError
				require.ErrorAs(t, err, &exitErr)
				require.Equal(t, tc.expectedExitCode, exitErr.ExitCode())
				require.Equal(t, tc.expectedSignal, exitErr.Signal())

				// The original error is still in the chain.
				var execExitErr *exec.ExitError
				require.ErrorAs(t, err, &execExitErr)
			}

			require.Equal(t, cmd.Process.Pid, state.pid)
			require.Equal(t, tc.expectedExitCode, state.exitCode)
			require.Equal(t, tc.expectedSignal, state.signal)
			require.NotNil(t, state.rusage)
		})
	}
}

func TestExecProcess_Terminate(t *testing.T) {
	t.Parallel()

	cmd := exec.CommandContext(testhelper.Context(t), "sh", "-c", "sleep 60")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())

	process := execProcess{cmd: cmd}
	require.NoError(t, process.Terminate())

	state, err := process.Wait()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, -1, state.exitCode)
	require.Equal(t, syscall.SIGTERM, state.signal)
}

func TestExecProcess_WaitWithoutReaping(t *testing.T) {
	t.Parallel()

	cmd := exec.CommandContext(testhelper.Context(t), "sh", "-c", "exit 0")
	require.NoError(t, cmd.Start())

	// Reap the process behind exec.Cmd's back so that its Wait fails.
	_, err := syscall.Wait4(cmd.Process.Pid, nil, 0, nil)
	require.NoError(t, err)

	state, err := execProcess{cmd: cmd}.Wait()
	require.ErrorIs(t, err, syscall.ECHILD)
	require.Nil(t, cmd.ProcessState)
	require.Equal(t, cmd.Process.Pid, state.pid)
	require.Equal(t, -1, state.exitCode)
}
