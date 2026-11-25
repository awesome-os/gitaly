package gittest

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/command"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
)

// ExecConfig contains configuration for ExecOpts.
type ExecConfig struct {
	// Stdin sets up stdin of the spawned command.
	Stdin io.Reader
	// Stdout sets up stdout of the spawned command. Note that `ExecOpts()` will not return any
	// output anymore if this field is set.
	Stdout io.Writer
	// Stderr sets up stderr of the spawned command. If this field is not set, the error is
	// dumped to test logs.
	Stderr io.Writer
	// Env contains environment variables that should be appended to the spawned command's
	// environment.
	Env []string
	// ExpectedExitCode is used to check the resulting exit code of the command. This can be used in case a command
	// is expected to return an error code.
	ExpectedExitCode int
	// MVCC, when set, runs the command under a context carrying the MVCC marker so the command
	// factory selects the MVCC-enabled Git binary.
	MVCC bool
}

// Exec runs a git command and returns the standard output, or fails.
func Exec(tb testing.TB, cfg config.Cfg, args ...string) []byte {
	tb.Helper()

	return ExecOpts(tb, cfg, ExecConfig{}, args...)
}

// ExecOpts runs a git command with the given configuration.
func ExecOpts(tb testing.TB, cfg config.Cfg, execCfg ExecConfig, args ...string) []byte {
	tb.Helper()

	cmd := createCommand(tb, cfg, execCfg, args...)

	// If the caller has passed an stdout writer to us we cannot use `cmd.Output()`. So
	// we detect this case and call `cmd.Run()` instead.
	if execCfg.Stdout != nil {
		if err := cmd.Run(); err != nil {
			handleExecErr(tb, cfg, execCfg, args, err)
		}

		return nil
	}

	output, err := cmd.Output()
	if err != nil {
		handleExecErr(tb, cfg, execCfg, args, err)
	}

	return output
}

// repoPathFromArgs returns the repository path passed to Git via the "-C <path>" argument, if
// present. Nearly all gittest helpers scope their commands to a repository this way. Otherwise it
// returns the first argument that is a repository, like the source of a clone or the remote of
// ls-remote.
//
// It trusts the arguments to name the repository, so it is only safe in tests: an argument that
// happens to be a repository path, like a revision or a remote URL, would mark the context for
// MVCC if that repository uses it.
func repoPathFromArgs(tb testing.TB, args []string) (string, bool) {
	tb.Helper()

	for i, arg := range args {
		if arg == "-C" && i+1 < len(args) {
			return args[i+1], true
		}
	}

	// The first argument that looks like a repository wins. Pass ExecConfig.MVCC when a command
	// needs the MVCC binary for something else.
	for _, arg := range args {
		if isRepository(arg) {
			return arg, true
		}
	}

	return "", false
}

// isRepository reports whether path looks like a bare repository: a directory with a config file
// and HEAD.
func isRepository(path string) bool {
	if !filepath.IsAbs(path) {
		return false
	}

	for _, name := range []string{"config", "HEAD"} {
		if info, err := os.Stat(filepath.Join(path, name)); err != nil || !info.Mode().IsRegular() {
			return false
		}
	}

	return true
}

func handleExecErr(tb testing.TB, cfg config.Cfg, execCfg ExecConfig, args []string, err error) {
	var stderr []byte
	var exitErr *exec.ExitError //nolint:forbidigo // The error stems from a plain exec.Cmd.
	if errors.As(err, &exitErr) {
		if execCfg.ExpectedExitCode == exitErr.ExitCode() {
			return
		}
		stderr = exitErr.Stderr
	}
	tb.Log(args)
	if len(stderr) > 0 {
		tb.Logf("%s\n", stderr)
	}
	tb.Fatal(err)
}

// NewCommand creates a new Git command ready for execution.
func NewCommand(tb testing.TB, cfg config.Cfg, args ...string) *exec.Cmd {
	tb.Helper()
	return createCommand(tb, cfg, ExecConfig{}, args...)
}

func createCommand(tb testing.TB, cfg config.Cfg, execCfg ExecConfig, args ...string) *exec.Cmd {
	tb.Helper()

	ctx := testhelper.Context(tb)

	factory := NewCommandFactory(tb, cfg)

	// When the command targets a repository (see repoPathFromArgs) and MVCC was not explicitly
	// requested, detect from disk whether the repository uses MVCC so the MVCC binary is selected
	// for it. This mirrors how the MVCC middleware marks the context in production and lets helpers
	// such as WriteCommit operate on MVCC repositories transparently.
	if execCfg.MVCC {
		ctx = gitcmd.ContextWithMVCC(ctx)
	} else if repoPath, ok := repoPathFromArgs(tb, args); ok {
		ctx = contextWithDetectedMVCC(ctx, repoPath)
	}

	execEnv := factory.GetExecutionEnvironment(ctx)

	gitConfig, err := factory.GlobalConfiguration(ctx)
	require.NoError(tb, err)
	gitConfig = append(gitConfig,
		gitcmd.ConfigPair{Key: "init.defaultBranch", Value: git.DefaultBranch},
		gitcmd.ConfigPair{Key: "init.templateDir", Value: ""},
		gitcmd.ConfigPair{Key: "user.name", Value: "Your Name"},
		gitcmd.ConfigPair{Key: "user.email", Value: "you@example.com"},
	)

	cmd := exec.CommandContext(ctx, execEnv.BinaryPath, args...)
	cmd.Env = command.AllowedEnvironment(os.Environ())
	cmd.Env = append(cmd.Env, "GIT_AUTHOR_DATE=1572776879 +0100", "GIT_COMMITTER_DATE=1572776879 +0100")
	cmd.Env = append(cmd.Env, gitcmd.ConfigPairsToGitEnvironment(gitConfig)...)
	cmd.Env = append(cmd.Env, execEnv.EnvironmentVariables...)
	cmd.Env = append(cmd.Env, execCfg.Env...)

	// Here we need to read directly from environment variables
	// because the context is not propagated from the tests.
	if val := os.Getenv("GIT_MVCC_MANIFEST"); val != "" {
		cmd.Env = append(cmd.Env, "GIT_MVCC_MANIFEST="+val)
	}

	if val := os.Getenv("GIT_MVCC_MANIFEST_PATH"); val != "" {
		cmd.Env = append(cmd.Env, "GIT_MVCC_MANIFEST_PATH="+val)
	}

	cmd.Stdout = execCfg.Stdout
	cmd.Stdin = execCfg.Stdin
	cmd.Stderr = execCfg.Stderr

	return cmd
}

var _ gitcmd.RepositoryExecutor = RepositoryPathExecutor{}

// RepositoryPathExecutor is a `gitcmd.RepositoryExecutor` that knows to execute commands in a Git repository identified by
// its absolute path.
type RepositoryPathExecutor struct {
	repoPath string
	storage.Repository
	factory gitcmd.CommandFactory
}

// NewRepositoryPathExecutor creates a new RepositoryPathExecutor for the given repository.
func NewRepositoryPathExecutor(tb testing.TB, cfg config.Cfg, repoPath string) RepositoryPathExecutor {
	relativePath, err := filepath.Rel(cfg.Storages[0].Path, repoPath)
	require.NoError(tb, err)

	return RepositoryPathExecutor{
		repoPath: repoPath,
		Repository: &gitalypb.Repository{
			StorageName:  cfg.Storages[0].Name,
			RelativePath: relativePath,
		},
		factory: NewCommandFactory(tb, cfg),
	}
}

// Exec executes a command in the given repository.
func (e RepositoryPathExecutor) Exec(ctx context.Context, cmd gitcmd.Command, opts ...gitcmd.CmdOpt) (*command.Command, error) {
	// Detect from disk whether the repository uses MVCC and mark the context, mirroring how the
	// MVCC middleware marks it in production. This ensures the command factory picks the MVCC
	// binary for MVCC repositories without any global test switch.
	return e.factory.New(contextWithDetectedMVCC(ctx, e.repoPath), e.Repository, cmd, opts...)
}

// ExecAndWait executes a command in the given repository and waits for it to exit.
func (e RepositoryPathExecutor) ExecAndWait(ctx context.Context, cmd gitcmd.Command, opts ...gitcmd.CmdOpt) error {
	command, err := e.Exec(ctx, cmd, opts...)
	if err != nil {
		return err
	}

	return command.Wait()
}

// GitVersion determines the Git version used by the executor.
func (e RepositoryPathExecutor) GitVersion(ctx context.Context) (string, error) {
	return e.factory.GitVersion(ctx)
}

// ObjectHash determines the object hash used by the repository.
func (e RepositoryPathExecutor) ObjectHash(ctx context.Context) (git.ObjectHash, error) {
	return gitcmd.DetectObjectHash(ctx, e.repoPath)
}

// ReferenceBackend detects the reference backend used by this repository.
func (e RepositoryPathExecutor) ReferenceBackend(ctx context.Context) (git.ReferenceBackend, error) {
	return gitcmd.DetectReferenceBackend(ctx, e.repoPath)
}
