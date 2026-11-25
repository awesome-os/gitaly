package spawnhelper

import (
	"fmt"
	"io"
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v19/internal/log"
)

var testLogger log.Logger

//nolint:gitaly-linters // testhelper imports this package, so the tests can't use testhelper.Run.
func TestMain(m *testing.M) {
	ServeIfInvoked()

	var err error
	if testLogger, err = log.Configure(io.Discard, "text", "panic"); err != nil {
		fmt.Fprintf(os.Stderr, "configure logger: %v\n", err)
		os.Exit(1)
	}

	Enable()
	code := m.Run()
	Shutdown()
	os.Exit(code)
}
