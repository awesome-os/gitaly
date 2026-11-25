package spawnhelper

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.com/gitlab-org/gitaly/v19/internal/log"
)

// startRetryInterval is how long to wait before starting the helper again after starting it
// failed or it exited unexpectedly. Every attempt forks Gitaly with its full file descriptor table.
const startRetryInterval = time.Minute

var defaultHelper struct {
	// client is the running helper. It is read without holding mu so that spawning doesn't
	// contend on it, and only changed while holding mu.
	client atomic.Pointer[Client]

	mu      sync.Mutex
	enabled bool
	// starting is set while the helper is being started. mu isn't held while starting, as that
	// forks Gitaly with its full file descriptor table.
	starting    bool
	lastFailure time.Time
	// closing tracks helpers that are being closed in the background, so that Shutdown can
	// wait for them too.
	closing sync.WaitGroup
}

// Enable makes Get start the spawn helper by executing the running binary as the Subcommand.
func Enable() {
	defaultHelper.mu.Lock()
	defer defaultHelper.mu.Unlock()

	defaultHelper.enabled = true
	defaultHelper.lastFailure = time.Time{}
}

// Get returns the running spawn helper. It starts the helper if it isn't running yet, and again at
// most once per startRetryInterval after starting it failed or it exited. It returns nil if the
// helper isn't enabled or running, or is being started concurrently, in which case processes must
// be spawned directly.
func Get(logger log.Logger) *Client {
	if client := defaultHelper.client.Load(); client != nil && !client.Disconnected() {
		return client
	}

	defaultHelper.mu.Lock()
	if !defaultHelper.enabled {
		defaultHelper.mu.Unlock()
		return nil
	}

	if client := defaultHelper.client.Load(); client != nil {
		if !client.Disconnected() {
			defaultHelper.mu.Unlock()
			return client
		}

		// Only Shutdown stops the helper on purpose, so it has crashed or failed to
		// receive a request. Reap it in the background, as it may only exit once its
		// remaining children have.
		logger.Warn("spawn helper disconnected unexpectedly, spawning commands directly")
		defaultHelper.closing.Go(func() { _ = client.Close() })
		defaultHelper.client.Store(nil)
		defaultHelper.lastFailure = time.Now()
	}

	if defaultHelper.starting || time.Since(defaultHelper.lastFailure) < startRetryInterval {
		defaultHelper.mu.Unlock()
		return nil
	}
	defaultHelper.starting = true
	defaultHelper.mu.Unlock()

	client, err := Start()

	defaultHelper.mu.Lock()
	defer defaultHelper.mu.Unlock()

	defaultHelper.starting = false
	if err != nil {
		defaultHelper.lastFailure = time.Now()
		logger.WithError(err).Warn("failed to start spawn helper, spawning commands directly")
		return nil
	}

	if !defaultHelper.enabled {
		// Shutdown has been called while starting.
		defaultHelper.closing.Go(func() { _ = client.Close() })
		return nil
	}

	defaultHelper.client.Store(client)
	return client
}

// Prestart starts the spawn helper if it is enabled, so that Gitaly forks it while it holds few file
// descriptors rather than on first use.
func Prestart(logger log.Logger) {
	Get(logger)
}

// DefaultPid returns the PID of the spawn helper returned by Get, or 0 if it isn't running.
func DefaultPid() int {
	if client := defaultHelper.client.Load(); client != nil {
		return client.Pid()
	}

	return 0
}

// Shutdown stops the spawn helper if it is running and waits for it to exit, which it does once
// its children have exited. Processes are spawned directly afterwards.
func Shutdown() {
	defaultHelper.mu.Lock()
	defaultHelper.enabled = false
	client := defaultHelper.client.Swap(nil)
	defaultHelper.mu.Unlock()

	if client != nil {
		_ = client.Close()
	}
	defaultHelper.closing.Wait()
}

// ServeIfInvoked runs the spawn helper and exits if the running binary has been executed as the
// spawn helper by a call to Enable. Test binaries that enable the spawn helper must call it first
// thing in TestMain.
func ServeIfInvoked() {
	if len(os.Args) != 2 || os.Args[1] != Subcommand {
		return
	}

	if err := Serve(); err != nil {
		fmt.Fprintf(os.Stderr, "spawn helper: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}
