//go:build !linux

package spawnhelper

import (
	"errors"

	"gitlab.com/gitlab-org/gitaly/v19/internal/log"
)

// Enable is a no-op as the spawn helper is only supported on Linux.
func Enable() {}

// Prestart is a no-op as the spawn helper is only supported on Linux.
func Prestart(log.Logger) {}

// DefaultPid returns 0 as the spawn helper is only supported on Linux.
func DefaultPid() int {
	return 0
}

// Shutdown is a no-op as the spawn helper is only supported on Linux.
func Shutdown() {}

// Serve returns an error as the spawn helper is only supported on Linux.
func Serve() error {
	return errors.New("spawn helper is only supported on Linux")
}

// ServeIfInvoked is a no-op as the spawn helper is only supported on Linux.
func ServeIfInvoked() {}
