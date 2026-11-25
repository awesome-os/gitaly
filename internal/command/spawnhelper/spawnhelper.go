// Package spawnhelper spawns processes on Gitaly's behalf from a long-lived helper process.
//
// Forking copies the parent's whole file descriptor table into the child, and exec then closes
// every close-on-exec entry again one by one. Gitaly holds tens of thousands of file descriptors,
// so every spawn pays for that. The helper only holds a handful of file descriptors, so spawns
// through it cost the same no matter how many file descriptors Gitaly holds.
//
// Gitaly and the helper talk over a SOCK_SEQPACKET socket pair. Gitaly sends the child's file
// descriptors with SCM_RIGHTS and the helper reports the PID and, once reaped, the wait status and
// resource usage of the child.
package spawnhelper

import "errors"

// Subcommand is the name of the gitaly subcommand that runs the spawn helper.
const Subcommand = "spawn-helper"

// ErrUnavailable is returned by Spawn if the spawn request couldn't be sent to the helper. Nothing
// has been spawned in that case, so the process can be spawned directly instead.
var ErrUnavailable = errors.New("spawn helper unavailable")
