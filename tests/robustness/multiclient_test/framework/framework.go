//go:build darwin || (linux && amd64)

// Package framework contains tools to enable multiple clients to connect to a
// central repository server and run robustness tests concurrently.
package framework

import (
	"context"
	"os/exec"

	"github.com/kopia/kopia/tests/robustness"
)

// ClientSnapshotter is an interface that wraps robustness.Snapshotter with
// methods for handling client connections to a server and cleanup.
type ClientSnapshotter interface {
	robustness.Snapshotter
	ConnectClient(fingerprint, user string) error
	DisconnectClient(user string)
	Cleanup()
}

// Server is an interface for a repository server.
type Server interface {
	// Initialize and cleanup the server
	ConnectOrCreateRepo(repoPath string) error
	Cleanup()

	// Handle client authorization
	AuthorizeClient(user string) error
	RemoveClient(user string)

	// Run commands directly on the server repository
	Run(args ...string) (stdout, stderr string, err error)
	RunGC(ctx context.Context, opts map[string]string) error

	// Get information from the server
	ServerCmd() *exec.Cmd
	ServerFingerprint() string
}

// FileWriter is a robustness.FileWriter with the ability to cleanup.
type FileWriter interface {
	robustness.FileWriter
	Cleanup()
}
