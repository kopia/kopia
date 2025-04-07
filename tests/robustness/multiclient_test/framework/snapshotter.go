//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package framework

import (
	"context"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
)

const (
	contentCacheLimitMBFlag  = "--content-cache-size-limit-mb"
	metadataCacheLimitMBFlag = "--metadata-cache-size-limit-mb"
)

// MultiClientSnapshotter manages a set of client Snapshotter instances and
// implements the Snapshotter interface itself. Snapshotter methods must be
// provided with a client-wrapped context so the MultiClientSnapshotter can
// delegate to a specific client Snapshotter.
type MultiClientSnapshotter struct {
	baseDirPath string
	server      Server

	// Map of client ID to ClientSnapshotter and associated lock
	clients map[string]ClientSnapshotter
	mu      sync.RWMutex

	// Function used to generate new ClientSnapshotters
	newClientSnapshotter newClientFn
}

// MultiClientSnapshotter implements robustness.Snapshotter.
var _ robustness.Snapshotter = (*MultiClientSnapshotter)(nil)

type newClientFn func(string) (ClientSnapshotter, error)

// NewMultiClientSnapshotter returns a MultiClientSnapshotter that is
// responsible for delegating Snapshotter method calls to a specific client's
// Snapshotter instance. ConnectOrCreateRepo must be invoked to start the server.
func NewMultiClientSnapshotter(baseDirPath string, f newClientFn) (*MultiClientSnapshotter, error) {
	s, err := snapmeta.NewSnapshotter(baseDirPath)
	if err != nil {
		return nil, err
	}

	return &MultiClientSnapshotter{
		newClientSnapshotter: f,
		server:               s,
		clients:              map[string]ClientSnapshotter{},
		baseDirPath:          baseDirPath,
	}, nil
}

// ConnectOrCreateRepo makes the MultiClientSnapshotter ready for use. It will
// connect to an existing repository if possible or create a new one, and
// start a repository server.
func (mcs *MultiClientSnapshotter) ConnectOrCreateRepo(repoPath string) error {
	if err := mcs.server.ConnectOrCreateRepo(repoPath); err != nil {
		return err
	}

	_, _, err := mcs.server.Run("policy", "set", "--global", "--keep-latest", strconv.Itoa(1<<31-1), "--compression", "s2-default")

	return err
}

// setCacheSizeLimits sets hard size limits for the content and metadata caches
// on an already connected repository.
func (mcs *MultiClientSnapshotter) setCacheSizeLimits(contentLimitSizeMB, metadataLimitSizeMB int) error {
	_, _, err := mcs.server.Run("cache", "set",
		metadataCacheLimitMBFlag, strconv.Itoa(metadataLimitSizeMB),
		contentCacheLimitMBFlag, strconv.Itoa(contentLimitSizeMB))

	return err
}

// ServerCmd returns the server command.
func (mcs *MultiClientSnapshotter) ServerCmd() *exec.Cmd {
	return mcs.server.ServerCmd()
}

// CreateSnapshot delegates to a specific client's Snapshotter.
func (mcs *MultiClientSnapshotter) CreateSnapshot(ctx context.Context, sourceDir string, opts map[string]string) (snapID string, fingerprint []byte, snapStats *robustness.CreateSnapshotStats, err error) {
	ks, err := mcs.createOrGetSnapshotter(ctx)
	if err != nil {
		return "", nil, nil, err
	}

	return ks.CreateSnapshot(ctx, sourceDir, opts)
}

// RestoreSnapshot delegates to a specific client's Snapshotter.
func (mcs *MultiClientSnapshotter) RestoreSnapshot(ctx context.Context, snapID, restoreDir string, opts map[string]string) (fingerprint []byte, err error) {
	ks, err := mcs.createOrGetSnapshotter(ctx)
	if err != nil {
		return nil, err
	}

	return ks.RestoreSnapshot(ctx, snapID, restoreDir, opts)
}

// RestoreSnapshotCompare delegates to a specific client's Snapshotter.
func (mcs *MultiClientSnapshotter) RestoreSnapshotCompare(ctx context.Context, snapID, restoreDir string, validationData []byte, reportOut io.Writer, opts map[string]string) (err error) {
	ks, err := mcs.createOrGetSnapshotter(ctx)
	if err != nil {
		return err
	}

	return ks.RestoreSnapshotCompare(ctx, snapID, restoreDir, validationData, reportOut, opts)
}

// DeleteSnapshot delegates to a specific client's Snapshotter.
func (mcs *MultiClientSnapshotter) DeleteSnapshot(ctx context.Context, snapID string, opts map[string]string) error {
	ks, err := mcs.createOrGetSnapshotter(ctx)
	if err != nil {
		return err
	}

	return ks.DeleteSnapshot(ctx, snapID, opts)
}

// RunGC runs garbage collection on the server repository directly since clients
// are not authorized to do this.
func (mcs *MultiClientSnapshotter) RunGC(ctx context.Context, opts map[string]string) error {
	return mcs.server.RunGC(ctx, opts)
}

// ListSnapshots delegates to a specific client's Snapshotter.
func (mcs *MultiClientSnapshotter) ListSnapshots(ctx context.Context) ([]string, error) {
	ks, err := mcs.createOrGetSnapshotter(ctx)
	if err != nil {
		return nil, err
	}

	return ks.ListSnapshots(ctx)
}

// Cleanup cleans up the server and all remaining clients. It delegates to a
// specific client's Snapshotter Cleanup method, but also disconnects the client
// from the server, removes the client from the server's user list, and removes
// the ClientSnapshotter from MultiClientSnapshotter.
func (mcs *MultiClientSnapshotter) Cleanup() {
	for clientID, s := range mcs.clients {
		s.DisconnectClient(clientID)
		s.Cleanup()
		mcs.server.RemoveClient(clientID)

		delete(mcs.clients, clientID)
	}

	mcs.server.Cleanup()
}

// CleanupClient cleans up a given client. It delegates to the client's
// Snapshotter Cleanup method, but also disconnects the client from the server,
// removes the client from the server's user list, and removes the
// ClientSnapshotter from MultiClientSnapshotter.
func (mcs *MultiClientSnapshotter) CleanupClient(ctx context.Context) {
	c := UnwrapContext(ctx)
	if c == nil {
		log.Println("Context does not contain a client")
		return
	}

	mcs.mu.Lock()
	s := mcs.clients[c.ID]
	delete(mcs.clients, c.ID)
	mcs.mu.Unlock()

	if s == nil {
		return
	}

	s.DisconnectClient(c.ID)
	s.Cleanup()
	mcs.server.RemoveClient(c.ID)
}

// createOrGetSnapshotter gets a client's Snapshotter from the given context if
// possible or creates a new ClientSnapshotter.
func (mcs *MultiClientSnapshotter) createOrGetSnapshotter(ctx context.Context) (robustness.Snapshotter, error) {
	c := UnwrapContext(ctx)
	if c == nil {
		log.Println("Context does not contain a client")
		return nil, robustness.ErrKeyNotFound
	}

	log.Printf("Issuing Kopia command for client: %s\n", c.ID)

	// Get existing ClientSnapshotter if available
	mcs.mu.RLock()
	cs, ok := mcs.clients[c.ID]
	mcs.mu.RUnlock()

	if ok {
		return cs, nil
	}

	// Create new ClientSnapshotter
	clientDir, err := os.MkdirTemp(mcs.baseDirPath, "client-")
	if err != nil {
		return nil, err
	}

	cs, err = mcs.newClientSnapshotter(clientDir)
	if err != nil {
		return nil, err
	}

	// Register client with server and create connection
	mcs.mu.Lock()
	err = mcs.server.AuthorizeClient(c.ID)
	mcs.mu.Unlock()

	if err != nil {
		return nil, err
	}

	if err := cs.ConnectClient(mcs.server.ServerFingerprint(), c.ID); err != nil {
		return nil, err
	}

	// Register new client snapshotter with MultiClientSnapshotter
	mcs.mu.Lock()
	defer mcs.mu.Unlock()

	mcs.clients[c.ID] = cs

	return cs, nil
}

// GetCacheDirInfo runs cache info command to get cache dir path for
// the repository.
func (mcs *MultiClientSnapshotter) GetCacheDirInfo() (stdout, stderr string, err error) {
	stdout, stderr, err = mcs.server.Run("cache", "info", "--path")
	if err == nil {
		// The current output of the cache info command contains a new line
		// at the end of the cache directory path.
		stdout = strings.Trim(stdout, "\n")
	}

	return stdout, stderr, err
}
