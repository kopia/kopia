package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/mount"
	"github.com/kopia/kopia/repo/object"
)

type mockController struct {
	mountPath  string
	unmountErr error
	unmounted  bool
	done       chan struct{}
}

func (m *mockController) Unmount(_ context.Context) error {
	m.unmounted = true
	return m.unmountErr
}

func (m *mockController) MountPath() string {
	return m.mountPath
}

func (m *mockController) Done() <-chan struct{} {
	return m.done
}

func newTestServer() *Server {
	return &Server{
		mounts: map[object.ID]mount.Controller{},
	}
}

func TestUnmountAndDeleteMountRemovesEntryOnSuccess(t *testing.T) {
	s := newTestServer()
	oid, err := object.ParseID("aabb001122334455")
	require.NoError(t, err)

	ctrl := &mockController{mountPath: "Z:", done: make(chan struct{})}
	s.mounts[oid] = ctrl

	// Drives the same code path as handleMountDelete — a regression that
	// reordered the call (skipped deleteMount on success) would fail here.
	unmountErr := s.unmountAndDeleteMount(context.Background(), oid, ctrl)

	require.NoError(t, unmountErr)
	require.Nil(t, s.mounts[oid])
	require.True(t, ctrl.unmounted)
}

func TestUnmountAndDeleteMountRemovesEntryEvenOnUnmountFailure(t *testing.T) {
	s := newTestServer()
	oid, err := object.ParseID("aabb001122334455")
	require.NoError(t, err)

	ctrl := &mockController{
		mountPath:  "Z:",
		unmountErr: context.DeadlineExceeded,
		done:       make(chan struct{}),
	}
	s.mounts[oid] = ctrl

	// If a future change ever returns early on Unmount error (the original
	// bug), this test fails on the s.mounts[oid] == nil assertion below
	// because deleteMount would be skipped.
	unmountErr := s.unmountAndDeleteMount(context.Background(), oid, ctrl)

	require.True(t, ctrl.unmounted)
	require.Error(t, unmountErr)
	require.Nil(t, s.mounts[oid])
}

func TestGetMountControllerReturnsNilWhenNotFound(t *testing.T) {
	s := newTestServer()
	oid, err := object.ParseID("aabb0011223344556677")
	require.NoError(t, err)

	s.serverMutex.Lock()
	c := s.mounts[oid]
	s.serverMutex.Unlock()

	require.Nil(t, c)
}

func TestDeleteMountIsIdempotent(t *testing.T) {
	s := newTestServer()
	oid, err := object.ParseID("aabb001122334455")
	require.NoError(t, err)

	ctrl := &mockController{mountPath: "Z:", done: make(chan struct{})}
	s.mounts[oid] = ctrl
	s.deleteMount(oid)

	// Second delete should not panic.
	s.deleteMount(oid)

	require.Nil(t, s.mounts[oid])
}

func TestListMountsReturnsClone(t *testing.T) {
	s := newTestServer()
	oid, err := object.ParseID("aabb001122334455")
	require.NoError(t, err)

	ctrl := &mockController{mountPath: "Z:", done: make(chan struct{})}
	s.mounts[oid] = ctrl

	listed := s.listMounts()
	require.Len(t, listed, 1)

	// Deleting from the clone should not affect the server's map.
	delete(listed, oid)
	require.Len(t, s.listMounts(), 1)
}
