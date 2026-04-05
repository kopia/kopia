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

func TestDeleteMountRemovesEntryOnSuccess(t *testing.T) {
	s := newTestServer()
	oid, err := object.ParseID("aabb001122334455")
	require.NoError(t, err)

	ctrl := &mockController{mountPath: "Z:", done: make(chan struct{})}
	s.mounts[oid] = ctrl

	// Verify mount exists.
	require.NotNil(t, s.mounts[oid])

	// Simulate successful unmount + delete.
	require.NoError(t, ctrl.Unmount(context.Background()))
	s.deleteMount(oid)

	// Mount should be removed.
	require.Nil(t, s.mounts[oid])
	require.True(t, ctrl.unmounted)
}

func TestDeleteMountRemovesEntryEvenOnUnmountFailure(t *testing.T) {
	s := newTestServer()
	oid, err := object.ParseID("aabb001122334455")
	require.NoError(t, err)

	ctrl := &mockController{
		mountPath:  "Z:",
		unmountErr: context.DeadlineExceeded,
		done:       make(chan struct{}),
	}
	s.mounts[oid] = ctrl

	// Simulate the fixed handleMountDelete behavior:
	// always call deleteMount, even when Unmount fails.
	unmountErr := ctrl.Unmount(context.Background())
	s.deleteMount(oid)

	// Unmount should have been attempted.
	require.True(t, ctrl.unmounted)
	// Error should have been returned.
	require.Error(t, unmountErr)
	// But mount should still be removed from the map.
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
