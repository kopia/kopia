package server

import (
	"context"
	"maps"
	"runtime"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/mount"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

func (s *Server) getMountController(ctx context.Context, rep repo.Repository, oid object.ID, createIfNotFound bool) (mount.Controller, error) {
	s.serverMutex.Lock()
	defer s.serverMutex.Unlock()

	c := s.mounts[oid]
	if c != nil {
		return c, nil
	}

	if !createIfNotFound {
		return nil, nil
	}

	userLog(ctx).Debugf("mount controller for %v not found, starting", oid)

	c, err := mount.Directory(ctx, snapshotfs.DirectoryEntry(rep, oid, nil), "*", s.uiMountOptions(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "unable to mount")
	}

	s.mounts[oid] = c

	return c, nil
}

// uiMountOptions returns mount options for mounts requested through the UI.
// On macOS the user can choose between FUSE (macFUSE) and WebDAV in UI preferences.
func (s *Server) uiMountOptions(ctx context.Context) mount.Options {
	if runtime.GOOS != "darwin" {
		return mount.Options{}
	}

	p, err := getUIPreferencesOrEmpty(s)
	if err != nil {
		userLog(ctx).Warnf("unable to read UI preferences, using default mount backend: %v", err)
		return mount.Options{}
	}

	return mount.Options{
		PreferWebDAV: p.MountBackend == serverapi.MountBackendWebDAV,
	}
}

func (s *Server) listMounts() map[object.ID]mount.Controller {
	s.serverMutex.RLock()
	defer s.serverMutex.RUnlock()

	return maps.Clone(s.mounts)
}

func (s *Server) deleteMount(oid object.ID) {
	s.serverMutex.Lock()
	defer s.serverMutex.Unlock()

	delete(s.mounts, oid)
}

// +checklocks:s.serverMutex
func (s *Server) unmountAllLocked(ctx context.Context) {
	for oid, c := range s.mounts {
		if err := c.Unmount(ctx); err != nil {
			userLog(ctx).Errorf("unable to unmount %v", oid)
		}

		delete(s.mounts, oid)
	}
}
