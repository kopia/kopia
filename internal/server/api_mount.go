package server

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/kopia/kopia/internal/mount"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

func (s *Server) handleMountCreate(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	req := &serverapi.MountSnapshotRequest{}
	if err := json.Unmarshal(body, req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	oid, err := object.ParseID(req.Root)
	if err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "unable to parse OID")
	}

	var c mount.Controller

	v, ok := s.mounts.Load(oid)
	if !ok {
		log(ctx).Debugf("mount controller for %v not found, starting", oid)

		var err error
		c, err = mount.Directory(ctx, snapshotfs.DirectoryEntry(s.rep, oid, nil), "*", mount.Options{})

		if err != nil {
			return nil, internalServerError(err)
		}

		if actual, loaded := s.mounts.LoadOrStore(oid, c); loaded {
			c.Unmount(ctx)                // nolint:errcheck
			c = actual.(mount.Controller) // nolint:forcetypeassert
		}
	} else {
		c = v.(mount.Controller) // nolint:forcetypeassert
	}

	log(ctx).Debugf("mount for %v => %v", oid, c.MountPath())

	return &serverapi.MountedSnapshot{
		Path: c.MountPath(),
		Root: oid,
	}, nil
}

func (s *Server) handleMountGet(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	oid := object.ID(mux.Vars(r)["rootObjectID"])

	v, ok := s.mounts.Load(oid)
	if !ok {
		return nil, notFoundError("mount point not found")
	}

	c := v.(mount.Controller) // nolint:forcetypeassert

	return &serverapi.MountedSnapshot{
		Path: c.MountPath(),
		Root: oid,
	}, nil
}

func (s *Server) handleMountDelete(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	oid := object.ID(mux.Vars(r)["rootObjectID"])

	v, ok := s.mounts.Load(oid)
	if !ok {
		return nil, notFoundError("mount point not found")
	}

	c := v.(mount.Controller) // nolint:forcetypeassert

	if err := c.Unmount(ctx); err != nil {
		return nil, internalServerError(err)
	}

	s.mounts.Delete(oid)

	return &serverapi.Empty{}, nil
}

func (s *Server) handleMountList(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	res := &serverapi.MountedSnapshots{
		Items: []*serverapi.MountedSnapshot{},
	}

	s.mounts.Range(func(key, val interface{}) bool {
		oid := key.(object.ID)      // nolint:forcetypeassert
		c := val.(mount.Controller) // nolint:forcetypeassert

		res.Items = append(res.Items, &serverapi.MountedSnapshot{
			Path: c.MountPath(),
			Root: oid,
		})
		return true
	})

	return res, nil
}

func (s *Server) unmountAll(ctx context.Context) {
	s.mounts.Range(func(key, val interface{}) bool {
		c := val.(mount.Controller) // nolint:forcetypeassert

		log(ctx).Debugf("unmounting %v from %v", key, c.MountPath())

		if err := c.Unmount(ctx); err != nil {
			log(ctx).Errorf("unable to unmount %v", key)
		}

		s.mounts.Delete(key)
		return true
	})
}
