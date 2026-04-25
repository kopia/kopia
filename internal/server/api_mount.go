package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kopia/kopia/internal/mount"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo/object"
)

const mountUnmountTimeout = 30 * time.Second

func handleMountCreate(ctx context.Context, rc requestContext) (any, *apiError) {
	req := &serverapi.MountSnapshotRequest{}
	if err := json.Unmarshal(rc.body, req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	oid, err := object.ParseID(req.Root)
	if err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "unable to parse OID")
	}

	c, err := rc.srv.getMountController(ctx, rc.rep, oid, true)
	if err != nil {
		return nil, internalServerError(err)
	}

	userLog(ctx).Debugf("mount for %v => %v", oid, c.MountPath())

	return &serverapi.MountedSnapshot{
		Path: c.MountPath(),
		Root: oid,
	}, nil
}

func handleMountGet(ctx context.Context, rc requestContext) (any, *apiError) {
	oid, err := object.ParseID(rc.muxVar("rootObjectID"))
	if err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "invalid root object ID")
	}

	c, err := rc.srv.getMountController(ctx, rc.rep, oid, false)
	if err != nil {
		return nil, internalServerError(err)
	}

	if c == nil {
		return nil, notFoundError("mount point not found")
	}

	return &serverapi.MountedSnapshot{
		Path: c.MountPath(),
		Root: oid,
	}, nil
}

func handleMountDelete(ctx context.Context, rc requestContext) (any, *apiError) {
	oid, err := object.ParseID(rc.muxVar("rootObjectID"))
	if err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "invalid root object ID")
	}

	c, err := rc.srv.getMountController(ctx, rc.rep, oid, false)
	if err != nil {
		return nil, internalServerError(err)
	}

	if c == nil {
		return nil, notFoundError("mount point not found")
	}

	if unmountErr := rc.srv.unmountAndDeleteMount(ctx, oid, c); unmountErr != nil {
		return nil, internalServerError(unmountErr)
	}

	return &serverapi.Empty{}, nil
}

// unmountAndDeleteMount calls Unmount on the controller and unconditionally
// removes the mount from s.mounts, even if Unmount fails. A failed unmount
// would otherwise leave a dead controller in the map and block future mounts
// for the same OID until server restart.
//
// The unmount uses context.WithoutCancel(ctx) so request-scoped values
// (auth/logging/tracing) survive but the unmount can't be aborted by the
// HTTP request's cancellation/timeout.
func (s *Server) unmountAndDeleteMount(ctx context.Context, oid object.ID, c mount.Controller) error {
	unmountCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mountUnmountTimeout)
	defer cancel()

	unmountErr := c.Unmount(unmountCtx)
	s.deleteMount(oid)

	return unmountErr
}

func handleMountList(_ context.Context, rc requestContext) (any, *apiError) {
	res := &serverapi.MountedSnapshots{
		Items: []*serverapi.MountedSnapshot{},
	}

	for oid, c := range rc.srv.listMounts() {
		res.Items = append(res.Items, &serverapi.MountedSnapshot{
			Path: c.MountPath(),
			Root: oid,
		})
	}

	return res, nil
}
