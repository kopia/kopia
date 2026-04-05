package server

import (
	"context"
	"encoding/json"
	"time"

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

	// Always remove the mount from the map, even if unmount fails.
	// A failed unmount leaves a dead controller that blocks future mounts.
	// Use a background context so the unmount is not cancelled by the HTTP request timeout.
	unmountCtx, cancel := context.WithTimeout(context.Background(), mountUnmountTimeout)
	defer cancel()

	unmountErr := c.Unmount(unmountCtx)

	rc.srv.deleteMount(oid)

	if unmountErr != nil {
		return nil, internalServerError(unmountErr)
	}

	return &serverapi.Empty{}, nil
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
