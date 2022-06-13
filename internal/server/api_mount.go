package server

import (
	"context"
	"encoding/json"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo/object"
)

func handleMountCreate(ctx context.Context, rc requestContext) (interface{}, *apiError) {
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

	log(ctx).Debugf("mount for %v => %v", oid, c.MountPath())

	return &serverapi.MountedSnapshot{
		Path: c.MountPath(),
		Root: oid,
	}, nil
}

func handleMountGet(ctx context.Context, rc requestContext) (interface{}, *apiError) {
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

func handleMountDelete(ctx context.Context, rc requestContext) (interface{}, *apiError) {
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

	if err := c.Unmount(ctx); err != nil {
		return nil, internalServerError(err)
	}

	rc.srv.deleteMount(oid)

	return &serverapi.Empty{}, nil
}

func handleMountList(ctx context.Context, rc requestContext) (interface{}, *apiError) {
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
