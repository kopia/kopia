package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

func handleObjectGet(ctx context.Context, rc requestContext) {
	oidstr := rc.muxVar("objectID")

	if !requireUIUser(ctx, rc) {
		http.Error(rc.w, "access denied", http.StatusForbidden)
		return
	}

	oid, err := object.ParseID(oidstr)
	if err != nil {
		http.Error(rc.w, "invalid object id", http.StatusBadRequest)
		return
	}

	obj, err := rc.rep.OpenObject(ctx, oid)
	if errors.Is(err, object.ErrObjectNotFound) {
		http.Error(rc.w, "object not found", http.StatusNotFound)
		return
	}

	if snapshotfs.IsDirectoryID(oid) {
		rc.w.Header().Set("Content-Type", "application/json")
	}

	fname := oid.String()
	if p := rc.queryParam("fname"); p != "" {
		fname = p
		rc.w.Header().Set("Content-Disposition", "attachment; filename=\""+p+"\"")
	}

	mtime := clock.Now()

	if p := rc.queryParam("mtime"); p != "" {
		if m, err := time.Parse(time.RFC3339Nano, p); err == nil {
			mtime = m
		}
	}

	http.ServeContent(rc.w, rc.req, fname, mtime, obj)
}
