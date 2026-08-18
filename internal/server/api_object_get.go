package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const downloadProgressUpdateInterval = 250 * time.Millisecond

type downloadProgressReader struct {
	object.Reader
	ctx    context.Context
	onRead func(int64)
}

func (r *downloadProgressReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}

	n, err := r.Reader.Read(p)
	if n > 0 {
		r.onRead(int64(n))
	}

	return n, err //nolint:wrapcheck
}

func serveObjectDownload(
	ctx context.Context,
	tasks *uitask.Manager,
	w http.ResponseWriter,
	req *http.Request,
	fname string,
	mtime time.Time,
	obj object.Reader,
) {
	// The response must remain synchronous so the browser can save the streamed
	// file. Running it as a task still makes long downloads observable in the UI.
	_ = tasks.Run(ctx, "Restore", "File: "+fname, func(ctx context.Context, ctrl uitask.Controller) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		ctrl.OnCancel(cancel)

		var (
			downloaded int64
			throttle   timetrack.Throttle
		)

		progress := newRestoreTaskProgress()
		stats := restore.Stats{
			EnqueuedFileCount:     1,
			EnqueuedTotalFileSize: obj.Length(),
		}
		progress.report(ctrl, stats)

		reader := &downloadProgressReader{
			Reader: obj,
			ctx:    ctx,
			onRead: func(n int64) {
				downloaded += n
				stats.RestoredTotalFileSize = downloaded
				if throttle.ShouldOutput(downloadProgressUpdateInterval) {
					progress.report(ctrl, stats)
				}
			},
		}

		http.ServeContent(w, req, fname, mtime, reader)

		if err := ctx.Err(); err != nil {
			return err
		}

		stats.RestoredFileCount = 1
		stats.RestoredTotalFileSize = downloaded
		progress.report(ctrl, stats)

		return nil
	})
}

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
	if err != nil {
		http.Error(rc.w, "unable to open object", http.StatusInternalServerError)
		return
	}
	defer obj.Close() //nolint:errcheck

	isDirectory := snapshotfs.IsDirectoryID(oid)
	if isDirectory {
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

	if !isDirectory && rc.queryParam("fname") != "" {
		serveObjectDownload(ctx, rc.srv.taskManager(), rc.w, rc.req, fname, mtime, obj)
		return
	}

	http.ServeContent(rc.w, rc.req, fname, mtime, obj)
}
