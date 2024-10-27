package server

import (
	"archive/zip"
	"context"
	"encoding/json"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

func restoreCounters(s restore.Stats) map[string]uitask.CounterValue {
	return map[string]uitask.CounterValue{
		"Restored Files":       uitask.SimpleCounter(int64(s.RestoredFileCount)),
		"Restored Directories": uitask.SimpleCounter(int64(s.RestoredDirCount)),
		"Restored Symlinks":    uitask.SimpleCounter(int64(s.RestoredSymlinkCount)),
		"Restored Bytes":       uitask.BytesCounter(s.RestoredTotalFileSize),
		"Ignored Errors":       uitask.SimpleCounter(int64(s.IgnoredErrorCount)),
		"Skipped Files":        uitask.SimpleCounter(int64(s.SkippedCount)),
		"Skipped Bytes":        uitask.BytesCounter(s.SkippedTotalFileSize),
	}
}

func handleRestore(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	var req serverapi.RestoreRequest

	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	rep := rc.rep

	if req.Root == "" {
		return nil, requestError(serverapi.ErrorMalformedRequest, "root not specified")
	}

	rootEntry, err := snapshotfs.FilesystemEntryFromIDWithPath(ctx, rep, req.Root, false)
	if err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "invalid root entry")
	}

	var (
		out         restore.Output
		description string
	)

	switch {
	case req.Filesystem != nil:
		if err := req.Filesystem.Init(ctx); err != nil {
			return nil, internalServerError(err)
		}

		out = req.Filesystem
		description = "Destination: " + req.Filesystem.TargetPath

	case req.ZipFile != "":
		f, err := os.Create(req.ZipFile)
		if err != nil {
			return nil, internalServerError(err)
		}

		if req.UncompressedZip {
			out = restore.NewZipOutput(f, zip.Store)
			description = "Uncompressed ZIP File: " + req.ZipFile
		} else {
			out = restore.NewZipOutput(f, zip.Deflate)
			description = "ZIP File: " + req.ZipFile
		}

	case req.TarFile != "":
		f, err := os.Create(req.TarFile)
		if err != nil {
			return nil, internalServerError(err)
		}

		out = restore.NewTarOutput(f)
		description = "TAR File: " + req.TarFile

	default:
		return nil, requestError(serverapi.ErrorMalformedRequest, "output not specified")
	}

	taskIDChan := make(chan string)

	// launch a goroutine that will continue the restore and can be observed in the Tasks UI.

	//nolint:errcheck
	go rc.srv.taskManager().Run(ctx, "Restore", description, func(ctx context.Context, ctrl uitask.Controller) error {
		taskIDChan <- ctrl.CurrentTaskID()

		opt := req.Options

		opt.ProgressCallback = func(ctx context.Context, s restore.Stats) {
			ctrl.ReportCounters(restoreCounters(s))
		}

		cancelChan := make(chan struct{})
		opt.Cancel = cancelChan

		ctrl.OnCancel(func() {
			close(opt.Cancel)
		})

		st, err := restore.Entry(ctx, rep, out, rootEntry, opt)
		if err == nil {
			ctrl.ReportCounters(restoreCounters(st))
		}

		return errors.Wrap(err, "error restoring")
	})

	taskID := <-taskIDChan

	task, ok := rc.srv.taskManager().GetTask(taskID)
	if !ok {
		return nil, internalServerError(errors.New("task not found"))
	}

	return task, nil
}
