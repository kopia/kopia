package server

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/ctxutil"
	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type estimateTaskProgress struct {
	ctrl uitask.Controller
}

func (p estimateTaskProgress) Processing(ctx context.Context, dirname string) {
	p.ctrl.ReportProgressInfo(dirname)
}

func (p estimateTaskProgress) Error(ctx context.Context, dirname string, err error, isIgnored bool) {
	if isIgnored {
		log(ctx).Errorf("ignored error in %v: %v", dirname, err)
	} else {
		log(ctx).Errorf("error in %v: %v", dirname, err)
	}
}

func (p estimateTaskProgress) Stats(ctx context.Context, st *snapshot.Stats, included, excluded snapshotfs.SampleBuckets, excludedDirs []string, final bool) {
	p.ctrl.ReportCounters(map[string]uitask.CounterValue{
		"Bytes":                uitask.BytesCounter(st.TotalFileSize),
		"Files":                uitask.SimpleCounter(int64(st.TotalFileCount)),
		"Directories":          uitask.SimpleCounter(int64(st.TotalDirectoryCount)),
		"Excluded Files":       uitask.SimpleCounter(int64(st.ExcludedFileCount)),
		"Excluded Directories": uitask.SimpleCounter(int64(st.ExcludedDirCount)),
		"Errors":               uitask.ErrorCounter(int64(st.ErrorCount)),
		"Ignored Errors":       uitask.ErrorCounter(int64(st.IgnoredErrorCount)),
	})
}

var _ snapshotfs.EstimateProgress = estimateTaskProgress{}

func (s *Server) handleEstimate(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	var req serverapi.EstimateRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	ctx = ctxutil.Detach(ctx)
	rep := s.rep

	resolvedRoot := filepath.Clean(ospath.ResolveUserFriendlyPath(req.Root, true))

	e, err := localfs.NewEntry(resolvedRoot)
	if err != nil {
		return nil, internalServerError(errors.Wrap(err, "can't get local fs entry"))
	}

	dir, ok := e.(fs.Directory)
	if !ok {
		return nil, internalServerError(errors.Wrap(err, "estimation is only supported on directories"))
	}

	taskIDChan := make(chan string)

	// launch a goroutine that will continue the estimate and can be observed in the Tasks UI.

	// nolint:errcheck
	go s.taskmgr.Run(ctx, "Estimate", resolvedRoot, func(ctx context.Context, ctrl uitask.Controller) error {
		taskIDChan <- ctrl.CurrentTaskID()

		estimatectx, cancel := context.WithCancel(ctx)
		defer cancel()

		ctrl.OnCancel(cancel)

		policyTree, err := policy.TreeForSource(ctx, s.rep, snapshot.SourceInfo{
			Host:     s.rep.ClientOptions().Hostname,
			UserName: s.rep.ClientOptions().Username,
			Path:     resolvedRoot,
		})
		if err != nil {
			return errors.Wrap(err, "unable to get policy tree")
		}

		return snapshotfs.Estimate(estimatectx, rep, dir, policyTree, estimateTaskProgress{ctrl})
	})

	taskID := <-taskIDChan

	task, ok := s.taskmgr.GetTask(taskID)
	if !ok {
		return nil, internalServerError(errors.Errorf("task not found"))
	}

	return task, nil
}
