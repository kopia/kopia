package server

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/internal/units"
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
	_ = excludedDirs
	_ = final

	p.ctrl.ReportCounters(map[string]uitask.CounterValue{
		"Bytes":                uitask.BytesCounter(atomic.LoadInt64(&st.TotalFileSize)),
		"Excluded Bytes":       uitask.BytesCounter(atomic.LoadInt64(&st.ExcludedTotalFileSize)),
		"Files":                uitask.SimpleCounter(int64(atomic.LoadInt32(&st.TotalFileCount))),
		"Directories":          uitask.SimpleCounter(int64(atomic.LoadInt32(&st.TotalDirectoryCount))),
		"Excluded Files":       uitask.SimpleCounter(int64(atomic.LoadInt32(&st.ExcludedFileCount))),
		"Excluded Directories": uitask.SimpleCounter(int64(atomic.LoadInt32(&st.ExcludedDirCount))),
		"Errors":               uitask.ErrorCounter(int64(atomic.LoadInt32(&st.ErrorCount))),
		"Ignored Errors":       uitask.ErrorCounter(int64(atomic.LoadInt32(&st.IgnoredErrorCount))),
	})

	if final {
		logBucketSamples(ctx, included, "Included", false)
		logBucketSamples(ctx, excluded, "Excluded", true)
	}
}

func logBucketSamples(ctx context.Context, buckets snapshotfs.SampleBuckets, prefix string, showExamples bool) {
	hasAny := false

	for i, bucket := range buckets {
		if bucket.Count == 0 {
			continue
		}

		var sizeRange string

		if i == 0 {
			sizeRange = fmt.Sprintf("< %-6v",
				units.BytesString(bucket.MinSize))
		} else {
			sizeRange = fmt.Sprintf("%-6v...%6v",
				units.BytesString(bucket.MinSize),
				units.BytesString(buckets[i-1].MinSize))
		}

		log(ctx).Infof("%v files %v: %7v files, total size %v\n",
			prefix,
			sizeRange,
			bucket.Count, units.BytesString(bucket.TotalSize))

		hasAny = true

		if showExamples && len(bucket.Examples) > 0 {
			log(ctx).Info("Examples:")

			for _, sample := range bucket.Examples {
				log(ctx).Infof(" - %v\n", sample)
			}
		}
	}

	if !hasAny {
		log(ctx).Infof("%v files: None", prefix)
	}
}

var _ snapshotfs.EstimateProgress = estimateTaskProgress{}

func handleEstimate(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	var req serverapi.EstimateRequest

	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

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

	policyTree, err := policy.TreeForSourceWithOverride(ctx, rc.rep, snapshot.SourceInfo{
		Host:     rc.rep.ClientOptions().Hostname,
		UserName: rc.rep.ClientOptions().Username,
		Path:     resolvedRoot,
	}, req.PolicyOverride)
	if err != nil {
		return nil, internalServerError(errors.Wrap(err, "unable to get policy tree"))
	}

	// launch a goroutine that will continue the estimate and can be observed in the Tasks UI.

	//nolint:errcheck
	go rc.srv.taskManager().Run(ctx, "Estimate", resolvedRoot, func(ctx context.Context, ctrl uitask.Controller) error {
		taskIDChan <- ctrl.CurrentTaskID()

		estimatectx, cancel := context.WithCancel(ctx)
		defer cancel()

		ctrl.OnCancel(cancel)

		return snapshotfs.Estimate(estimatectx, dir, policyTree, estimateTaskProgress{ctrl}, req.MaxExamplesPerBucket)
	})

	taskID := <-taskIDChan

	task, ok := rc.srv.taskManager().GetTask(taskID)
	if !ok {
		return nil, internalServerError(errors.New("task not found"))
	}

	return task, nil
}
