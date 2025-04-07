package server

import (
	"context"
	"encoding/json"
	"os"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func handleSourcesList(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	_, multiUser := rc.rep.(repo.DirectRepository)

	resp := &serverapi.SourcesResponse{
		Sources:       []*serverapi.SourceStatus{},
		LocalHost:     rc.rep.ClientOptions().Hostname,
		LocalUsername: rc.rep.ClientOptions().Username,
		MultiUser:     multiUser,
	}

	for src, v := range rc.srv.snapshotAllSourceManagers() {
		if sourceMatchesURLFilter(src, rc.req.URL.Query()) {
			resp.Sources = append(resp.Sources, v.Status())
		}
	}

	sort.Slice(resp.Sources, func(i, j int) bool {
		return resp.Sources[i].Source.String() < resp.Sources[j].Source.String()
	})

	return resp, nil
}

func handleSourcesCreate(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	var req serverapi.CreateSnapshotSourceRequest

	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	if req.Path == "" {
		return nil, requestError(serverapi.ErrorMalformedRequest, "missing path")
	}

	if req.Policy == nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "missing policy")
	}

	req.Path = ospath.ResolveUserFriendlyPath(req.Path, true)

	_, err := os.Stat(req.Path)
	if os.IsNotExist(err) {
		return nil, requestError(serverapi.ErrorPathNotFound, "path does not exist")
	}

	if err != nil {
		return nil, internalServerError(err)
	}

	sourceInfo := snapshot.SourceInfo{
		UserName: rc.rep.ClientOptions().Username,
		Host:     rc.rep.ClientOptions().Hostname,
		Path:     req.Path,
	}

	resp := &serverapi.CreateSnapshotSourceResponse{}

	if err = repo.WriteSession(ctx, rc.rep, repo.WriteSessionOptions{
		Purpose: "handleSourcesCreate",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		return policy.SetPolicy(ctx, w, sourceInfo, req.Policy)
	}); err != nil {
		return nil, internalServerError(errors.Wrap(err, "unable to set initial policy"))
	}

	manager := rc.srv.getOrCreateSourceManager(ctx, sourceInfo)

	if req.CreateSnapshot {
		resp.SnapshotStarted = true

		log(ctx).Debugf("scheduling snapshot of %v immediately...", sourceInfo)
		manager.scheduleSnapshotNow()
	}

	return resp, nil
}
