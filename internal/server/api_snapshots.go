package server

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func handleListSnapshots(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	si := getSnapshotSourceFromURL(rc.req.URL)

	manifestIDs, err := snapshot.ListSnapshotManifests(ctx, rc.rep, &si, nil)
	if err != nil {
		return nil, internalServerError(err)
	}

	manifests, err := snapshot.LoadSnapshots(ctx, rc.rep, manifestIDs)
	if err != nil {
		return nil, internalServerError(err)
	}

	manifests = snapshot.SortByTime(manifests, false)

	resp := &serverapi.SnapshotsResponse{
		Snapshots: []*serverapi.Snapshot{},
	}

	pol, _, _, err := policy.GetEffectivePolicy(ctx, rc.rep, si)
	if err == nil {
		pol.RetentionPolicy.ComputeRetentionReasons(manifests)
	}

	for _, m := range manifests {
		resp.Snapshots = append(resp.Snapshots, convertSnapshotManifest(m))
	}

	resp.UnfilteredCount = len(resp.Snapshots)

	if rc.queryParam("all") == "" {
		resp.Snapshots = uniqueSnapshots(resp.Snapshots)
		resp.UniqueCount = len(resp.Snapshots)
	} else {
		resp.UniqueCount = len(uniqueSnapshots(resp.Snapshots))
	}

	return resp, nil
}

func handleDeleteSnapshots(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	var req serverapi.DeleteSnapshotsRequest

	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request")
	}

	sm := rc.srv.snapshotAllSourceManagers()[req.SourceInfo]
	if sm == nil {
		return nil, requestError(serverapi.ErrorNotFound, "unknown source")
	}

	// stop source manager and remove from map
	if req.DeleteSourceAndPolicy {
		if !rc.srv.deleteSourceManager(ctx, req.SourceInfo) {
			return nil, requestError(serverapi.ErrorNotFound, "unknown source")
		}
	}

	if err := repo.WriteSession(ctx, rc.rep, repo.WriteSessionOptions{
		Purpose: "DeleteSnapshots",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		var manifestIDs []manifest.ID

		if req.DeleteSourceAndPolicy {
			mans, err := snapshot.ListSnapshotManifests(ctx, w, &req.SourceInfo, nil)
			if err != nil {
				return errors.Wrap(err, "unable to list snapshots")
			}

			manifestIDs = mans
		} else {
			snaps, err := snapshot.LoadSnapshots(ctx, w, req.SnapshotManifestIDs)
			if err != nil {
				return errors.Wrap(err, "unable to load snapshots")
			}

			for _, sn := range snaps {
				if sn.Source != req.SourceInfo {
					return errors.New("source info does not match snapshot source")
				}
			}

			manifestIDs = req.SnapshotManifestIDs
		}

		for _, m := range manifestIDs {
			if err := w.DeleteManifest(ctx, m); err != nil {
				return errors.Wrap(err, "uanble to delete snapshot")
			}
		}

		if req.DeleteSourceAndPolicy {
			if err := policy.RemovePolicy(ctx, w, req.SourceInfo); err != nil {
				return errors.Wrap(err, "unable to remove policy")
			}
		}

		return nil
	}); err != nil {
		// if source deletion failed, refresh the repository to rediscover the source
		rc.srv.Refresh()

		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}

func handleEditSnapshots(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	var req serverapi.EditSnapshotsRequest

	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request")
	}

	var snaps []*serverapi.Snapshot

	if err := repo.WriteSession(ctx, rc.rep, repo.WriteSessionOptions{
		Purpose: "EditSnapshots",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		for _, id := range req.Snapshots {
			snap, err := snapshot.LoadSnapshot(ctx, w, id)
			if err != nil {
				return errors.Wrap(err, "unable to load snapshot")
			}

			changed := false

			if snap.UpdatePins(req.AddPins, req.RemovePins) {
				changed = true
			}

			if req.NewDescription != nil {
				changed = true
				snap.Description = *req.NewDescription
			}

			if changed {
				if err := snapshot.UpdateSnapshot(ctx, w, snap); err != nil {
					return errors.Wrap(err, "error updating snapshot")
				}
			}

			snaps = append(snaps, convertSnapshotManifest(snap))
		}

		return nil
	}); err != nil {
		return nil, internalServerError(err)
	}

	return snaps, nil
}

func forAllSourceManagersMatchingURLFilter(ctx context.Context, managers map[snapshot.SourceInfo]*sourceManager, c func(s *sourceManager, ctx context.Context) serverapi.SourceActionResponse, values url.Values) (interface{}, *apiError) {
	resp := &serverapi.MultipleSourceActionResponse{
		Sources: map[string]serverapi.SourceActionResponse{},
	}

	for src, mgr := range managers {
		if mgr.isRunningReadOnly() {
			continue
		}

		if !sourceMatchesURLFilter(src, values) {
			continue
		}

		resp.Sources[src.String()] = c(mgr, ctx)
	}

	if len(resp.Sources) == 0 {
		return nil, notFoundError("no source matching the provided filters")
	}

	return resp, nil
}

func handleUpload(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	return forAllSourceManagersMatchingURLFilter(ctx, rc.srv.snapshotAllSourceManagers(), (*sourceManager).upload, rc.req.URL.Query())
}

func handleCancel(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	return forAllSourceManagersMatchingURLFilter(ctx, rc.srv.snapshotAllSourceManagers(), (*sourceManager).cancel, rc.req.URL.Query())
}

func handlePause(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	return forAllSourceManagersMatchingURLFilter(ctx, rc.srv.snapshotAllSourceManagers(), (*sourceManager).pause, rc.req.URL.Query())
}

func handleResume(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	return forAllSourceManagersMatchingURLFilter(ctx, rc.srv.snapshotAllSourceManagers(), (*sourceManager).resume, rc.req.URL.Query())
}

func uniqueSnapshots(rows []*serverapi.Snapshot) []*serverapi.Snapshot {
	result := []*serverapi.Snapshot{}
	resultByRootEntry := map[string]*serverapi.Snapshot{}

	for _, r := range rows {
		last := resultByRootEntry[r.RootEntry]
		if last == nil {
			result = append(result, r)
			resultByRootEntry[r.RootEntry] = r
		} else {
			last.RetentionReasons = append(last.RetentionReasons, r.RetentionReasons...)
			last.Pins = append(last.Pins, r.Pins...)
		}
	}

	for _, r := range result {
		r.RetentionReasons = policy.CompactRetentionReasons(r.RetentionReasons)
		r.Pins = policy.CompactPins(r.Pins)
	}

	return result
}

func sourceMatchesURLFilter(src snapshot.SourceInfo, query url.Values) bool {
	if v := query.Get("host"); v != "" && src.Host != v {
		return false
	}

	if v := query.Get("userName"); v != "" && src.UserName != v {
		return false
	}

	if v := query.Get("path"); v != "" && src.Path != v {
		return false
	}

	return true
}

func convertSnapshotManifest(m *snapshot.Manifest) *serverapi.Snapshot {
	e := &serverapi.Snapshot{
		ID:               m.ID,
		Description:      m.Description,
		StartTime:        m.StartTime,
		EndTime:          m.EndTime,
		IncompleteReason: m.IncompleteReason,
		RootEntry:        m.RootObjectID().String(),
		RetentionReasons: append([]string{}, m.RetentionReasons...),
		Pins:             append([]string{}, m.Pins...),
	}

	if re := m.RootEntry; re != nil {
		e.Summary = re.DirSummary
	}

	return e
}
