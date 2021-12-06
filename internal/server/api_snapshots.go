package server

import (
	"context"
	"net/http"
	"net/url"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func (s *Server) handleSnapshotList(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	si := getSnapshotSourceFromURL(r.URL)

	manifestIDs, err := snapshot.ListSnapshotManifests(ctx, s.rep, &si, nil)
	if err != nil {
		return nil, internalServerError(err)
	}

	manifests, err := snapshot.LoadSnapshots(ctx, s.rep, manifestIDs)
	if err != nil {
		return nil, internalServerError(err)
	}

	manifests = snapshot.SortByTime(manifests, false)

	resp := &serverapi.SnapshotsResponse{
		Snapshots: []*serverapi.Snapshot{},
	}

	pol, _, _, err := policy.GetEffectivePolicy(ctx, s.rep, si)
	if err == nil {
		pol.RetentionPolicy.ComputeRetentionReasons(manifests)
	}

	for _, m := range manifests {
		resp.Snapshots = append(resp.Snapshots, convertSnapshotManifest(m))
	}

	resp.UnfilteredCount = len(resp.Snapshots)

	if r.URL.Query().Get("all") == "" {
		resp.Snapshots = uniqueSnapshots(resp.Snapshots)
		resp.UniqueCount = len(resp.Snapshots)
	} else {
		resp.UniqueCount = len(uniqueSnapshots(resp.Snapshots))
	}

	return resp, nil
}

func uniqueSnapshots(rows []*serverapi.Snapshot) []*serverapi.Snapshot {
	var result []*serverapi.Snapshot

	for _, r := range rows {
		if len(result) == 0 {
			result = append(result, r)
			continue
		}

		last := result[len(result)-1]

		if r.RootEntry == last.RootEntry {
			last.RetentionReasons = append(last.RetentionReasons, r.RetentionReasons...)
			last.Pins = append(last.Pins, r.Pins...)
		} else {
			result = append(result, r)
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
