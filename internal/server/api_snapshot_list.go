package server

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

type snapshotListEntry struct {
	ID               manifest.ID          `json:"id"`
	Source           snapshot.SourceInfo  `json:"source"`
	Description      string               `json:"description"`
	StartTime        time.Time            `json:"startTime"`
	EndTime          time.Time            `json:"endTime"`
	IncompleteReason string               `json:"incomplete,omitempty"`
	Summary          *fs.DirectorySummary `json:"summary"`
	RootEntry        string               `json:"rootID"`
	RetentionReasons []string             `json:"retention"`
}

type snapshotListResponse struct {
	Snapshots []*snapshotListEntry `json:"snapshots"`
}

func (s *Server) handleSourceSnapshotList(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	manifestIDs, err := snapshot.ListSnapshotManifests(ctx, s.rep, nil)
	if err != nil {
		return nil, internalServerError(err)
	}

	manifests, err := snapshot.LoadSnapshots(ctx, s.rep, manifestIDs)
	if err != nil {
		return nil, internalServerError(err)
	}

	resp := &snapshotListResponse{
		Snapshots: []*snapshotListEntry{},
	}
	groups := snapshot.GroupBySource(manifests)
	for _, grp := range groups {
		first := grp[0]
		if !sourceMatchesURLFilter(first.Source, r.URL.Query()) {
			continue
		}

		pol, _, err := policy.GetEffectivePolicy(ctx, s.rep, first.Source)
		if err == nil {
			pol.RetentionPolicy.ComputeRetentionReasons(grp)
		}

		for _, m := range grp {
			resp.Snapshots = append(resp.Snapshots, convertSnapshotManifest(m))
		}
	}

	return resp, nil
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

func convertSnapshotManifest(m *snapshot.Manifest) *snapshotListEntry {
	e := &snapshotListEntry{
		ID:               m.ID,
		Source:           m.Source,
		Description:      m.Description,
		StartTime:        m.StartTime,
		EndTime:          m.EndTime,
		IncompleteReason: m.IncompleteReason,
		RootEntry:        m.RootObjectID().String(),
		RetentionReasons: m.RetentionReasons,
	}

	if re := m.RootEntry; re != nil {
		e.Summary = re.DirSummary
	}

	return e
}
