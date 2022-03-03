package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/remoterepoapi"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

func (s *Server) handleObjectGet(w http.ResponseWriter, r *http.Request) {
	oidstr := mux.Vars(r)["objectID"]

	if !requireUIUser(s, r) {
		http.Error(w, "access denied", http.StatusForbidden)
		return
	}

	oid, err := object.ParseID(oidstr)
	if err != nil {
		http.Error(w, "invalid object id", http.StatusBadRequest)
		return
	}

	obj, err := s.rep.OpenObject(r.Context(), oid)
	if errors.Is(err, object.ErrObjectNotFound) {
		http.Error(w, "object not found", http.StatusNotFound)
		return
	}

	if snapshotfs.IsDirectoryID(oid) {
		w.Header().Set("Content-Type", "application/json")
	}

	fname := oid.String()
	if p := r.URL.Query().Get("fname"); p != "" {
		fname = p
		w.Header().Set("Content-Disposition", "attachment; filename=\""+p+"\"")
	}

	mtime := clock.Now()

	if p := r.URL.Query().Get("mtime"); p != "" {
		if m, err := time.Parse(time.RFC3339Nano, p); err == nil {
			mtime = m
		}
	}

	http.ServeContent(w, r, fname, mtime, obj)
}

func (s *Server) handleObjectsPrefetch(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	var req remoterepoapi.PrefetchObjectsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request")
	}

	contentIDs, err := s.rep.PrefetchObjects(ctx, req.ObjectIDs)
	if err != nil {
		if errors.Is(err, object.ErrObjectNotFound) {
			return nil, notFoundError("object not found")
		}

		return nil, internalServerError(err)
	}

	return &remoterepoapi.PrefetchObjectsResponse{
		ContentIDs: contentIDs,
	}, nil
}
