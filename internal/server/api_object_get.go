package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/kopia/kopia/repo/object"
)

func (s *Server) handleObjectGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "incompatible HTTP method", http.StatusMethodNotAllowed)
		return
	}

	oidstr := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]

	oid, err := object.ParseID(oidstr)
	if err != nil {
		http.Error(w, "invalid object id", http.StatusBadRequest)
		return
	}

	obj, err := s.rep.OpenObject(r.Context(), oid)
	if err == object.ErrObjectNotFound {
		http.Error(w, "object not found", http.StatusNotFound)
		return
	}

	if cid, _, ok := oid.ContentID(); ok && cid.Prefix() == "k" {
		w.Header().Set("Content-Type", "application/json")
	}

	fname := oid.String()
	if p := r.URL.Query().Get("fname"); p != "" {
		fname = p
		w.Header().Set("Content-Disposition", "attachment; filename=\""+p+"\"")
	}

	mtime := time.Now()

	if p := r.URL.Query().Get("mtime"); p != "" {
		if m, err := time.Parse(time.RFC3339Nano, p); err == nil {
			mtime = m
		}
	}

	http.ServeContent(w, r, fname, mtime, obj)
}
