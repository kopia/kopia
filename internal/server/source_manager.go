package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/kopia/kopia/snapshot"
	"github.com/rs/zerolog/log"
)

type sourceStatus struct {
	Source           snapshot.SourceInfo `json:"source"`
	Status           string              `json:"status"` // IDLE, UPLOADING
	Policy           *snapshot.Policy    `json:"policy"`
	LastSnapshotTime time.Time           `json:"lastSnapshotTime"`
	NextSnapshotTime *time.Time          `json:"nextSnapshotTime,omitempty"`
}

type sourceManager struct {
	server *Server
	src    snapshot.SourceInfo
	closed chan struct{}

	mu               sync.RWMutex
	pol              *snapshot.Policy
	status           string
	nextSnapshotTime *time.Time
	lastSnapshotTime time.Time
}

func (s *sourceManager) Status() sourceStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return sourceStatus{
		Source:           s.src,
		Status:           s.status,
		LastSnapshotTime: s.lastSnapshotTime,
		NextSnapshotTime: s.nextSnapshotTime,
		Policy:           s.pol,
	}
}

func (s *sourceManager) setStatus(stat string) {
	s.mu.Lock()
	s.status = stat
	s.mu.Unlock()
}

func (s *sourceManager) run() {
	s.setStatus("INITIALIZING")

	defer s.setStatus("STOPPED")
	s.setStatus("RUNNING")

	s.refreshStatus()

	for {
		select {
		case <-s.closed:
			return
		case <-time.After(15 * time.Second):
			s.refreshStatus()
			s.setStatus(fmt.Sprintf("IDLE-%v", time.Now()))
		}
	}
}

func (s *sourceManager) refreshStatus() {
	log.Info().Msgf("refreshing status for %v", s.src)
	pol, _, err := s.server.policyManager.GetEffectivePolicy(s.src)
	if err != nil {
		s.setStatus("FAILED")
		return
	}

	s.pol = pol
	snapshots, err := s.server.snapshotManager.ListSnapshots(s.src)
	if err != nil {
		s.setStatus("FAILED")
		return
	}
	snaps := snapshot.SortByTime(snapshots, true)
	if len(snaps) > 0 {
		s.lastSnapshotTime = snaps[0].StartTime
	}
}

func newSourceManager(src snapshot.SourceInfo, server *Server) *sourceManager {
	m := &sourceManager{
		src:    src,
		server: server,
		status: "UNKNOWN",
		closed: make(chan struct{}),
	}

	return m
}
