package repo

import (
	"math"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/content"
)

// errCannotResume is returned when a write session cannot prove that nothing it wrote
// since its last flush was lost with a broken inner session.
var errCannotResume = errors.New("write session cannot be resumed")

// pendingAck marks a write whose acknowledgement has not been received.
const pendingAck = math.MaxUint64

// contentWriteKind describes how a content write is tracked.
type contentWriteKind int

const (
	contentWriteTracked contentWriteKind = iota
	contentWriteAlreadyTracked
	contentWriteOverflow
)

// writeTracker tracks what a write session wrote since its last successful flush,
// so that the session can decide whether it may continue on a new inner session
// after the previous one broke.
//
// A write is attributed to the flush epoch in which its acknowledgement arrived.
// A flush that starts in epoch E makes durable every write acknowledged in epoch <= E,
// because the server processed those writes before it received the flush request.
type writeTracker struct {
	mu sync.Mutex

	// +checklocks:mu
	epoch uint64

	// content ID -> epoch of acknowledgement, or pendingAck.
	// +checklocks:mu
	contents map[content.ID]uint64

	// writes not tracked individually because more than maxUnflushedWritesToTrack were pending.
	// +checklocks:mu
	overflowPending int

	// +checklocks:mu
	overflowAcked bool

	// +checklocks:mu
	overflowAckEpoch uint64

	// manifest mutations sent whose outcome is not known yet (or never will be).
	// +checklocks:mu
	manifestPending int

	// epoch of acknowledgement -> number of manifest mutations.
	// +checklocks:mu
	manifestAcked map[uint64]int
}

func (t *writeTracker) beginContentWrite(cid content.ID) contentWriteKind {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.contents[cid]; ok {
		return contentWriteAlreadyTracked
	}

	if len(t.contents) >= maxUnflushedWritesToTrack {
		t.overflowPending++
		return contentWriteOverflow
	}

	if t.contents == nil {
		t.contents = map[content.ID]uint64{}
	}

	t.contents[cid] = pendingAck

	return contentWriteTracked
}

// contentWriteNotSent undoes beginContentWrite for a request that never reached the server.
func (t *writeTracker) contentWriteNotSent(cid content.ID, kind contentWriteKind) {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch kind {
	case contentWriteTracked:
		delete(t.contents, cid)
	case contentWriteOverflow:
		t.overflowPending--
	case contentWriteAlreadyTracked:
	}
}

func (t *writeTracker) contentWriteAcked(cid content.ID, kind contentWriteKind) {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch kind {
	case contentWriteTracked, contentWriteAlreadyTracked:
		if t.contents[cid] == pendingAck {
			t.contents[cid] = t.epoch
		}

	case contentWriteOverflow:
		t.overflowPending--
		t.overflowAcked = true
		t.overflowAckEpoch = t.epoch
	}
}

func (t *writeTracker) beginManifestMutation() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.manifestPending++
}

// manifestMutationDone records the outcome of a manifest mutation.
// Mutations with an unknown outcome stay pending, which prevents resuming.
func (t *writeTracker) manifestMutationDone(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch {
	case err == nil:
		t.manifestPending--

		if t.manifestAcked == nil {
			t.manifestAcked = map[uint64]int{}
		}

		t.manifestAcked[t.epoch]++

	case errors.Is(err, errRequestNotSent):
		t.manifestPending--
	}
}

// startFlush starts a new epoch and returns the epoch whose writes the flush makes durable.
func (t *writeTracker) startFlush() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	e := t.epoch
	t.epoch++

	return e
}

// flushSucceeded forgets the writes made durable by a successful flush started in the provided epoch.
func (t *writeTracker) flushSucceeded(epoch uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for cid, e := range t.contents {
		if e <= epoch {
			delete(t.contents, cid)
		}
	}

	for e := range t.manifestAcked {
		if e <= epoch {
			delete(t.manifestAcked, e)
		}
	}

	if t.overflowAcked && t.overflowAckEpoch <= epoch {
		t.overflowAcked = false
	}
}

// contentsToVerify returns the contents written since the last successful flush that must be
// in the repository before the write session can continue on a new inner session,
// or errCannotResume if that cannot be established by looking at contents alone.
func (t *writeTracker) contentsToVerify() ([]content.ID, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.overflowPending > 0 || t.overflowAcked {
		return nil, errors.Wrapf(errCannotResume, "more than %v contents were written since the last flush", maxUnflushedWritesToTrack)
	}

	if t.manifestPending > 0 || len(t.manifestAcked) > 0 {
		// manifests written on the broken session are not verified.
		return nil, errors.Wrap(errCannotResume, "manifests were changed since the last flush")
	}

	result := make([]content.ID, 0, len(t.contents))
	for cid := range t.contents {
		result = append(result, cid)
	}

	return result, nil
}
