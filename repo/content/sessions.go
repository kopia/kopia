package content

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobcrypto"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// BlobIDPrefixSession is the prefix for blob IDs indicating active sessions.
// Each blob ID will consist of {sessionID}.{suffix}.
const BlobIDPrefixSession blob.ID = "s"

const sessionIDLength = 8

// SessionID represents identifier of a session.
type SessionID string

// SessionInfo describes a particular session and is persisted in Session blob.
type SessionInfo struct {
	ID             SessionID `json:"id"`
	StartTime      time.Time `json:"startTime"`
	CheckpointTime time.Time `json:"checkpointTime"`
	User           string    `json:"username"`
	Host           string    `json:"hostname"`
}

//nolint:gochecknoglobals
var (
	sessionIDEpochStartTime   = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	sessionIDEpochGranularity = 30 * 24 * time.Hour
)

// generateSessionID generates a random session identifier.
func generateSessionID(now time.Time) (SessionID, error) {
	// generate session ID as {random-64-bit}{epoch-number}
	// where epoch number is roughly the number of months since 2000-01-01
	// so our 64-bit number only needs to be unique per month.
	// Given number of seconds per month, this allows >1000 sessions per
	// second before significant probability of collision while keeping the
	// session identifiers relatively short.
	r := make([]byte, sessionIDLength)
	if _, err := cryptorand.Read(r); err != nil {
		return "", errors.Wrap(err, "unable to read crypto bytes")
	}

	epochNumber := int(now.Sub(sessionIDEpochStartTime) / sessionIDEpochGranularity)

	return SessionID(fmt.Sprintf("%v%016x%x", BlobIDPrefixSession, r, epochNumber)), nil
}

func (bm *WriteManager) getOrStartSessionLocked(ctx context.Context) (SessionID, error) {
	if bm.currentSessionInfo.ID != "" {
		return bm.currentSessionInfo.ID, nil
	}

	id, err := generateSessionID(bm.timeNow())
	if err != nil {
		return "", errors.Wrap(err, "unable to generate session ID")
	}

	bm.currentSessionInfo = SessionInfo{
		ID:        id,
		StartTime: bm.timeNow(),
		User:      bm.sessionUser,
		Host:      bm.sessionHost,
	}

	bm.sessionMarkerBlobIDs = nil
	if err := bm.writeSessionMarkerLocked(ctx); err != nil {
		return "", errors.Wrap(err, "unable to write session marker")
	}

	return id, nil
}

// commitSession commits the current session by deleting all session marker blobs
// that got written.
func (bm *WriteManager) commitSession(ctx context.Context) error {
	for _, b := range bm.sessionMarkerBlobIDs {
		if err := bm.st.DeleteBlob(ctx, b); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
			return errors.Wrapf(err, "failed to delete session marker %v", b)
		}
	}

	bm.currentSessionInfo.ID = ""
	bm.sessionMarkerBlobIDs = nil

	return nil
}

// writeSessionMarkerLocked writes a session marker indicating last time the session
// was known to be alive.
// TODO(jkowalski): write this periodically when sessions span the duration of an upload.
func (bm *WriteManager) writeSessionMarkerLocked(ctx context.Context) error {
	cp := bm.currentSessionInfo
	cp.CheckpointTime = bm.timeNow()

	js, err := json.Marshal(cp)
	if err != nil {
		return errors.Wrap(err, "unable to serialize session marker payload")
	}

	var encrypted gather.WriteBuffer
	defer encrypted.Close()

	sessionBlobID, err := blobcrypto.Encrypt(bm.format, gather.FromSlice(js), BlobIDPrefixSession, blob.ID(bm.currentSessionInfo.ID), &encrypted)
	if err != nil {
		return errors.Wrap(err, "unable to encrypt session marker")
	}

	bm.onUpload(int64(encrypted.Length()))

	if err := bm.st.PutBlob(ctx, sessionBlobID, encrypted.Bytes(), blob.PutOptions{}); err != nil {
		return errors.Wrapf(err, "unable to write session marker: %v", string(sessionBlobID))
	}

	bm.sessionMarkerBlobIDs = append(bm.sessionMarkerBlobIDs, sessionBlobID)

	return nil
}

// SessionIDFromBlobID returns session ID from a given blob ID or empty string if it's not a session blob ID.
func SessionIDFromBlobID(b blob.ID) SessionID {
	parts := strings.Split(string(b), "-")
	if len(parts) == 1 {
		return ""
	}

	for _, sid := range parts[1:] {
		if strings.HasPrefix(sid, string(BlobIDPrefixSession)) {
			return SessionID(sid)
		}
	}

	return ""
}

// ListActiveSessions returns a set of all active sessions in a given storage.
func (bm *WriteManager) ListActiveSessions(ctx context.Context) (map[SessionID]*SessionInfo, error) {
	blobs, err := blob.ListAllBlobs(ctx, bm.st, BlobIDPrefixSession)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list session blobs")
	}

	m := map[SessionID]*SessionInfo{}

	var payload gather.WriteBuffer
	defer payload.Close()

	var decrypted gather.WriteBuffer
	defer decrypted.Close()

	for _, b := range blobs {
		payload.Reset()
		decrypted.Reset()

		sid := SessionIDFromBlobID(b.BlobID)
		if sid == "" {
			return nil, errors.Errorf("found invalid session blob %v", b.BlobID)
		}

		si := &SessionInfo{}

		err := bm.st.GetBlob(ctx, b.BlobID, 0, -1, &payload)
		if err != nil {
			if errors.Is(err, blob.ErrBlobNotFound) {
				continue
			}

			return nil, errors.Wrapf(err, "error loading session: %v", b.BlobID)
		}

		err = blobcrypto.Decrypt(bm.format, payload.Bytes(), b.BlobID, &decrypted)
		if err != nil {
			return nil, errors.Wrapf(err, "error decrypting session: %v", b.BlobID)
		}

		if err := json.NewDecoder(decrypted.Bytes().Reader()).Decode(si); err != nil {
			return nil, errors.Wrapf(err, "error parsing session: %v", b.BlobID)
		}

		if old := m[sid]; old == nil || si.CheckpointTime.After(old.CheckpointTime) {
			m[sid] = si
		}
	}

	return m, nil
}
