package blobtesting

import (
	"testing"
	"time"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

func TestObjectLockingStorage(t *testing.T) {
	r := NewVersionedMapStorage(nil)
	if r == nil {
		t.Errorf("unexpected result: %v", r)
	}

	VerifyStorage(testlogging.Context(t), t, r, blob.PutOptions{
		RetentionMode:   blob.Governance,
		RetentionPeriod: 24 * time.Hour,
	})
}
