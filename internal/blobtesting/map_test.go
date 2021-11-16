package blobtesting

import (
	"testing"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

func TestMapStorage(t *testing.T) {
	data := DataMap{}

	r := NewMapStorage(data, nil, nil)
	if r == nil {
		t.Errorf("unexpected result: %v", r)
	}

	VerifyStorage(testlogging.Context(t), t, r, blob.PutOptions{})
}
