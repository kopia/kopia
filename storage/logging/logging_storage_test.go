package logging

import (
	"context"
	"testing"

	"github.com/kopia/repo/internal/storagetesting"
)

func TestLoggingStorage(t *testing.T) {
	data := map[string][]byte{}
	r := NewWrapper(storagetesting.NewMapStorage(data, nil, nil))
	if r == nil {
		t.Errorf("unexpected result: %v", r)
	}
	storagetesting.VerifyStorage(context.Background(), t, r)
}
