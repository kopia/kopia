package blobtesting

import (
	"context"
	"testing"
)

func TestMapStorage(t *testing.T) {
	data := DataMap{}

	r := NewMapStorage(data, nil, nil)
	if r == nil {
		t.Errorf("unexpected result: %v", r)
	}

	VerifyStorage(context.Background(), t, r)
}
