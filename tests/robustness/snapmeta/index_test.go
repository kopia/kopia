//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package snapmeta

import (
	"testing"
)

func TestIndex(t *testing.T) {
	idx := Index{}

	const (
		snapshotIndexName = "snapshotIndex"
		snapIDKey         = "snapID1"
	)

	idx.AddToIndex(snapIDKey, snapshotIndexName)

	keys := idx.GetKeys(snapshotIndexName)
	if got, want := len(keys), 1; got != want {
		t.Fatalf("expected %v keys but got %v", want, got)
	}

	if got, want := keys[0], snapIDKey; got != want {
		t.Fatalf("expected %v but got %v", want, got)
	}

	idx.RemoveFromIndex(snapIDKey, snapshotIndexName)

	keys = idx.GetKeys(snapshotIndexName)
	if got, want := len(keys), 0; got != want {
		t.Fatalf("expected %v keys but got %v", want, got)
	}
}
