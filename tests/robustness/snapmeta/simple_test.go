//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package snapmeta

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/kopia/kopia/tests/robustness"
)

func TestSimpleBasic(t *testing.T) {
	ctx := context.Background()

	simple := NewSimple()

	gotData, err := simple.Load(ctx, "non-existent-key")
	if !errors.Is(err, robustness.ErrKeyNotFound) {
		t.Fatalf("Did not get expected error: %q", err)
	}

	if gotData != nil {
		t.Fatalf("Expecting nil data return from a key that does not exist")
	}

	storeKey := "key-to-store"
	data := []byte("some stored data")
	simple.Store(ctx, storeKey, data)

	gotData, err = simple.Load(ctx, storeKey)
	if err != nil {
		t.Fatalf("Error getting key: %v", err)
	}

	if !bytes.Equal(gotData, data) {
		t.Fatalf("Did not get the correct data")
	}

	simple.Delete(ctx, storeKey)

	gotData, err = simple.Load(ctx, storeKey)
	if !errors.Is(err, robustness.ErrKeyNotFound) {
		t.Fatalf("Did not get expected error: %q", err)
	}

	if gotData != nil {
		t.Fatalf("Expecting nil data return from a deleted key")
	}
}
