package storagetesting

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/kopia/kopia/storage"
)

// AssertGetBlock asserts that the specified storage block has correct content.
func AssertGetBlock(ctx context.Context, t *testing.T, s storage.Storage, block string, expected []byte) {
	b, err := s.GetBlock(ctx, block, 0, -1)
	if err != nil {
		t.Errorf(errorPrefix()+"GetBlock(%v) returned error %v, expected data: %v", block, err, expected)
		return
	}

	if !bytes.Equal(b, expected) {
		t.Errorf(errorPrefix()+"GetBlock(%v) returned %x, but expected %x", block, b, expected)
	}
}

// AssertGetBlockNotFound asserts that GetBlock() for specified storage block returns ErrBlockNotFound.
func AssertGetBlockNotFound(ctx context.Context, t *testing.T, s storage.Storage, block string) {
	b, err := s.GetBlock(ctx, block, 0, -1)
	if err != storage.ErrBlockNotFound || b != nil {
		t.Errorf(errorPrefix()+"GetBlock(%v) returned %v, %v but expected ErrBlockNotFound", block, b, err)
	}
}

// AssertListResults asserts that the list results with given prefix return the specified list of names in order.
func AssertListResults(ctx context.Context, t *testing.T, s storage.Storage, prefix string, expected ...string) {
	var names []string

	ctx, cancel := context.WithCancel(ctx)
	blocks := s.ListBlocks(ctx, prefix)
	defer cancel()
	for e := range blocks {
		names = append(names, e.BlockID)
	}

	if !reflect.DeepEqual(names, expected) {
		t.Errorf(errorPrefix()+"ListBlocks(%v) returned %v, but expected %v", prefix, names, expected)
	}
}

func errorPrefix() string {
	if _, fn, line, ok := runtime.Caller(2); ok {
		return fmt.Sprintf("called from %v:%v: ", filepath.Base(fn), line)
	}

	return ""
}
