package storagetesting

import (
	"bytes"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/kopia/kopia/blob"
)

// AssertGetBlock asserts that the specified storage block has correct content.
func AssertGetBlock(t *testing.T, s blob.Storage, block string, expected []byte) {
	b, err := s.GetBlock(block)
	if err != nil {
		t.Errorf(errorPrefix()+"GetBlock(%v) returned error %v, expected data: %v", block, err, expected)
		return
	}

	if !bytes.Equal(b, expected) {
		t.Errorf(errorPrefix()+"GetBlock(%v) returned %x, but expected %x", block, b, expected)
	}
}

// AssertGetBlockNotFound asserts that GetBlock() for specified storage block returns ErrBlockNotFound.
func AssertGetBlockNotFound(t *testing.T, s blob.Storage, block string) {
	b, err := s.GetBlock(block)
	if err != blob.ErrBlockNotFound || b != nil {
		t.Errorf(errorPrefix()+"GetBlock(%v) returned %v, %v but expected ErrBlockNotFound", block, b, err)
	}
}

// AssertBlockExists asserts that BlockExists() the specified storage block returns the correct value.
func AssertBlockExists(t *testing.T, s blob.Storage, block string, expected bool) {
	_, err := s.BlockSize(block)
	var exists bool
	if err == nil {
		exists = true
	} else if err == blob.ErrBlockNotFound {
		exists = false
	} else {
		t.Errorf(errorPrefix()+"BlockSize(%v) returned error: %v", block, err)
		return
	}

	if exists != expected {
		t.Errorf(errorPrefix()+"BlockSize(%v) returned exists=%v, but expected %v", block, exists, expected)
	}
}

// AssertListResults asserts that the list results with given prefix return the specified list of names in order.
func AssertListResults(t *testing.T, s blob.Storage, prefix string, expected ...string) {
	var names []string

	for e := range s.ListBlocks(prefix) {
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
