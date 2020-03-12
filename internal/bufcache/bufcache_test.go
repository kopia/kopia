package bufcache_test

import (
	"testing"

	"github.com/kopia/kopia/internal/bufcache"
)

func TestBufCache(t *testing.T) {
	cases := []struct {
		requestCap    int
		wantResultCap int
	}{
		{0, 256},
		{1, 256},
		{256, 256},
		{257, 1024},
		{1024, 1024},
		{1025, 4096},
		{1 << 24, 1 << 24},     // 16 MB
		{1 << 25, 1 << 25},     // 32 MB
		{1<<25 + 3, 1<<25 + 3}, // 32 MB + 3, not pooled anymore
	}

	for _, tc := range cases {
		result := bufcache.EmptyBytesWithCapacity(tc.requestCap)
		if got, want := cap(result), tc.wantResultCap; got != want {
			t.Errorf("got invalid capacity of buffer: %v, want %v", got, want)
		}
	}
}
