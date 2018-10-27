package packindex_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/kopia/repo/internal/packindex"
)

func TestSubset(t *testing.T) {
	cases := []struct {
		aBlocks, bBlocks []string
		want             bool
	}{
		{[]string{}, []string{"aa"}, true},
		{[]string{}, []string{"aa", "bb"}, true},
		{[]string{"aa"}, []string{"aa"}, true},
		{[]string{"aa"}, []string{"bb"}, false},
		{[]string{"aa"}, []string{"aa", "bb"}, true},
		{[]string{"aa"}, []string{"aa", "bb", "cc"}, true},
		{[]string{"aa", "bb"}, []string{"bb", "cc"}, false},
		{[]string{"aa", "bb"}, []string{"aa"}, false},
		{[]string{"aa", "bb"}, []string{}, false},
		{[]string{"aa", "bb", "cc", "dd", "ee", "ff"}, []string{"aa", "bb", "cc", "dd", "ee", "ff"}, true},
		{[]string{"aa", "bb", "cc", "dd", "ee", "ff"}, []string{"aa", "bb", "cc", "dd", "ef", "ff"}, false},
		{[]string{"aa", "bb", "cc", "dd", "ee", "ff"}, []string{"aa", "bb", "cc", "dd", "ee", "ef", "ff"}, true},
	}

	for _, tc := range cases {
		a, err := indexWithBlockIDs(tc.aBlocks)
		if err != nil {
			t.Fatalf("error building index: %v", err)
		}
		b, err := indexWithBlockIDs(tc.bBlocks)
		if err != nil {
			t.Fatalf("error building index: %v", err)
		}

		if got, want := packindex.IsSubset(a, b), tc.want; got != want {
			t.Errorf("invalid value of IsSubset(%v,%v): %v, wanted %v", tc.aBlocks, tc.bBlocks, got, want)
		}
	}
}
func indexWithBlockIDs(items []string) (packindex.Index, error) {
	b := packindex.NewBuilder()
	for _, it := range items {
		b.Add(packindex.Info{
			BlockID:    it,
			PackFile:   "x",
			PackOffset: 1,
			Length:     1,
		})
	}
	var buf bytes.Buffer
	if err := b.Build(&buf); err != nil {
		return nil, fmt.Errorf("build error: %v", err)
	}
	return packindex.Open(bytes.NewReader(buf.Bytes()))
}
