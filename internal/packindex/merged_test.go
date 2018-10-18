package packindex_test

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/kopia/kopia/internal/packindex"
)

func TestMerged(t *testing.T) {
	i1, err := indexWithItems(
		packindex.Info{BlockID: "aabbcc", TimestampSeconds: 1, PackFile: "xx", PackOffset: 11},
		packindex.Info{BlockID: "ddeeff", TimestampSeconds: 1, PackFile: "xx", PackOffset: 111},
		packindex.Info{BlockID: "z010203", TimestampSeconds: 1, PackFile: "xx", PackOffset: 111},
		packindex.Info{BlockID: "de1e1e", TimestampSeconds: 4, PackFile: "xx", PackOffset: 111},
	)
	if err != nil {
		t.Fatalf("can't create index: %v", err)
	}
	i2, err := indexWithItems(
		packindex.Info{BlockID: "aabbcc", TimestampSeconds: 3, PackFile: "yy", PackOffset: 33},
		packindex.Info{BlockID: "xaabbcc", TimestampSeconds: 1, PackFile: "xx", PackOffset: 111},
		packindex.Info{BlockID: "de1e1e", TimestampSeconds: 4, PackFile: "xx", PackOffset: 222, Deleted: true},
	)
	if err != nil {
		t.Fatalf("can't create index: %v", err)
	}
	i3, err := indexWithItems(
		packindex.Info{BlockID: "aabbcc", TimestampSeconds: 2, PackFile: "zz", PackOffset: 22},
		packindex.Info{BlockID: "ddeeff", TimestampSeconds: 1, PackFile: "zz", PackOffset: 222},
		packindex.Info{BlockID: "k010203", TimestampSeconds: 1, PackFile: "xx", PackOffset: 111},
		packindex.Info{BlockID: "k020304", TimestampSeconds: 1, PackFile: "xx", PackOffset: 111},
	)
	if err != nil {
		t.Fatalf("can't create index: %v", err)
	}

	m := packindex.Merged{i1, i2, i3}
	i, err := m.GetInfo("aabbcc")
	if err != nil || i == nil {
		t.Fatalf("unable to get info: %v", err)
	}
	if got, want := i.PackOffset, uint32(33); got != want {
		t.Errorf("invalid pack offset %v, wanted %v", got, want)
	}

	var inOrder []string
	m.Iterate("", func(i packindex.Info) error {
		inOrder = append(inOrder, i.BlockID)
		if i.BlockID == "de1e1e" {
			if i.Deleted {
				t.Errorf("iteration preferred deleted block over non-deleted")
			}
		}
		return nil
	})

	if i, err := m.GetInfo("de1e1e"); err != nil {
		t.Errorf("error getting deleted block info: %v", err)
	} else if i.Deleted {
		t.Errorf("GetInfo preferred deleted block over non-deleted")
	}

	expectedInOrder := []string{
		"aabbcc",
		"ddeeff",
		"de1e1e",
		"k010203",
		"k020304",
		"xaabbcc",
		"z010203",
	}
	if !reflect.DeepEqual(inOrder, expectedInOrder) {
		t.Errorf("unexpected items in order: %v, wanted %v", inOrder, expectedInOrder)
	}

	if err := m.Close(); err != nil {
		t.Errorf("unexpected error in Close(): %v", err)
	}
}

func indexWithItems(items ...packindex.Info) (packindex.Index, error) {
	b := packindex.NewBuilder()
	for _, it := range items {
		b.Add(it)
	}
	var buf bytes.Buffer
	if err := b.Build(&buf); err != nil {
		return nil, fmt.Errorf("build error: %v", err)
	}
	return packindex.Open(bytes.NewReader(buf.Bytes()))
}
