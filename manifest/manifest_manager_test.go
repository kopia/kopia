package manifest

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/storagetesting"

	"github.com/kopia/kopia/block"
)

func TestManifest(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	mgr, err := newManagerForTesting(t, data, keyTime)
	if err != nil {
		t.Fatalf("unable to open block manager: %v", mgr)
	}

	item1 := map[string]int{"foo": 1, "bar": 2}
	item2 := map[string]int{"foo": 2, "bar": 3}
	item3 := map[string]int{"foo": 3, "bar": 4}

	labels1 := map[string]string{"type": "item", "color": "red"}
	labels2 := map[string]string{"type": "item", "color": "blue", "shape": "square"}
	labels3 := map[string]string{"type": "item", "shape": "square", "color": "red"}

	id1 := addAndVerify(t, mgr, labels1, item1)
	id2 := addAndVerify(t, mgr, labels2, item2)
	id3 := addAndVerify(t, mgr, labels3, item3)

	cases := []struct {
		criteria map[string]string
		expected []string
	}{
		{map[string]string{"color": "red"}, []string{id1, id3}},
		{map[string]string{"color": "blue"}, []string{id2}},
		{map[string]string{"color": "green"}, nil},
		{map[string]string{"color": "red", "shape": "square"}, []string{id3}},
		{map[string]string{"color": "blue", "shape": "square"}, []string{id2}},
		{map[string]string{"color": "red", "shape": "circle"}, nil},
	}

	// verify before flush
	for _, tc := range cases {
		verifyMatches(t, mgr, tc.criteria, tc.expected)
	}
	verifyItem(t, mgr, id1, labels1, item1)
	verifyItem(t, mgr, id2, labels2, item2)
	verifyItem(t, mgr, id3, labels3, item3)

	if err := mgr.Flush(); err != nil {
		t.Errorf("flush error: %v", err)
	}
	if err := mgr.Flush(); err != nil {
		t.Errorf("flush error: %v", err)
	}

	// verify after flush
	for _, tc := range cases {
		verifyMatches(t, mgr, tc.criteria, tc.expected)
	}
	verifyItem(t, mgr, id1, labels1, item1)
	verifyItem(t, mgr, id2, labels2, item2)
	verifyItem(t, mgr, id3, labels3, item3)

	// verify in new manager
	mgr2, err := newManagerForTesting(t, data, keyTime)
	if err != nil {
		t.Fatalf("can't open block manager: %v", err)
	}
	for _, tc := range cases {
		verifyMatches(t, mgr2, tc.criteria, tc.expected)
	}
	verifyItem(t, mgr2, id1, labels1, item1)
	verifyItem(t, mgr2, id2, labels2, item2)
	verifyItem(t, mgr2, id3, labels3, item3)
	if err := mgr2.Flush(); err != nil {
		t.Errorf("flush error: %v", err)
	}

	// delete from one
	mgr.Delete(id3)
	verifyItemNotFound(t, mgr, id3)
	mgr.Flush()
	verifyItemNotFound(t, mgr, id3)

	// still found in another
	verifyItem(t, mgr2, id3, labels3, item3)
	if err := mgr2.load(); err != nil {
		t.Errorf("unable to load: %v", err)
	}

	if err := mgr.Compact(); err != nil {
		t.Errorf("can't compact: %v", err)
	}

	if got, want := len(mgr.b.ListGroupBlocks(manifestGroupID)), 1; got != want {
		t.Errorf("unexpected number of blocks: %v, want %v", got, want)
	}

	mgr.b.Flush()

	mgr3, err := newManagerForTesting(t, data, keyTime)
	if err != nil {
		t.Fatalf("can't open manager: %v", err)
	}

	verifyItem(t, mgr3, id1, labels1, item1)
	verifyItem(t, mgr3, id2, labels2, item2)
	verifyItemNotFound(t, mgr3, id3)
}

func addAndVerify(t *testing.T, mgr *Manager, labels map[string]string, data map[string]int) string {
	t.Helper()
	id, err := mgr.Put(labels, data)
	if err != nil {
		t.Errorf("unable to add %v (%v): %v", labels, data, err)
		return ""
	}

	verifyItem(t, mgr, id, labels, data)
	return id
}

func verifyItem(t *testing.T, mgr *Manager, id string, labels map[string]string, data map[string]int) {
	t.Helper()

	l, err := mgr.GetMetadata(id)
	if err != nil {
		t.Errorf("unable to retrieve %q: %v", id, err)
		return
	}

	if !reflect.DeepEqual(l.Labels, labels) {
		t.Errorf("invalid labels retrieved %v, wanted %v", l.Labels, labels)
	}
}

func verifyItemNotFound(t *testing.T, mgr *Manager, id string) {
	t.Helper()

	_, err := mgr.GetMetadata(id)
	if got, want := err, ErrNotFound; got != want {
		t.Errorf("invalid error when getting %q %v, expected %v", id, err, ErrNotFound)
		return
	}
}

func verifyMatches(t *testing.T, mgr *Manager, labels map[string]string, expected []string) {
	t.Helper()

	var matches []string
	for _, m := range mgr.Find(labels) {
		matches = append(matches, m.ID)
	}
	sort.Strings(matches)
	sort.Strings(expected)

	if !reflect.DeepEqual(matches, expected) {
		t.Errorf("invalid matches for %v: %v, expected %v", labels, matches, expected)
	}
}

func newManagerForTesting(t *testing.T, data map[string][]byte, keyTime map[string]time.Time) (*Manager, error) {
	st := storagetesting.NewMapStorage(data, keyTime)

	bm, err := block.NewManager(st, block.FormattingOptions{
		BlockFormat:            "TESTONLY_MD5",
		MaxPackedContentLength: 10000,
		MaxPackSize:            100000,
	}, block.CachingOptions{})
	if err != nil {
		return nil, fmt.Errorf("can't create block manager: %v", err)
	}

	return NewManager(bm)
}
