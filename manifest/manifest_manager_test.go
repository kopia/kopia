package manifest

import (
	"reflect"
	"sort"
	"testing"

	"github.com/kopia/kopia/internal/storagetesting"

	"github.com/kopia/kopia/block"
)

func TestManifest(t *testing.T) {
	data := map[string][]byte{}

	mgr, err := newManaferForTesting(t, data)
	if err != nil {
		t.Fatalf("unable to open block manager: %v", mgr)
	}

	item1 := map[string]int{"foo": 1, "bar": 2}
	item2 := map[string]int{"foo": 2, "bar": 3}
	item3 := map[string]int{"foo": 3, "bar": 4}

	labels1 := map[string]string{"color": "red"}
	labels2 := map[string]string{"color": "blue", "shape": "square"}
	labels3 := map[string]string{"shape": "square", "color": "red"}

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
	mgr2, err := newManaferForTesting(t, data)
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
	if err := mgr2.Load(); err != nil {
		t.Errorf("unable to load: %v", err)
	}
}

func addAndVerify(t *testing.T, mgr *Manager, labels map[string]string, data map[string]int) string {
	t.Helper()
	id, err := mgr.Add(labels, data)
	if err != nil {
		t.Errorf("unable to add %v (%v): %v", labels, data, err)
		return ""
	}

	verifyItem(t, mgr, id, labels, data)
	return id
}

func verifyItem(t *testing.T, mgr *Manager, id string, labels map[string]string, data map[string]int) {
	t.Helper()
	var retrieved map[string]int

	l, err := mgr.Get(id, &retrieved)
	if err != nil {
		t.Errorf("unable to retrieve %q: %v", id, err)
		return
	}

	if !reflect.DeepEqual(l, labels) {
		t.Errorf("invalid labels retrieved %v, wanted %v", l, labels)
	}
}

func verifyItemNotFound(t *testing.T, mgr *Manager, id string) {
	t.Helper()
	var retrieved map[string]int

	_, err := mgr.Get(id, &retrieved)
	if got, want := err, ErrNotFound; got != want {
		t.Errorf("invalid error when getting %q %v, expected %v", id, err, ErrNotFound)
		return
	}
}

func verifyMatches(t *testing.T, mgr *Manager, labels map[string]string, expected []string) {
	t.Helper()

	matches := mgr.Find(labels)
	sort.Strings(matches)
	sort.Strings(expected)

	if !reflect.DeepEqual(matches, expected) {
		t.Errorf("invalid matches for %v: %v, expected %v", labels, matches, expected)
	}
}

func newManaferForTesting(t *testing.T, data map[string][]byte) (*Manager, error) {
	formatter, err := block.FormatterFactories["TESTONLY_MD5"](block.FormattingOptions{})
	if err != nil {
		panic("can't create formatter")
	}
	st := storagetesting.NewMapStorage(data)

	return NewManager(block.NewManager(st, 10000, 100000, formatter))
}
