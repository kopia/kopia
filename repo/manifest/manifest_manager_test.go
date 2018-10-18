package manifest

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/kopia/kopia/repo/block"
	"github.com/kopia/kopia/repo/internal/storagetesting"
)

func TestManifest(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	mgr, setupErr := newManagerForTesting(ctx, t, data)
	if setupErr != nil {
		t.Fatalf("unable to open block manager: %v", setupErr)
	}

	item1 := map[string]int{"foo": 1, "bar": 2}
	item2 := map[string]int{"foo": 2, "bar": 3}
	item3 := map[string]int{"foo": 3, "bar": 4}

	labels1 := map[string]string{"type": "item", "color": "red"}
	labels2 := map[string]string{"type": "item", "color": "blue", "shape": "square"}
	labels3 := map[string]string{"type": "item", "shape": "square", "color": "red"}

	id1 := addAndVerify(ctx, t, mgr, labels1, item1)
	id2 := addAndVerify(ctx, t, mgr, labels2, item2)
	id3 := addAndVerify(ctx, t, mgr, labels3, item3)

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
		verifyMatches(ctx, t, mgr, tc.criteria, tc.expected)
	}
	verifyItem(ctx, t, mgr, id1, labels1, item1)
	verifyItem(ctx, t, mgr, id2, labels2, item2)
	verifyItem(ctx, t, mgr, id3, labels3, item3)

	if err := mgr.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}
	if err := mgr.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	// verify after flush
	for _, tc := range cases {
		verifyMatches(ctx, t, mgr, tc.criteria, tc.expected)
	}
	verifyItem(ctx, t, mgr, id1, labels1, item1)
	verifyItem(ctx, t, mgr, id2, labels2, item2)
	verifyItem(ctx, t, mgr, id3, labels3, item3)

	// flush underlying block manager and verify in new manifest manager.
	mgr.b.Flush(ctx)
	mgr2, setupErr := newManagerForTesting(ctx, t, data)
	if setupErr != nil {
		t.Fatalf("can't open block manager: %v", setupErr)
	}
	for _, tc := range cases {
		verifyMatches(ctx, t, mgr2, tc.criteria, tc.expected)
	}
	verifyItem(ctx, t, mgr2, id1, labels1, item1)
	verifyItem(ctx, t, mgr2, id2, labels2, item2)
	verifyItem(ctx, t, mgr2, id3, labels3, item3)
	if err := mgr2.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	// delete from one
	time.Sleep(1 * time.Second)
	if err := mgr.Delete(ctx, id3); err != nil {
		t.Errorf("delete error: %v", err)
	}
	verifyItemNotFound(ctx, t, mgr, id3)
	mgr.Flush(ctx)
	verifyItemNotFound(ctx, t, mgr, id3)

	// still found in another
	verifyItem(ctx, t, mgr2, id3, labels3, item3)
	if err := mgr2.loadCommittedBlocksLocked(ctx); err != nil {
		t.Errorf("unable to load: %v", err)
	}

	if err := mgr.Compact(ctx); err != nil {
		t.Errorf("can't compact: %v", err)
	}

	blks, err := mgr.b.ListBlocks(manifestBlockPrefix)
	if err != nil {
		t.Errorf("unable to list manifest blocks: %v", err)
	}
	if got, want := len(blks), 1; got != want {
		t.Errorf("unexpected number of blocks: %v, want %v", got, want)
	}

	mgr.b.Flush(ctx)

	mgr3, err := newManagerForTesting(ctx, t, data)
	if err != nil {
		t.Fatalf("can't open manager: %v", err)
	}

	verifyItem(ctx, t, mgr3, id1, labels1, item1)
	verifyItem(ctx, t, mgr3, id2, labels2, item2)
	verifyItemNotFound(ctx, t, mgr3, id3)
}

func addAndVerify(ctx context.Context, t *testing.T, mgr *Manager, labels map[string]string, data map[string]int) string {
	t.Helper()
	id, err := mgr.Put(ctx, labels, data)
	if err != nil {
		t.Errorf("unable to add %v (%v): %v", labels, data, err)
		return ""
	}

	verifyItem(ctx, t, mgr, id, labels, data)
	return id
}

func verifyItem(ctx context.Context, t *testing.T, mgr *Manager, id string, labels map[string]string, data map[string]int) {
	t.Helper()

	l, err := mgr.GetMetadata(ctx, id)
	if err != nil {
		t.Errorf("unable to retrieve %q: %v", id, err)
		return
	}

	if !reflect.DeepEqual(l.Labels, labels) {
		t.Errorf("invalid labels retrieved %v, wanted %v", l.Labels, labels)
	}
}

func verifyItemNotFound(ctx context.Context, t *testing.T, mgr *Manager, id string) {
	t.Helper()

	_, err := mgr.GetMetadata(ctx, id)
	if got, want := err, ErrNotFound; got != want {
		t.Errorf("invalid error when getting %q %v, expected %v", id, err, ErrNotFound)
		return
	}
}

func verifyMatches(ctx context.Context, t *testing.T, mgr *Manager, labels map[string]string, expected []string) {
	t.Helper()

	var matches []string
	items, err := mgr.Find(ctx, labels)
	if err != nil {
		t.Errorf("error in Find(): %v", err)
		return
	}
	for _, m := range items {
		matches = append(matches, m.ID)
	}
	sort.Strings(matches)
	sort.Strings(expected)

	if !reflect.DeepEqual(matches, expected) {
		t.Errorf("invalid matches for %v: %v, expected %v", labels, matches, expected)
	}
}

func newManagerForTesting(ctx context.Context, t *testing.T, data map[string][]byte) (*Manager, error) {
	st := storagetesting.NewMapStorage(data, nil, nil)

	bm, err := block.NewManager(ctx, st, block.FormattingOptions{
		BlockFormat: "UNENCRYPTED_HMAC_SHA256_128",
		MaxPackSize: 100000,
	}, block.CachingOptions{})
	if err != nil {
		return nil, fmt.Errorf("can't create block manager: %v", err)
	}

	return NewManager(ctx, bm)
}
