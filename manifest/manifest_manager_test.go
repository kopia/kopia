package manifest

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kopia/repo/block"
	"github.com/kopia/repo/internal/storagetesting"
	"github.com/pkg/errors"
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

func TestManifestInitCorruptedBlock(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	st := storagetesting.NewMapStorage(data, nil, nil)

	f := block.FormattingOptions{
		Hash:        "HMAC-SHA256-128",
		Encryption:  "NONE",
		MaxPackSize: 100000,
	}

	// write some data to storage
	bm, err := block.NewManager(ctx, st, f, block.CachingOptions{}, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	mgr, err := NewManager(ctx, bm)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	mgr.Put(ctx, map[string]string{"type": "foo"}, map[string]string{"some": "value"}) //nolint:errcheck
	mgr.Flush(ctx)
	bm.Flush(ctx)

	// corrupt data at the storage level.
	for k, v := range data {
		if strings.HasPrefix(k, "p") {
			for i := 0; i < len(v); i++ {
				v[i] ^= 1
			}
		}
	}

	// make a new block manager based on corrupted data.
	bm, err = block.NewManager(ctx, st, f, block.CachingOptions{}, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	mgr, err = NewManager(ctx, bm)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	cases := []struct {
		desc string
		f    func() error
	}{
		{"GetRaw", func() error { _, err := mgr.GetRaw(ctx, "anything"); return err }},
		{"GetMetadata", func() error { _, err := mgr.GetMetadata(ctx, "anything"); return err }},
		{"Get", func() error { return mgr.Get(ctx, "anything", nil) }},
		{"Delete", func() error { return mgr.Delete(ctx, "anything") }},
		{"Find", func() error { _, err := mgr.Find(ctx, nil); return err }},
		{"Put", func() error {
			_, err := mgr.Put(ctx, map[string]string{
				"type": "foo",
			}, map[string]string{
				"some": "value",
			})
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.f()
			if err == nil || !strings.Contains(err.Error(), "invalid checksum") {
				t.Errorf("invalid error when initializing malformed manifest manager: %v", err)
			}
		})
	}
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

	var d2 map[string]int
	if err := mgr.Get(ctx, id, &d2); err != nil {
		t.Errorf("Get failed: %v", err)
	}

	if !reflect.DeepEqual(d2, data) {
		t.Errorf("invalid data retrieved %v, wanted %v", d2, data)
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
		Hash:        "HMAC-SHA256-128",
		Encryption:  "NONE",
		MaxPackSize: 100000,
	}, block.CachingOptions{}, nil)
	if err != nil {
		return nil, errors.Wrap(err, "can't create block manager")
	}

	return NewManager(ctx, bm)
}

func TestManifestInvalidPut(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	mgr, setupErr := newManagerForTesting(ctx, t, data)
	if setupErr != nil {
		t.Fatalf("unable to open block manager: %v", setupErr)
	}

	cases := []struct {
		labels        map[string]string
		payload       interface{}
		expectedError string
	}{
		{map[string]string{"": ""}, "xxx", "'type' label is required"},
		{map[string]string{"type": "blah"}, complex128(1), "marshal error"},
	}

	for i, tc := range cases {
		_, err := mgr.Put(ctx, tc.labels, tc.payload)
		if err == nil || !strings.Contains(err.Error(), tc.expectedError) {
			t.Errorf("invalid error when putting case %v: %v, expected %v", i, err, tc.expectedError)
		}
	}
}

func TestManifestAutoCompaction(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}

	for i := 0; i < 100; i++ {
		mgr, setupErr := newManagerForTesting(ctx, t, data)
		if setupErr != nil {
			t.Fatalf("unable to open block manager: %v", setupErr)
		}

		item1 := map[string]int{"foo": 1, "bar": 2}
		labels1 := map[string]string{"type": "item", "color": "red"}
		addAndVerify(ctx, t, mgr, labels1, item1)
		mgr.Flush(ctx)
	}
}
