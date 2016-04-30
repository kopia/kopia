package blob

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func TestLoggingStorage(t *testing.T) {
	data := map[string][]byte{}
	r := NewLoggingWrapper(NewMapStorage(data))
	if r == nil {
		t.Errorf("unexpected result: %v", r)
	}
	verifyStorage(t, r)
}

func TestMapStorage(t *testing.T) {
	data := map[string][]byte{}
	r := NewMapStorage(data)
	if r == nil {
		t.Errorf("unexpected result: %v", r)
	}
	verifyStorage(t, r)
}

func TestFileStorage(t *testing.T) {
	// Test varioush shard configurations.
	for _, shardSpec := range [][]int{
		[]int{0},
		[]int{1},
		[]int{3, 3},
		[]int{2},
		[]int{1, 1},
		[]int{1, 2},
		[]int{2, 2, 2},
	} {
		path, _ := ioutil.TempDir("", "r-fs")
		defer os.RemoveAll(path)

		r, err := NewFSStorage(&FSStorageOptions{
			Path:            path,
			DirectoryShards: shardSpec,
		})
		if r == nil || err != nil {
			t.Errorf("unexpected result: %v %v", r, err)
		}
		verifyStorage(t, r)
	}
}

func verifyStorage(t *testing.T, r Storage) {
	blocks := []struct {
		blk      string
		contents []byte
	}{
		{blk: string("abcdbbf4f0507d054ed5a80a5b65086f602b"), contents: []byte{}},
		{blk: string("zxce0e35630770c54668a8cfb4e414c6bf8f"), contents: []byte{1}},
		{blk: string("abff4585856ebf0748fd989e1dd623a8963d"), contents: bytes.Repeat([]byte{1}, 1000)},
		{blk: string("abgc3dca496d510f492c858a2df1eb824e62"), contents: bytes.Repeat([]byte{1}, 10000)},
	}

	// First verify that blocks don't exist.
	for _, b := range blocks {
		if x, err := r.BlockExists(b.blk); x || err != nil {
			t.Errorf("block exists or error: %v %v", b.blk, err)
		}

		data, err := r.GetBlock(b.blk)
		if err != ErrBlockNotFound {
			t.Errorf("unexpected error when calling GetBlock(%v): %v", b.blk, err)
		}
		if data != nil {
			t.Errorf("got data when calling GetBlock(%v): %v", b.blk, data)
		}
	}

	// Now add blocks.
	for _, b := range blocks {
		r.PutBlock(b.blk, ioutil.NopCloser(bytes.NewBuffer(b.contents)), PutOptions{})

		if x, err := r.BlockExists(b.blk); !x || err != nil {
			t.Errorf("block does not exist after adding it: %v %v", b.blk, err)
		}

		data, err := r.GetBlock(b.blk)
		if err != nil {
			t.Errorf("unexpected error when calling GetBlock(%v) after adding: %v", b.blk, err)
		}
		if !bytes.Equal(data, b.contents) {
			t.Errorf("got data when calling GetBlock(%v): %v", b.blk, data)
		}
	}

	// List
	ch := r.ListBlocks(string("ab"))
	e1, ok := <-ch
	if !ok || e1.BlockID != blocks[0].blk {
		t.Errorf("missing result 0")
	}
	e2, ok := <-ch
	if !ok || e2.BlockID != blocks[2].blk {
		t.Errorf("missing result 2")
	}
	e3, ok := <-ch
	if !ok || e3.BlockID != blocks[3].blk {
		t.Errorf("missing result 3")
	}
	e4, ok := <-ch
	if ok {
		t.Errorf("unexpected item: %v", e4)
	}

	if e1.TimeStamp.After(e2.TimeStamp) || e2.TimeStamp.After(e3.TimeStamp) {
		t.Errorf("timings are not sorted: %v %v %v", e1.TimeStamp, e2.TimeStamp, e3.TimeStamp)
	}
}
