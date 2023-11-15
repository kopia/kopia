// Package storj implements Storage based on the Storj distributed storage system.
package storj

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

const (
	blobname     = "testblob-1"
	blobcontents = "a test blob"
	testbucket   = "kopia-test"
)

func TestConnect(t *testing.T) {
	ctx := context.Background()
	accessgrant := getStorjTestConfiguration(t)

	opts := &Options{
		BucketName:  testbucket,
		AccessGrant: accessgrant,
	}

	rs, err := _new(ctx, opts, true)
	if err != nil {
		t.Errorf("expected  New to succeed: %v", err)
	}

	removeTheBucket(t, ctx, rs, testbucket)
}

// TODO(rjk): Would it be nicer to populate the options object in this function?
func getStorjTestConfiguration(t *testing.T) string {
	t.Helper()
	accessgrant := os.Getenv("STORJ_ACCESS_GRANT")
	if accessgrant == "" {
		t.Skip("skipping test because Storj access grant is not set")
	}
	return accessgrant
}

func TestListBlobs(t *testing.T) {
	ctx := context.Background()
	accessgrant := getStorjTestConfiguration(t)

	opts := &Options{
		BucketName:  testbucket,
		AccessGrant: accessgrant,
	}

	storj, err := _new(ctx, opts, true)
	if err != nil {
		t.Fatalf("expected  New to succeed: %v", err)
	}
	defer removeTheBucket(t, ctx, storj, testbucket)

	now := time.Now()
	diffopts := cmpopts.EquateApproxTime(5 * time.Second)
	sortopts := cmpopts.SortSlices(func(a, b blob.Metadata) bool {
		return string(a.BlobID) < string(b.BlobID)
	})

	_mh := func(id string, timestamp time.Time) blob.Metadata {
		return blob.Metadata{
			BlobID:    blob.ID(id),
			Length:    int64(len(blobcontents)),
			Timestamp: timestamp,
		}
	}

	dotshards := blob.Metadata{
		BlobID:    ".shards",
		Length:    int64(43),
		Timestamp: now,
	}

	for _, tv := range []struct {
		want    []blob.Metadata
		prefix  string
		objects []string
	}{
		{
			prefix:  "",
			objects: []string{},
			want:    []blob.Metadata{},
		},
		{
			prefix:  "",
			objects: []string{"a", "b", "c"},
			want: []blob.Metadata{
				_mh("a", now),
				_mh("b", now),
				_mh("c", now),
				dotshards,
			},
		},
		{
			prefix:  "dd",
			objects: []string{"dd1", "dd2", "dd3"},
			want: []blob.Metadata{
				_mh("dd1", now),
				_mh("dd2", now),
				_mh("dd3", now),
			},
		},
		{
			prefix:  "a",
			objects: []string{},
			want: []blob.Metadata{
				_mh("a", now),
			},
		},
		{
			prefix: "",
			objects: []string{
				"pab14b12eb9811fc58da90ed0f6579c13-s750cf660c8f3480f122",
				"pee963b9e844e9bfa6ad0077960a556f7-s750cf660c8f3480f122",
				"xn0_af57ceddd161394455b90db173f5c0d9-s6682f1d36839014a121-c1",
			},
			want: []blob.Metadata{
				dotshards,
				_mh("dd1", now),
				_mh("dd2", now),
				_mh("dd3", now),
				_mh("a", now),
				_mh("b", now),
				_mh("c", now),
				_mh("pab14b12eb9811fc58da90ed0f6579c13-s750cf660c8f3480f122", now),
				_mh("pee963b9e844e9bfa6ad0077960a556f7-s750cf660c8f3480f122", now),
				_mh("xn0_af57ceddd161394455b90db173f5c0d9-s6682f1d36839014a121-c1", now),
			},
		},
		{
			prefix:  "p",
			objects: []string{},
			want: []blob.Metadata{
				_mh("pab14b12eb9811fc58da90ed0f6579c13-s750cf660c8f3480f122", now),
				_mh("pee963b9e844e9bfa6ad0077960a556f7-s750cf660c8f3480f122", now),
			},
		},
		{
			prefix:  "pe",
			objects: []string{},
			want: []blob.Metadata{
				_mh("pee963b9e844e9bfa6ad0077960a556f7-s750cf660c8f3480f122", now),
			},
		},
		{
			prefix:  "xn0",
			objects: []string{},
			want: []blob.Metadata{
				_mh("xn0_af57ceddd161394455b90db173f5c0d9-s6682f1d36839014a121-c1", now),
			},
		},
		{
			prefix:  "xq",
			objects: []string{},
			want:    []blob.Metadata{},
		},
	} {
		got := make([]blob.Metadata, 0)

		for _, k := range tv.objects {
			if err := storj.PutBlob(ctx, blob.ID(k), gather.FromSlice([]byte(blobcontents)), blob.PutOptions{}); err != nil {
				t.Fatalf("expected an error free write to %q: %v", k, err)
			}
		}

		// TODO(rjk): Consider testing that the error is propagated up through my ListBlobs.
		cb := func(b blob.Metadata) error {
			got = append(got, b)
			return nil
		}
		if err := storj.ListBlobs(ctx, blob.ID(tv.prefix), cb); err != nil {
			t.Errorf("ListBlobs unexpected error %v", err)
		}

		want := tv.want
		if diff := cmp.Diff(want, got, diffopts, sortopts); diff != "" {
			t.Errorf("response mismatch (-want +got):\n%s", diff)
		}
	}
}

func TestPutOpts(t *testing.T) {
	ctx := context.Background()
	accessgrant := getStorjTestConfiguration(t)

	opts := &Options{
		BucketName:  testbucket,
		AccessGrant: accessgrant,
	}

	storj, err := _new(ctx, opts, true)
	if err != nil {
		t.Fatalf("expected  New to succeed: %v", err)
	}
	defer removeTheBucket(t, ctx, storj, testbucket)

	// Put the blob.
	if err := storj.PutBlob(ctx, blob.ID(blobname), gather.FromSlice([]byte(blobcontents)), blob.PutOptions{}); err != nil {
		t.Errorf("expected an error free write: %v", err)
	}

	// Put the same blob again. I did not expect this to fail but
	// apparently there is some kind of propagation delay that
	// must be accomodated when over-writing an existing blob.
	if err := storj.PutBlob(ctx, blob.ID(blobname), gather.FromSlice([]byte(blobcontents)), blob.PutOptions{}); err != nil {
		t.Errorf("expected an error free write: %#v", err)
	}

	// Put the same blob again but with DoNotRecreate
	if err := storj.PutBlob(ctx, blob.ID(blobname), gather.FromSlice([]byte(blobcontents)), blob.PutOptions{DoNotRecreate: true}); !errors.Is(err, blob.ErrBlobAlreadyExists) {
		t.Errorf("wrong or nil error: %#v", err)
	}

	if err := storj.PutBlob(ctx, blob.ID("something else"), gather.FromSlice([]byte(blobcontents)), blob.PutOptions{}); err != nil {
		t.Errorf("expected an error free write: %v", err)
	}
}

func TestPutGetBlob(t *testing.T) {
	ctx := context.Background()
	accessgrant := getStorjTestConfiguration(t)

	opts := &Options{
		BucketName:  testbucket,
		AccessGrant: accessgrant,
	}

	storj, err := _new(ctx, opts, true)
	if err != nil {
		t.Fatalf("expected  New to succeed: %v", err)
	}
	defer removeTheBucket(t, ctx, storj, testbucket)

	diffopts := cmpopts.EquateApproxTime(2 * time.Second)
	now := time.Now()

	// Put the blob.
	if err := storj.PutBlob(ctx, blob.ID(blobname), gather.FromSlice([]byte(blobcontents)), blob.PutOptions{}); err != nil {
		t.Errorf("expected an error free write: %v", err)
	}

	// Read the blob metadata
	metadata, err := storj.GetMetadata(ctx, blob.ID(blobname))
	if err != nil {
		t.Errorf("expected an error free GetMetadata for %q: %v", blobname, err)
	}
	got, want := metadata, blob.Metadata{BlobID: blob.ID(blobname), Length: 11, Timestamp: now}
	if diff := cmp.Diff(want, got, diffopts); diff != "" {
		t.Errorf("response mismatch (-want +got):\n%s", diff)
	}

	// Get the blob back.
	wb := gather.NewWriteBuffer()
	if err := storj.GetBlob(ctx, blob.ID(blobname), 0, int64(len(blobcontents)), wb); err != nil {
		t.Fatalf("expected an error-free get: %v", err)
	}
	if got, want := string(wb.ToByteSlice()), blobcontents; got != want {
		t.Errorf("read-back contents don't match. got %v, want %v", got, want)
	}

	// Get the blob back with negative length
	wb = gather.NewWriteBuffer()
	if err := storj.GetBlob(ctx, blob.ID(blobname), 0, -1, wb); err != nil {
		t.Errorf("expected an error-free get: %v", err)
	}
	if got, want := string(wb.ToByteSlice()), blobcontents; got != want {
		t.Errorf("read-back contents don't match. got %v, want %v", got, want)
	}

	// Get the blob back with invalid range (too big)
	wb = gather.NewWriteBuffer()
	if err := storj.GetBlob(ctx, blob.ID(blobname), 0, int64(1000), wb); !errors.Is(err, blob.ErrInvalidRange) {
		t.Errorf("wrong or nil error, got: %#v", err)
	}

	// Get the blob back with invalid range (bad offset)
	wb = gather.NewWriteBuffer()
	if err := storj.GetBlob(ctx, blob.ID(blobname), 300, int64(2), wb); !errors.Is(err, blob.ErrInvalidRange) {
		t.Errorf("wrong or nil error, got: %#v", err)
	}

	wb = gather.NewWriteBuffer()
	if err := storj.GetBlob(ctx, blob.ID("no such blob"), 0, -1, wb); !errors.Is(err, blob.ErrBlobNotFound) {
		t.Errorf("wrong or nil error: %#v", err)
	}

	// Slices work.
	wb = gather.NewWriteBuffer()
	if err := storj.GetBlob(ctx, blob.ID(blobname), 3, int64(4), wb); err != nil {
		t.Errorf("expected an error-free get: %#v", err)
	}
	if got, want := string(wb.ToByteSlice()), blobcontents[3:3+4]; got != want {
		t.Errorf("read-back contents don't match. got %v, want %v", got, want)
	}
}

// Also tests "DeleteBlob" and Close.
func removeTheBucket(t *testing.T, ctx context.Context, rsj *storjStorage, bucket string) {
	t.Helper()

	blobs := []blob.ID{}

	cb := func(b blob.Metadata) error {
		blobs = append(blobs, b.BlobID)
		return nil
	}

	if err := rsj.ListBlobs(ctx, "", cb); err != nil {
		t.Fatalf("removeTheBucket: ListBlobs unexpected error %v", err)
	}

	for _, b := range blobs {
		if err := rsj.DeleteBlob(ctx, b); err != nil {
			t.Errorf("removeTheBucket: DeleteBlob %q failed: %v", string(b), err)
		}
	}

	if err := rsj.deleteBucket(ctx, bucket); err != nil {
		t.Errorf("removeTheBucket: DeleteBucket of %q failed: %v", bucket, err)
	}

	if err := rsj.Close(ctx); err != nil {
		t.Errorf("removeTheBucket: Close failed: %v", err)
	}
}

func (storj *storjStorage) deleteBucket(ctx context.Context, bucket string) error {
	_, err := storj.Storage.Impl.(*impl).project.DeleteBucket(ctx, bucket)
	return err
}
