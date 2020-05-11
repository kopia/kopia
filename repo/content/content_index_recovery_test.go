package content

import (
	"testing"
	"time"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

func TestContentIndexRecovery(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(t, data, keyTime, nil)

	content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
	content2 := writeContentAndVerify(ctx, t, bm, seededRandomData(11, 100))
	content3 := writeContentAndVerify(ctx, t, bm, seededRandomData(12, 100))

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	// delete all index blobs
	assertNoError(t, bm.st.ListBlobs(ctx, indexBlobPrefix, func(bi blob.Metadata) error {
		log(ctx).Debugf("deleting %v", bi.BlobID)
		return bm.st.DeleteBlob(ctx, bi.BlobID)
	}))

	bm.Close(ctx)

	// now with index blobs gone, all contents appear to not be found
	bm = newTestContentManager(t, data, keyTime, nil)
	defer bm.Close(ctx)

	verifyContentNotFound(ctx, t, bm, content1)
	verifyContentNotFound(ctx, t, bm, content2)
	verifyContentNotFound(ctx, t, bm, content3)

	totalRecovered := 0

	// pass 1 - just list contents to recover, but don't commit
	for _, prefix := range PackBlobIDPrefixes {
		err := bm.st.ListBlobs(ctx, prefix, func(bi blob.Metadata) error {
			infos, err := bm.RecoverIndexFromPackBlob(ctx, bi.BlobID, bi.Length, false)
			if err != nil {
				return err
			}
			totalRecovered += len(infos)
			log(ctx).Debugf("recovered %v contents", len(infos))
			return nil
		})
		if err != nil {
			t.Errorf("error recovering: %v", err)
		}
	}

	if got, want := totalRecovered, 3; got != want {
		t.Errorf("invalid # of contents recovered: %v, want %v", got, want)
	}

	// contents are stil not found
	verifyContentNotFound(ctx, t, bm, content1)
	verifyContentNotFound(ctx, t, bm, content2)
	verifyContentNotFound(ctx, t, bm, content3)

	// pass 2 now pass commit=true to add recovered contents to index
	totalRecovered = 0

	for _, prefix := range PackBlobIDPrefixes {
		err := bm.st.ListBlobs(ctx, prefix, func(bi blob.Metadata) error {
			infos, rerr := bm.RecoverIndexFromPackBlob(ctx, bi.BlobID, bi.Length, true)
			if rerr != nil {
				return rerr
			}
			totalRecovered += len(infos)
			log(ctx).Debugf("recovered %v contents", len(infos))
			return nil
		})
		if err != nil {
			t.Errorf("error recovering: %v", err)
		}
	}

	if got, want := totalRecovered, 3; got != want {
		t.Errorf("invalid # of contents recovered: %v, want %v", got, want)
	}

	verifyContent(ctx, t, bm, content1, seededRandomData(10, 100))
	verifyContent(ctx, t, bm, content2, seededRandomData(11, 100))
	verifyContent(ctx, t, bm, content3, seededRandomData(12, 100))

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	verifyContent(ctx, t, bm, content1, seededRandomData(10, 100))
	verifyContent(ctx, t, bm, content2, seededRandomData(11, 100))
	verifyContent(ctx, t, bm, content3, seededRandomData(12, 100))
}
