package content

import (
	"context"
	"math"
	"strings"
	"sync"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

type prefetchOptions struct {
	fullBlobPrefetchCountThreshold int
	fullBlobPrefetchBytesThreshold int64
}

//nolint:gochecknoglobals
var defaultPrefetchOptions = &prefetchOptions{2, 5e6}

//nolint:gochecknoglobals
var prefetchHintToOptions = map[string]*prefetchOptions{
	"":        defaultPrefetchOptions,
	"default": defaultPrefetchOptions,
	"contents": {
		fullBlobPrefetchCountThreshold: math.MaxInt,
		fullBlobPrefetchBytesThreshold: math.MaxInt64,
	},
	"blobs": {
		fullBlobPrefetchCountThreshold: 0,
		fullBlobPrefetchBytesThreshold: 0,
	},
}

func (o *prefetchOptions) shouldPrefetchEntireBlob(infos []Info) bool {
	if len(infos) < o.fullBlobPrefetchCountThreshold {
		return false
	}

	var total int64
	for _, i := range infos {
		total += int64(i.PackedLength)
	}

	return total >= o.fullBlobPrefetchBytesThreshold
}

// PrefetchContents fetches the provided content IDs into the cache.
// Note that due to cache configuration, it's not guaranteed that all contents will
// actually be added to the cache.
//
//nolint:gocyclo
func (bm *WriteManager) PrefetchContents(ctx context.Context, contentIDs []ID, hint string) []ID {
	o := prefetchHintToOptions[hint]
	if o == nil {
		o = defaultPrefetchOptions
	}

	bm.mu.RLock()
	defer bm.mu.RUnlock()

	var (
		prefetched []ID

		// keep track of how contents we'll be fetching from each blob
		contentsByBlob = map[blob.ID][]Info{}
	)

	for _, ci := range contentIDs {
		_, bi, _ := bm.getContentInfoReadLocked(ctx, ci)
		if bi == (Info{}) {
			continue
		}

		contentsByBlob[bi.PackBlobID] = append(contentsByBlob[bi.PackBlobID], bi)
		prefetched = append(prefetched, ci)
	}

	if hint == "none" {
		return prefetched
	}

	type work struct {
		blobID    blob.ID
		contentID ID
	}

	workCh := make(chan work)

	var wg sync.WaitGroup

	go func() {
		defer close(workCh)

		for b, infos := range contentsByBlob {
			if o.shouldPrefetchEntireBlob(infos) {
				workCh <- work{blobID: b}
			} else {
				for _, bi := range infos {
					workCh <- work{contentID: bi.ContentID}
				}
			}
		}
	}()

	for range parallelFetches {
		wg.Add(1)

		go func() {
			defer wg.Done()

			var tmp gather.WriteBuffer
			defer tmp.Close()

			for w := range workCh {
				switch {
				case strings.HasPrefix(string(w.blobID), string(PackBlobIDPrefixRegular)):
					if err := bm.contentCache.PrefetchBlob(ctx, w.blobID); err != nil {
						bm.log.Debugw("error prefetching data blob", "blobID", w.blobID, "err", err)
					}
				case strings.HasPrefix(string(w.blobID), string(PackBlobIDPrefixSpecial)):
					if err := bm.metadataCache.PrefetchBlob(ctx, w.blobID); err != nil {
						bm.log.Debugw("error prefetching metadata blob", "blobID", w.blobID, "err", err)
					}
				case w.contentID != EmptyID:
					tmp.Reset()

					if _, err := bm.getContentDataAndInfo(ctx, w.contentID, &tmp); err != nil {
						bm.log.Debugw("error prefetching content", "contentID", w.contentID, "err", err)
					}
				}
			}
		}()
	}

	wg.Wait()

	return prefetched
}
