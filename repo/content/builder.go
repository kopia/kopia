package content

import (
	"io"
	"runtime"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

// packIndexBuilder prepares and writes content index.
type packIndexBuilder map[ID]Info

// clone returns a deep clone of packIndexBuilder.
func (b packIndexBuilder) clone() packIndexBuilder {
	if b == nil {
		return nil
	}

	r := packIndexBuilder{}

	for k, v := range b {
		r[k] = v
	}

	return r
}

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b packIndexBuilder) Add(i Info) {
	cid := i.GetContentID()

	if old, ok := b[cid]; !ok || i.GetTimestampSeconds() >= old.GetTimestampSeconds() {
		b[cid] = i
	}
}

// base36Value stores a base-36 reverse lookup such that ASCII character corresponds to its
// base-36 value ('0'=0..'9'=9, 'a'=10, 'b'=11, .., 'z'=35).
var base36Value [256]byte

func init() {
	for i := 0; i < 10; i++ {
		base36Value['0'+i] = byte(i)
	}

	for i := 0; i < 26; i++ {
		base36Value['a'+i] = byte(i + 10) //nolint:gomnd
		base36Value['A'+i] = byte(i + 10) //nolint:gomnd
	}
}

// sortedContents returns the list of []Info sorted lexicographically using bucket sort
// sorting is optimized based on the format of content IDs (optional single-character
// alphanumeric prefix (0-9a-z), followed by hexadecimal digits (0-9a-f).
func (b packIndexBuilder) sortedContents() []Info {
	var buckets [36 * 16][]Info

	// phase 1 - bucketize into 576 (36 *16) separate lists
	// by first [0-9a-z] and second character [0-9a-f].
	for cid, v := range b {
		first := int(base36Value[cid[0]])
		second := int(base36Value[cid[1]])

		buck := first<<4 + second //nolint:gomnd

		buckets[buck] = append(buckets[buck], v)
	}

	// phase 2 - sort each non-empty bucket in parallel using goroutines
	// this is much faster than sorting one giant list.
	var wg sync.WaitGroup

	numWorkers := runtime.NumCPU()
	for worker := 0; worker < numWorkers; worker++ {
		worker := worker

		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := range buckets {
				if i%numWorkers == worker {
					buck := buckets[i]

					sort.Slice(buck, func(i, j int) bool {
						return buck[i].GetContentID() < buck[j].GetContentID()
					})
				}
			}
		}()
	}

	wg.Wait()

	// Phase 3 - merge results from all buckets.
	result := make([]Info, 0, len(b))

	for i := 0; i < len(buckets); i++ {
		result = append(result, buckets[i]...)
	}

	return result
}

// Build writes the pack index to the provided output.
func (b packIndexBuilder) Build(output io.Writer, version int) error {
	switch version {
	case v1IndexVersion:
		return b.buildV1(output)

	case v2IndexVersion:
		return b.buildV2(output)

	default:
		return errors.Errorf("unsupported index version: %v", version)
	}
}
