package index

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	stderrors "errors"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// MinimumSpillingMemory keeps full output shards above the reader's small-index
// merge threshold (100 entries), even with maximum-length pack IDs: a quarter
// of 1 MiB holds at least 205 entries at the maximum estimated size of 1276 bytes.
const MinimumSpillingMemory int64 = 1 << 20

// ErrSpillingFileMissing indicates that a temporary run or its directory has
// disappeared. The caller can retry with a new builder by replaying its inputs.
// Missing source data and other I/O failures do not produce this error.
var ErrSpillingFileMissing = errors.New("index compaction temporary file disappeared")

func spillingFileError(err error) error {
	if stderrors.Is(err, os.ErrNotExist) {
		return stderrors.Join(ErrSpillingFileMissing, err)
	}

	return err
}

const (
	maxSpillingFanIn    = 16
	maxSpillingIOBuffer = 32 << 10
	// Reserve most of the budget for serialization and slice growth. Input
	// buffers share a quarter of the budget, with at most 1/64 per input.
	spillingWorkingDataDivisor = 4
	spillingInputBufferDivisor = 64
	spillingInfoBaseSize       = 256
	spillingPackIDSizeFactor   = 4
)

// SpillingBuilder prepares content indexes with bounded working memory. When its
// input exceeds the budget, it sorts and merges runs in a private temporary
// directory. Temporary files contain plaintext index metadata. Call Close to
// remove them, including after an error. It is not safe for concurrent use.
//
// The budget is an estimate for the builder's live working data, not a limit on
// process RSS: input index buffers, Go's GC, and the emit callback are separate.
type SpillingBuilder struct {
	items       []Info
	itemsBytes  int64
	bufferLimit int64
	ioSize      int
	fanIn       int
	tempParent  string
	directory   string
	runs        []spillingRun
	built       bool
	closed      bool
	err         error
}

type spillingRun struct {
	path  string
	count int64
	level int
}

// NewSpillingBuilder creates a builder with at least 1 MiB of working memory.
// tempDir is the parent directory for temporary files, or empty for the system
// temporary directory. No files are created unless the input needs to spill.
func NewSpillingBuilder(memoryLimitBytes int64, tempDir string) (*SpillingBuilder, error) {
	if memoryLimitBytes < MinimumSpillingMemory {
		return nil, errors.Errorf("index compaction memory budget must be at least %v bytes", MinimumSpillingMemory)
	}

	ioSize := int(min(int64(maxSpillingIOBuffer), memoryLimitBytes/spillingInputBufferDivisor))

	return &SpillingBuilder{
		// Leave room for slice growth, serialization's dictionaries/output, and
		// bounded input buffers during the final merge.
		bufferLimit: memoryLimitBytes / spillingWorkingDataDivisor,
		ioSize:      ioSize,
		fanIn:       int(min(int64(maxSpillingFanIn), memoryLimitBytes/(spillingWorkingDataDivisor*int64(ioSize)))),
		tempParent:  tempDir,
	}, nil
}

// Add adds an entry. Duplicate content IDs use the same precedence as Builder;
// entries with equal precedence retain the first value added, even across runs.
func (b *SpillingBuilder) Add(ctx context.Context, i Info) error {
	if err := b.ready(ctx); err != nil {
		return err
	}

	if err := validateSpillingInfo(i); err != nil {
		b.err = err
		return err
	}

	size := spillingInfoSize(i)
	if len(b.items) > 0 && b.itemsBytes+size > b.bufferLimit {
		if err := b.flushRun(ctx); err != nil {
			b.err = err
			return err
		}
	}

	// Do not retain a substring backed by a much larger source index buffer.
	i.PackBlobID = blob.ID(strings.Clone(string(i.PackBlobID)))
	b.items = append(b.items, i)
	b.itemsBytes += size

	return nil
}

func (b *SpillingBuilder) ready(ctx context.Context) error {
	switch {
	case b.err != nil:
		return b.err
	case b.closed:
		return errors.New("index compaction builder is closed")
	case b.built:
		return errors.New("index compaction builder has already built its output")
	default:
		return errors.Wrap(ctx.Err(), "index compaction canceled")
	}
}

// spillingInfoSize conservatively accounts for Info, slice capacity, string
// storage, and the dictionaries and encoded bytes needed when building a shard.
func spillingInfoSize(i Info) int64 {
	return spillingInfoBaseSize + spillingPackIDSizeFactor*int64(len(i.PackBlobID))
}

func sortSpillingInfos(items []Info) []Info {
	slices.SortStableFunc(items, func(a, b Info) int {
		if a.ContentID.less(b.ContentID) {
			return -1
		}

		if b.ContentID.less(a.ContentID) {
			return 1
		}

		return 0
	})

	n := 0
	for _, i := range items {
		if n == 0 || items[n-1].ContentID != i.ContentID {
			items[n] = i
			n++
		} else if contentInfoGreaterThanStruct(&i, &items[n-1]) {
			items[n-1] = i
		}
	}

	clear(items[n:])

	return items[:n]
}

func (b *SpillingBuilder) flushRun(ctx context.Context) error {
	if len(b.items) == 0 {
		return nil
	}

	b.items = sortSpillingInfos(b.items)

	run, err := b.writeRun(ctx, func(emit func(Info) error) error {
		for _, i := range b.items {
			if err := emit(i); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	clear(b.items)
	b.items = b.items[:0]
	b.itemsBytes = 0
	b.runs = append(b.runs, run)

	// Compact equal-level adjacent runs while adding input. This bounds even
	// the run metadata to O(log input size), without repeatedly merging a
	// growing run. Runs always cover consecutive intervals of insertion order.
	for len(b.runs) >= b.fanIn {
		first := len(b.runs) - b.fanIn
		if b.runs[first].level != b.runs[len(b.runs)-1].level {
			break
		}

		merged, err := b.mergeToRun(ctx, b.runs[first:])
		if err != nil {
			return err
		}

		b.runs = append(b.runs[:first], merged)
	}

	return nil
}

func (b *SpillingBuilder) writeRun(ctx context.Context, produce func(func(Info) error) error) (run spillingRun, err error) {
	if err := ctx.Err(); err != nil {
		return run, errors.Wrap(err, "index compaction canceled")
	}

	if b.directory == "" {
		b.directory, err = os.MkdirTemp(b.tempParent, "kopia-index-")
		if err != nil {
			return run, errors.Wrap(spillingFileError(err), "creating index compaction temporary directory")
		}
	}

	f, err := os.CreateTemp(b.directory, "run-")
	if err != nil {
		return run, errors.Wrap(spillingFileError(err), "creating index compaction run")
	}
	defer func() {
		err = stderrors.Join(err, errors.Wrap(f.Close(), "closing index compaction run"))
	}()

	run.path = f.Name()
	w := bufio.NewWriterSize(f, b.ioSize)

	var storage [spillingMaxRecordSize]byte
	if err := produce(func(i Info) error {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "index compaction canceled")
		}

		if err := writeSpillingInfo(w, i, storage[:]); err != nil {
			return err
		}

		run.count++

		return nil
	}); err != nil {
		return run, err
	}

	return run, errors.Wrap(w.Flush(), "flushing index compaction run")
}

func (b *SpillingBuilder) mergeToRun(ctx context.Context, runs []spillingRun) (spillingRun, error) {
	run, err := b.writeRun(ctx, func(emit func(Info) error) error {
		return b.mergeRuns(ctx, runs, emit)
	})
	if err != nil {
		return run, err
	}

	for _, input := range runs {
		run.level = max(run.level, input.level+1)
		if err := os.Remove(input.path); err != nil && !stderrors.Is(err, os.ErrNotExist) {
			return run, errors.Wrap(err, "removing merged index compaction run")
		}
	}

	return run, nil
}

// BuildShards consumes the builder and emits stable index shards sequentially.
// Both shardSize (entry count) and the working memory budget bound each shard.
// Bytes passed to emit are only valid until emit returns. No callback occurs for
// empty input. Callers must stage all shards before publishing a complete set.
func (b *SpillingBuilder) BuildShards(ctx context.Context, indexVersion, shardSize int, emit func(gather.Bytes) error) error {
	if err := b.ready(ctx); err != nil {
		return err
	}

	if shardSize <= 0 {
		return errors.New("invalid shard size")
	}

	if indexVersion != Version1 && indexVersion != Version2 {
		return errors.Errorf("unsupported index version: %v", indexVersion)
	}

	if emit == nil {
		return errors.New("missing index compaction output callback")
	}

	b.built = true

	b.err = b.buildShards(ctx, indexVersion, shardSize, emit)

	return b.err
}

func (b *SpillingBuilder) buildShards(ctx context.Context, indexVersion, shardSize int, emit func(gather.Bytes) error) error {
	if len(b.runs) == 0 {
		return b.buildInMemoryShards(ctx, indexVersion, shardSize, emit)
	}

	if err := b.flushRun(ctx); err != nil {
		return err
	}

	b.items = nil

	if err := b.consolidateRuns(ctx); err != nil {
		return err
	}

	var (
		items []Info
		size  int64
	)

	flush := func() error {
		if len(items) == 0 {
			return nil
		}

		if err := emitSpillingShard(ctx, items, indexVersion, emit); err != nil {
			return err
		}

		clear(items)
		items = items[:0]
		size = 0

		return nil
	}

	if err := b.mergeRuns(ctx, b.runs, func(i Info) error {
		if len(items) > 0 && (len(items) >= shardSize || size+spillingInfoSize(i) > b.bufferLimit) {
			if err := flush(); err != nil {
				return err
			}
		}

		items = append(items, i)
		size += spillingInfoSize(i)

		return nil
	}); err != nil {
		return err
	}

	return flush()
}

func (b *SpillingBuilder) buildInMemoryShards(ctx context.Context, indexVersion, shardSize int, emit func(gather.Bytes) error) error {
	b.items = sortSpillingInfos(b.items)
	defer func() { b.items = nil }()

	for len(b.items) > 0 {
		n := min(shardSize, len(b.items))
		if err := emitSpillingShard(ctx, b.items[:n], indexVersion, emit); err != nil {
			return err
		}

		clear(b.items[:n])
		b.items = b.items[n:]
	}

	return errors.Wrap(ctx.Err(), "index compaction canceled")
}

func (b *SpillingBuilder) consolidateRuns(ctx context.Context) error {
	for len(b.runs) > b.fanIn {
		var next []spillingRun
		for start := 0; start < len(b.runs); start += b.fanIn {
			end := min(start+b.fanIn, len(b.runs))
			if end-start == 1 {
				next = append(next, b.runs[start])
				continue
			}

			run, err := b.mergeToRun(ctx, b.runs[start:end])
			if err != nil {
				return err
			}

			next = append(next, run)
		}

		b.runs = next
	}

	return nil
}

func emitSpillingShard(ctx context.Context, items []Info, version int, emit func(gather.Bytes) error) error {
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "index compaction canceled")
	}

	refs := make([]*Info, len(items))
	for n := range items {
		refs[n] = &items[n]
	}

	// A bytes.Buffer avoids gather's minimum pooled allocation for tiny budgets.
	var output bytes.Buffer
	if err := buildSortedContents(refs, &output, version); err != nil {
		return errors.Wrap(err, "building index compaction shard")
	}

	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "index compaction canceled")
	}

	if err := emit(gather.FromSlice(output.Bytes())); err != nil {
		return errors.Wrap(err, "emitting index compaction shard")
	}

	return errors.Wrap(ctx.Err(), "index compaction canceled")
}

// Close releases memory and removes this builder's temporary directory. It may
// be called again if temporary file cleanup fails, for example on Windows.
func (b *SpillingBuilder) Close() error {
	b.closed = true
	b.items = nil

	b.runs = nil
	if b.directory != "" {
		if err := os.RemoveAll(b.directory); err != nil {
			return errors.Wrap(err, "removing index compaction temporary directory")
		}

		b.directory = ""
	}

	return nil
}

type spillingInput struct {
	f         *os.File
	r         *bufio.Reader
	remaining int64
	ordinal   int
	info      Info
	storage   [spillingMaxRecordSize]byte
}

// advance reports whether another record was read. Only the validated end of
// the run returns false without an error; an EOF within a record is corruption.
func (r *spillingInput) advance() (bool, error) {
	if r.remaining == 0 {
		if _, err := r.r.ReadByte(); err == io.EOF {
			return false, nil
		} else if err != nil {
			return false, errors.Wrap(err, "reading index compaction run trailer")
		}

		return false, errors.New("unexpected trailing data in index compaction run")
	}

	i, err := readSpillingInfo(r.r, r.storage[:])
	if err != nil {
		return false, err
	}

	r.info = i
	r.remaining--

	return true, nil
}

type spillingHeap []*spillingInput //nolint:recvcheck // heap.Interface mutates the slice only in Push and Pop.

func (h spillingHeap) Len() int { return len(h) }
func (h spillingHeap) Less(i, j int) bool {
	if h[i].info.ContentID == h[j].info.ContentID {
		// Runs cover consecutive insertion intervals, so this preserves first
		// wins for equal-precedence entries through every merge pass.
		return h[i].ordinal < h[j].ordinal
	}

	return h[i].info.ContentID.less(h[j].info.ContentID)
}
func (h spillingHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *spillingHeap) Push(v any) {
	input := v.(*spillingInput) //nolint:forcetypeassert // This heap contains only spillingInput pointers.
	*h = append(*h, input)
}

func (h *spillingHeap) Pop() any {
	old := *h
	v := old[len(old)-1]
	*h = old[:len(old)-1]

	return v
}

func (b *SpillingBuilder) mergeRuns(ctx context.Context, runs []spillingRun, emit func(Info) error) (err error) {
	var (
		inputs []*spillingInput
		h      spillingHeap
	)
	defer func() {
		for _, input := range inputs {
			err = stderrors.Join(err, errors.Wrap(input.f.Close(), "closing index compaction input run"))
		}
	}()

	for ordinal, run := range runs {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "index compaction canceled")
		}

		f, err := os.Open(run.path)
		if err != nil {
			return errors.Wrap(spillingFileError(err), "opening index compaction input run")
		}

		input := &spillingInput{f: f, r: bufio.NewReaderSize(f, b.ioSize), remaining: run.count, ordinal: ordinal}

		inputs = append(inputs, input)
		if more, err := input.advance(); err != nil {
			return err
		} else if !more {
			continue
		}

		heap.Push(&h, input)
	}

	return mergeSpillingInputs(ctx, h, emit)
}

func mergeSpillingInputs(ctx context.Context, h spillingHeap, emit func(Info) error) error {
	var (
		pending     Info
		havePending bool
	)
	for len(h) > 0 {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "index compaction canceled")
		}

		next := h[0]

		i := next.info
		if !havePending || pending.ContentID != i.ContentID {
			if havePending {
				if err := emit(pending); err != nil {
					return err
				}
			}

			pending = i
			havePending = true
		} else if contentInfoGreaterThanStruct(&i, &pending) {
			pending = i
		}

		if more, err := next.advance(); err != nil {
			return err
		} else if !more {
			heap.Pop(&h)
		} else {
			heap.Fix(&h, 0)
		}
	}

	if havePending {
		return emit(pending)
	}

	return errors.Wrap(ctx.Err(), "index compaction canceled")
}
