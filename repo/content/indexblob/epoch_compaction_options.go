package indexblob

import (
	"context"

	"github.com/kopia/kopia/repo/content/index"
)

// MinimumEpochCompactionMemoryLimit prevents full output shards small enough to
// trigger unbounded in-memory recombination by repository readers.
const MinimumEpochCompactionMemoryLimit = index.MinimumSpillingMemory

// DefaultEpochCompactionMemoryLimit is the working-memory budget for index
// compaction. It does not include repository caches, input blob decryption,
// the Go runtime, or other concurrent operations.
const DefaultEpochCompactionMemoryLimit int64 = 512 << 20

// EpochCompactionOptions controls local resources used to compact indexes.
// These options do not change the repository format or epoch safety rules.
type EpochCompactionOptions struct {
	// MemoryLimitBytes is a working-memory budget, not a process RSS limit.
	// Zero uses the manager's local setting, or DefaultEpochCompactionMemoryLimit.
	// Only applies when the manager has a cache directory.
	MemoryLimitBytes int64
}

type epochCompactionOptionsKey struct{}

// WithEpochCompactionOptions sets the resource options for compactions started
// with ctx, including both single-epoch and range checkpoint compactions.
func WithEpochCompactionOptions(ctx context.Context, opt EpochCompactionOptions) context.Context {
	return context.WithValue(ctx, epochCompactionOptionsKey{}, opt)
}

func epochCompactionOptionsFromContext(ctx context.Context, localMemoryLimit int64) EpochCompactionOptions {
	opt, _ := ctx.Value(epochCompactionOptionsKey{}).(EpochCompactionOptions)
	if opt.MemoryLimitBytes == 0 {
		opt.MemoryLimitBytes = localMemoryLimit
	}

	if opt.MemoryLimitBytes == 0 {
		opt.MemoryLimitBytes = DefaultEpochCompactionMemoryLimit
	}

	return opt
}
