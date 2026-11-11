package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
)

func spillingTestInfo(n, version int) Info {
	var hash [16]byte
	binary.BigEndian.PutUint64(hash[8:], uint64(n)) //nolint:gosec

	id, _ := IDFromHash("", hash[:])
	if n%3 == 0 {
		id, _ = IDFromHash("g", hash[:])
	}

	i := Info{
		ContentID:           id,
		TimestampSeconds:    int64(1000 + n%15),
		PackBlobID:          blob.ID(fmt.Sprintf("p%032x", n%113)),
		PackOffset:          uint32(n * 10),  //nolint:gosec
		OriginalLength:      uint32(100 + n), //nolint:gosec
		PackedLength:        uint32(132 + n), //nolint:gosec
		Deleted:             n%7 == 0,
		FormatVersion:       byte(n % 2),
		EncryptionKeyID:     byte(n % 3),
		CompressionHeaderID: compression.HeaderID(n % 5), //nolint:gosec
	}
	if version == Version1 {
		i.EncryptionKeyID = 0
		i.CompressionHeaderID = 0
	}

	return i
}

func spillingShardContents(t *testing.T, data gather.Bytes) []Info {
	t.Helper()

	idx, err := Open(data.ToByteSlice(), nil, func() int { return 32 })
	require.NoError(t, err)

	defer idx.Close()

	var result []Info
	require.NoError(t, idx.Iterate(AllIDs, func(i Info) error {
		result = append(result, i)
		return nil
	}))

	return result
}

func TestSpillingBuilderEquivalent(t *testing.T) {
	for _, version := range []int{Version1, Version2} {
		for _, spill := range []bool{false, true} {
			t.Run(fmt.Sprintf("v%v/spill=%v", version, spill), func(t *testing.T) {
				ctx := context.Background()
				temp := t.TempDir()

				budget := int64(64 << 20)
				if spill {
					budget = MinimumSpillingMemory
				}

				b, err := NewSpillingBuilder(budget, temp)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Close()) })

				if spill {
					b.fanIn = 3 // force several merge levels with a modest test data set
				}

				old := NewOneUseBuilder()

				var maxLevel int
				for pass := range 7 {
					// Interleaved sorted orders and prefixes ensure run boundaries do
					// not align with key boundaries. Later passes include exact ties
					// whose other fields differ, deletions, and superseding records.
					for n := range 1200 {
						i := spillingTestInfo((n*7919)%1200, version)
						i = spillingTestVersion(i, pass, n)
						old.Add(i)
						require.NoError(t, b.Add(ctx, i))
						require.LessOrEqual(t, b.itemsBytes, b.bufferLimit)

						for _, run := range b.runs {
							maxLevel = max(maxLevel, run.level)
						}
					}
				}

				want := map[ID]Info{}
				oldShards, cleanup, err := old.BuildShards(version, true, 10000)
				require.NoError(t, err)

				defer cleanup()

				for _, data := range oldShards {
					for _, i := range spillingShardContents(t, data) {
						want[i.ContentID] = i
					}
				}

				got := map[ID]Info{}

				var previous ID
				require.NoError(t, b.BuildShards(ctx, version, 97, func(data gather.Bytes) error {
					contents := spillingShardContents(t, data)
					require.LessOrEqual(t, len(contents), 97)

					var estimated int64
					for _, i := range contents {
						if len(got) > 0 {
							require.True(t, previous.less(i.ContentID), "shards must have strictly increasing unique keys")
						}

						previous = i.ContentID
						got[i.ContentID] = i
						estimated += spillingInfoSize(i)
					}

					require.LessOrEqual(t, estimated, b.bufferLimit)

					return nil
				}))
				require.Equal(t, want, got)

				if spill {
					require.GreaterOrEqual(t, maxLevel, 2)
				} else {
					require.Empty(t, b.directory)
				}

				require.NoError(t, b.Close())
				require.NoError(t, b.Close())

				entries, err := os.ReadDir(temp)
				require.NoError(t, err)
				require.Empty(t, entries)
			})
		}
	}
}

func spillingTestVersion(i Info, pass, n int) Info {
	switch pass {
	case 1:
		i.PackOffset++ // equal comparison: retain the earlier record
	case 2:
		i.Deleted = true
	case 3:
		i.TimestampSeconds += int64(n % 2)
	case 4:
		i.PackBlobID = "pz" + i.PackBlobID
	case 5:
		i.TimestampSeconds--
	case 6:
		i.PackOffset += 99
	}

	return i
}

func TestSpillingBuilderFirstWinsAcrossMergeLevels(t *testing.T) {
	b, err := NewSpillingBuilder(MinimumSpillingMemory, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	b.fanIn = 2
	ctx := context.Background()
	first := spillingTestInfo(1, Version2)
	require.NoError(t, b.Add(ctx, first))

	for n := range 2000 {
		i := first
		i.PackOffset = uint32(n + 100) //nolint:gosec
		i.EncryptionKeyID++
		require.NoError(t, b.Add(ctx, i))
	}

	var got []Info
	require.NoError(t, b.BuildShards(ctx, Version2, 100, func(data gather.Bytes) error {
		got = append(got, spillingShardContents(t, data)...)
		return nil
	}))
	require.Equal(t, []Info{first}, got)
}

func TestSpillingBuilderEmptyAndInvalidArguments(t *testing.T) {
	_, err := NewSpillingBuilder(0, "")
	require.Error(t, err)
	_, err = NewSpillingBuilder(MinimumSpillingMemory-1, "")
	require.Error(t, err)
	b, err := NewSpillingBuilder(MinimumSpillingMemory, t.TempDir())
	require.NoError(t, err)

	defer b.Close()

	ctx := context.Background()
	emit := func(gather.Bytes) error {
		t.Fatal("empty input must not emit an index")
		return nil
	}
	require.Error(t, b.BuildShards(ctx, Version2, 0, emit))
	require.Error(t, b.BuildShards(ctx, Version2, -1, emit))
	require.Error(t, b.BuildShards(ctx, 99, 1, emit))
	require.Error(t, b.BuildShards(ctx, Version2, 1, nil))
	require.NoError(t, b.BuildShards(ctx, Version2, 1, emit))
	require.Empty(t, b.directory)
	require.Error(t, b.Add(ctx, spillingTestInfo(1, Version2)))
	require.Error(t, b.BuildShards(ctx, Version2, 1, emit))
}

func newSpilledTestBuilder(t *testing.T) (*SpillingBuilder, string) {
	t.Helper()
	temp := t.TempDir()
	b, err := NewSpillingBuilder(MinimumSpillingMemory, temp)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	for n := range 1500 {
		require.NoError(t, b.Add(context.Background(), spillingTestInfo(n, Version2)))
	}

	require.NotEmpty(t, b.runs)

	return b, temp
}

func TestSpillingBuilderCancellationAndEmitError(t *testing.T) {
	for _, cancel := range []bool{true, false} {
		t.Run(fmt.Sprintf("cancel=%v", cancel), func(t *testing.T) {
			b, temp := newSpilledTestBuilder(t)

			ctx, stop := context.WithCancel(context.Background())
			defer stop()

			failure := errors.New("output disk full")
			if cancel {
				failure = context.Canceled
			}

			calls := 0
			err := b.BuildShards(ctx, Version2, 10, func(gather.Bytes) error {
				calls++

				if cancel {
					stop()
					return nil
				}

				return failure
			})
			require.ErrorIs(t, err, failure)
			require.Equal(t, 1, calls)
			require.NoError(t, b.Close())

			entries, err := os.ReadDir(temp)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}

	b, _ := newSpilledTestBuilder(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, b.Add(ctx, spillingTestInfo(1, Version2)), context.Canceled)
	require.ErrorIs(t, b.BuildShards(ctx, Version2, 10, func(gather.Bytes) error { return nil }), context.Canceled)
}

func TestSpillingBuilderRunCorruption(t *testing.T) {
	for _, damage := range []string{"truncate-header", "truncate-record", "remove-record", "checksum", "trailing", "missing"} {
		t.Run(damage, func(t *testing.T) {
			b, temp := newSpilledTestBuilder(t)
			path := b.runs[0].path
			data, err := os.ReadFile(path)
			require.NoError(t, err)

			switch damage {
			case "truncate-header":
				data = data[:5]
			case "truncate-record":
				data = data[:len(data)-1]
			case "remove-record":
				data = nil
			case "checksum":
				data[4] ^= 1
			case "trailing":
				data = append(data, 1)
			}

			if damage == "missing" {
				require.NoError(t, os.Remove(path))
			} else {
				require.NoError(t, os.WriteFile(path, data, 0o600))
			}

			require.Error(t, b.BuildShards(context.Background(), Version2, 1000, func(gather.Bytes) error { return nil }))
			require.NoError(t, b.Close())

			entries, err := os.ReadDir(temp)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestSpillingBuilderTemporaryFileFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	b, err := NewSpillingBuilder(MinimumSpillingMemory, path)
	require.NoError(t, err)

	defer b.Close()

	for n := range 1000 {
		err = b.Add(context.Background(), spillingTestInfo(n, Version2))
		if err != nil {
			break
		}
	}

	require.ErrorContains(t, err, "temporary directory")
	require.Equal(t, err, b.Add(context.Background(), spillingTestInfo(1, Version2)))
	require.Equal(t, err, b.BuildShards(context.Background(), Version2, 100, func(gather.Bytes) error { return nil }))
}

func TestSpillingBuilderOwnsInputAndPropagatesFormatErrors(t *testing.T) {
	b, err := NewSpillingBuilder(MinimumSpillingMemory, t.TempDir())
	require.NoError(t, err)

	defer b.Close()

	i := spillingTestInfo(37, Version2)
	want := i
	require.NoError(t, b.Add(context.Background(), i))
	i.PackOffset++
	i.PackBlobID = "changed"

	require.NoError(t, b.BuildShards(context.Background(), Version2, 100, func(data gather.Bytes) error {
		require.Equal(t, []Info{want}, spillingShardContents(t, data))
		return nil
	}))

	spilled, _ := newSpilledTestBuilder(t)
	called := false
	err = spilled.BuildShards(context.Background(), Version1, 1000, func(gather.Bytes) error {
		called = true
		return nil
	})
	require.Error(t, err) // v1 cannot encode v2 compression/key fields
	require.False(t, called)
}

type spillingErrorWriter struct{ err error }

func (w spillingErrorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestSpillingRecordRoundTripAndIOErrors(t *testing.T) {
	i := spillingTestInfo(37, Version2)
	i.TimestampSeconds = -9223372036854775807
	i.OriginalLength = ^uint32(0)
	i.PackedLength = ^uint32(0)
	i.PackOffset = ^uint32(0)
	i.EncryptionKeyID = 254
	i.FormatVersion = 254
	i.CompressionHeaderID = ^compression.HeaderID(0)
	i.Deleted = true
	i.PackBlobID = blob.ID(strings.Repeat("x", spillingMaxPackID))
	i.ContentID, _ = IDFromHash("z", bytes.Repeat([]byte{0xff}, maxIDDataLength))

	var (
		buf     bytes.Buffer
		storage [spillingMaxRecordSize]byte
	)
	require.NoError(t, writeSpillingInfo(&buf, i, storage[:]))
	got, err := readSpillingInfo(&buf, storage[:])
	require.NoError(t, err)
	require.Equal(t, i, got)

	_, err = readSpillingInfo(&buf, storage[:])
	require.ErrorIs(t, err, io.EOF)

	failure := errors.New("disk full")
	require.ErrorIs(t, writeSpillingInfo(spillingErrorWriter{failure}, i, storage[:]), failure)
	require.ErrorIs(t, writeSpillingInfo(spillingErrorWriter{nil}, i, storage[:]), io.ErrShortWrite)
}

func BenchmarkSpillingBuilder(b *testing.B) {
	for _, entries := range []int{100000, 1000000} {
		b.Run(fmt.Sprintf("entries=%v", entries), func(b *testing.B) {
			benchmarkSpillingBuilder(b, entries)
		})
	}
}

func benchmarkSpillingBuilder(b *testing.B, entries int) {
	b.Helper()

	for _, budget := range []int64{1 << 20, 64 << 20} {
		b.Run(fmt.Sprintf("budget=%vMiB", budget>>20), func(b *testing.B) {
			temp := b.TempDir()
			b.ReportAllocs()

			for range b.N {
				builder, err := NewSpillingBuilder(budget, temp)
				if err != nil {
					b.Fatal(err)
				}

				var maxBatch int64
				for n := range entries {
					if err := builder.Add(context.Background(), spillingTestInfo(n, Version2)); err != nil {
						b.Fatal(err)
					}

					maxBatch = max(maxBatch, builder.itemsBytes)
				}

				if err := builder.BuildShards(context.Background(), Version2, entries, func(gather.Bytes) error { return nil }); err != nil {
					b.Fatal(err)
				}

				if err := builder.Close(); err != nil {
					b.Fatal(err)
				}

				b.ReportMetric(float64(maxBatch), "max-batch-estimated-B")
			}
		})
	}

	b.Run("one-use", func(b *testing.B) {
		b.ReportAllocs()

		for range b.N {
			builder := NewOneUseBuilder()
			for n := range entries {
				builder.Add(spillingTestInfo(n, Version2))
			}

			_, closeShards, err := builder.BuildShards(Version2, true, entries)
			if err != nil {
				b.Fatal(err)
			}

			closeShards()
		}
	})
}
