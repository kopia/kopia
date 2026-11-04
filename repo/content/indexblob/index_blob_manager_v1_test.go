package indexblob

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/completeset"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/format"
)

// Use a small budget to exercise disk runs and multiple output shards with real
// encrypted blobs, without requiring a production-sized repository.
const testEpochCompactionMemory = MinimumEpochCompactionMemoryLimit

func newEpochCompactionTestManager(t *testing.T, version int) (*ManagerV1, *blobtesting.FaultyStorage) {
	t.Helper()

	st := blobtesting.NewFaultyStorage(blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil))
	v0 := newIndexBlobManagerForTesting(t, st, clock.Now)

	return NewManagerV1(st, v0.enc, nil, clock.Now, &format.ContentFormat{
		IndexVersion: version,
	}, nil, "", 0), st
}

func epochCompactionTestEntries(t *testing.T, count int) []index.Info {
	t.Helper()

	result := make([]index.Info, count)
	for i := range result {
		id, err := index.ParseID(fmt.Sprintf("%032x", i+1))
		require.NoError(t, err)

		result[i] = index.Info{
			ContentID:        id,
			PackBlobID:       blob.ID(fmt.Sprintf("p%032x", i%8)),
			TimestampSeconds: 1700000000 + int64(i%10),
			PackedLength:     1052,
			OriginalLength:   1024,
			PackOffset:       uint32(i * 2048), //nolint:gosec
			FormatVersion:    1,
			Deleted:          i%13 == 0,
		}
	}

	return result
}

func writeEpochCompactionTestBlob(t *testing.T, ctx context.Context, m *ManagerV1, version int, entries []index.Info) blob.ID {
	t.Helper()

	b := index.NewOneUseBuilder()
	for _, ent := range entries {
		b.Add(ent)
	}

	shards, cleanup, err := b.BuildShards(version, true, DefaultIndexShardSize)
	require.NoError(t, err)

	defer cleanup()

	require.Len(t, shards, 1)

	bm, err := m.enc.EncryptAndWriteBlob(ctx, shards[0], "xn0_", "")
	require.NoError(t, err)

	return bm.BlobID
}

func readEpochCompactionTestBlob(t *testing.T, ctx context.Context, m *ManagerV1, id blob.ID) []index.Info {
	t.Helper()

	var data gather.WriteBuffer
	defer data.Close()

	require.NoError(t, m.enc.GetEncryptedBlob(ctx, id, &data))

	ndx, err := index.Open(data.ToByteSlice(), nil, m.enc.crypter.Encryptor().Overhead)
	require.NoError(t, err)

	defer ndx.Close()

	var result []index.Info
	require.NoError(t, ndx.Iterate(index.AllIDs, func(ent index.Info) error {
		result = append(result, ent)
		return nil
	}))

	return result
}

func epochCompactionExpected(t *testing.T, ctx context.Context, m *ManagerV1, ids []blob.ID, version int) map[index.ID]index.Info {
	t.Helper()

	b := index.NewOneUseBuilder()
	for _, id := range ids {
		for _, ent := range readEpochCompactionTestBlob(t, ctx, m, id) {
			b.Add(ent)
		}
	}

	shards, cleanup, err := b.BuildShards(version, true, DefaultIndexShardSize)
	require.NoError(t, err)

	defer cleanup()

	result := map[index.ID]index.Info{}
	for _, shard := range shards {
		ndx, err := index.Open(shard.ToByteSlice(), nil, m.enc.crypter.Encryptor().Overhead)
		require.NoError(t, err)
		require.NoError(t, ndx.Iterate(index.AllIDs, func(ent index.Info) error {
			result[ent.ContentID] = ent
			return nil
		}))
		require.NoError(t, ndx.Close())
	}

	return result
}

func requireEpochCompactionTempEmpty(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(filepath.Join(dir, "index-compaction"))
	require.NoError(t, err)
	require.Empty(t, entries, "compaction must release temporary files, including on Windows")
}

func requireEpochCompactionOriginals(t *testing.T, ctx context.Context, m *ManagerV1, ids []blob.ID) {
	t.Helper()

	for _, id := range ids {
		_, err := m.st.GetMetadata(ctx, id)
		require.NoError(t, err, "compaction must leave original indexes for epoch cleanup")
	}
}

func TestCompactEpochBoundedMatchesExistingBuilder(t *testing.T) {
	for _, version := range []int{index.Version1, index.Version2} {
		t.Run(fmt.Sprintf("version-%v", version), func(t *testing.T) {
			ctx := testlogging.Context(t)
			m, _ := newEpochCompactionTestManager(t, version)
			entries := epochCompactionTestEntries(t, 768)
			ids := []blob.ID{writeEpochCompactionTestBlob(t, ctx, m, version, entries)}

			// Repeated IDs arrive in later input blobs after many disk runs. Cover
			// newer timestamps, delete/undelete ties, pack-ID ties and exact ties.
			for pass := range 3 {
				for i := range entries {
					switch i % 4 {
					case 0:
						entries[i].TimestampSeconds++
						entries[i].Deleted = pass%2 == 0
					case 1:
						entries[i].Deleted = pass%2 == 0
					case 2:
						entries[i].PackBlobID = blob.ID(fmt.Sprintf("p%032x", 100+pass))
					case 3:
						// Equal comparison fields preserve the first occurrence.
						entries[i].PackOffset++
					}
				}

				ids = append(ids, writeEpochCompactionTestBlob(t, ctx, m, version, entries))
			}

			want := epochCompactionExpected(t, ctx, m, ids, version)
			tempDir := t.TempDir()
			m.cacheDirectory = tempDir
			ctx = WithEpochCompactionOptions(ctx, EpochCompactionOptions{MemoryLimitBytes: testEpochCompactionMemory})
			require.NoError(t, m.CompactEpoch(ctx, ids, "xs0_"))

			outputs, err := blob.ListAllBlobs(ctx, m.st, "xs0_")
			require.NoError(t, err)
			require.Greater(t, len(outputs), 1)
			sets := completeset.FindAll(outputs)
			require.Len(t, sets, 1)
			require.Len(t, sets[0], len(outputs))

			got := map[index.ID]index.Info{}

			var session string
			for _, output := range outputs {
				parts := strings.Split(string(output.BlobID), "-")
				require.Len(t, parts, 3)
				require.Equal(t, fmt.Sprintf("c%d", len(outputs)), parts[2])

				if session == "" {
					session = parts[1]
				}

				require.Equal(t, session, parts[1])

				for _, ent := range readEpochCompactionTestBlob(t, ctx, m, output.BlobID) {
					_, exists := got[ent.ContentID]
					require.False(t, exists, "an ID must only appear in one output shard")

					got[ent.ContentID] = ent
				}
			}

			require.Equal(t, want, got)
			requireEpochCompactionOriginals(t, ctx, m, ids)
			requireEpochCompactionTempEmpty(t, tempDir)
		})
	}
}

func TestCompactEpochBoundedUploadFailureAndRetry(t *testing.T) {
	ctx := testlogging.Context(t)
	m, st := newEpochCompactionTestManager(t, index.Version2)
	ids := []blob.ID{writeEpochCompactionTestBlob(t, ctx, m, index.Version2, epochCompactionTestEntries(t, 768))}
	tempDir := t.TempDir()
	m.cacheDirectory = tempDir
	ctx = WithEpochCompactionOptions(ctx, EpochCompactionOptions{MemoryLimitBytes: testEpochCompactionMemory})

	// One real upload succeeds, then the next fails before being persisted.
	wantErr := errors.New("upload failed")

	st.AddFault(blobtesting.MethodPutBlob).Before(func() {})
	st.AddFault(blobtesting.MethodPutBlob).ErrorInstead(wantErr)
	require.ErrorIs(t, m.CompactEpoch(ctx, ids, "xs0_"), wantErr)
	partial, err := blob.ListAllBlobs(ctx, st, "xs0_")
	require.NoError(t, err)
	require.Len(t, partial, 1)
	require.Empty(t, completeset.FindAll(partial))
	requireEpochCompactionTempEmpty(t, tempDir)
	requireEpochCompactionOriginals(t, ctx, m, ids)

	require.NoError(t, m.CompactEpoch(ctx, ids, "xs0_"))
	all, err := blob.ListAllBlobs(ctx, st, "xs0_")
	require.NoError(t, err)

	sets := completeset.FindAll(all)
	require.Len(t, sets, 1)
	require.Len(t, all, len(sets[0])+1)

	failedSession := strings.Split(string(partial[0].BlobID), "-")[1]
	for _, bm := range sets[0] {
		require.NotEqual(t, failedSession, strings.Split(string(bm.BlobID), "-")[1])
	}

	requireEpochCompactionTempEmpty(t, tempDir)
	requireEpochCompactionOriginals(t, ctx, m, ids)
}

func TestCompactEpochBoundedInputFailuresPublishNothing(t *testing.T) {
	for _, kind := range []string{"missing", "invalid-header", "invalid-iteration"} {
		t.Run(kind, func(t *testing.T) {
			ctx := testlogging.Context(t)
			m, _ := newEpochCompactionTestManager(t, index.Version2)
			ids := []blob.ID{writeEpochCompactionTestBlob(t, ctx, m, index.Version2, epochCompactionTestEntries(t, 768))}

			badID := blob.ID("xn0_missing")
			if kind != "missing" {
				data := []byte("invalid index")
				if kind == "invalid-iteration" {
					// A V1 header with one entry, but only enough bytes for its key.
					// Opening succeeds; iteration must propagate the truncated-entry error.
					data = make([]byte, 25)
					data[0], data[1] = index.Version1, 17
					binary.BigEndian.PutUint16(data[2:4], 20)
					binary.BigEndian.PutUint32(data[4:8], 1)
					ndx, err := index.Open(data, nil, m.enc.crypter.Encryptor().Overhead)
					require.NoError(t, err)
					require.Error(t, ndx.Iterate(index.AllIDs, func(index.Info) error { return nil }))
					require.NoError(t, ndx.Close())
				}

				bm, err := m.enc.EncryptAndWriteBlob(ctx, gather.FromSlice(data), "xn0_", "")
				require.NoError(t, err)

				badID = bm.BlobID
			}

			tempDir := t.TempDir()
			m.cacheDirectory = tempDir
			ctx = WithEpochCompactionOptions(ctx, EpochCompactionOptions{MemoryLimitBytes: testEpochCompactionMemory})
			require.Error(t, m.CompactEpoch(ctx, append(ids, badID), "xs0_"))
			outputs, err := blob.ListAllBlobs(ctx, m.st, "xs0_")
			require.NoError(t, err)
			require.Empty(t, outputs)
			requireEpochCompactionTempEmpty(t, tempDir)
			requireEpochCompactionOriginals(t, ctx, m, ids)
		})
	}
}

func TestCompactEpochBoundedTemporaryDirectoryFailure(t *testing.T) {
	ctx := testlogging.Context(t)
	m, _ := newEpochCompactionTestManager(t, index.Version2)
	ids := []blob.ID{writeEpochCompactionTestBlob(t, ctx, m, index.Version2, epochCompactionTestEntries(t, 768))}
	tempDir := t.TempDir()
	m.cacheDirectory = tempDir
	filePath := filepath.Join(tempDir, "not-a-directory")
	require.NoError(t, os.WriteFile(filePath, []byte("keep"), 0o600))
	m.cacheDirectory = filePath
	ctx = WithEpochCompactionOptions(ctx, EpochCompactionOptions{MemoryLimitBytes: testEpochCompactionMemory})
	require.Error(t, m.CompactEpoch(ctx, ids, "xs0_"))
	outputs, err := blob.ListAllBlobs(ctx, m.st, "xs0_")
	require.NoError(t, err)
	require.Empty(t, outputs)
	requireEpochCompactionOriginals(t, ctx, m, ids)

	data, err := os.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, "keep", string(data))
}

func TestCompactEpochTemporaryLocation(t *testing.T) {
	for _, useCache := range []bool{true, false} {
		t.Run(fmt.Sprintf("cache-%v", useCache), func(t *testing.T) {
			ctx := testlogging.Context(t)
			m, st := newEpochCompactionTestManager(t, index.Version2)

			osTemp := t.TempDir()
			for _, name := range []string{"TMP", "TEMP", "TMPDIR"} {
				t.Setenv(name, osTemp)
			}

			expected := os.TempDir()
			if useCache {
				m.cacheDirectory = filepath.Join(t.TempDir(), "cache")
				expected = filepath.Join(m.cacheDirectory, "index-compaction")
			}

			id := writeEpochCompactionTestBlob(t, ctx, m, index.Version2, epochCompactionTestEntries(t, 768))

			st.AddFault(blobtesting.MethodGetBlob).Before(func() {})
			st.AddFault(blobtesting.MethodGetBlob).Before(func() {
				entries, err := os.ReadDir(expected)
				require.NoError(t, err)

				if useCache {
					// The first input must already have spilled in the cache directory.
					require.NotEmpty(t, entries)
					entries, err = os.ReadDir(os.TempDir())
					require.NoError(t, err)
					require.Empty(t, entries)
				} else {
					require.Empty(t, entries, "compaction without caching must remain in memory")
				}
			})

			ctx = WithEpochCompactionOptions(ctx, EpochCompactionOptions{MemoryLimitBytes: testEpochCompactionMemory})
			require.NoError(t, m.CompactEpoch(ctx, []blob.ID{id, id}, "xs0_"))
			outputs, err := blob.ListAllBlobs(ctx, st, "xs0_")
			require.NoError(t, err)

			if useCache {
				require.Greater(t, len(outputs), 1)
			} else {
				require.Len(t, outputs, 1, "the small override must not switch to the spilling builder")
			}

			require.Equal(t,
				epochCompactionExpected(t, ctx, m, []blob.ID{id}, index.Version2),
				epochCompactionExpected(t, ctx, m, blob.IDsFromMetadata(outputs), index.Version2))

			entries, err := os.ReadDir(expected)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestCompactEpochLocalBudgetAndOverride(t *testing.T) {
	for _, tc := range []struct {
		name            string
		local, override int64
		spills          bool
	}{
		{name: "default"},
		{name: "saved-budget", local: testEpochCompactionMemory, spills: true},
		{name: "larger-override", local: testEpochCompactionMemory, override: 2 * testEpochCompactionMemory},
		{name: "smaller-override", local: 2 * testEpochCompactionMemory, override: testEpochCompactionMemory, spills: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testlogging.Context(t)
			m, st := newEpochCompactionTestManager(t, index.Version2)
			// Use the public constructor so this also checks the local option is retained.
			m = NewManagerV1(st, m.enc, nil, clock.Now, m.formattingOptions, nil, t.TempDir(), tc.local)

			id := writeEpochCompactionTestBlob(t, ctx, m, index.Version2, epochCompactionTestEntries(t, 768))
			if tc.override != 0 {
				ctx = WithEpochCompactionOptions(ctx, EpochCompactionOptions{MemoryLimitBytes: tc.override})
			}

			require.NoError(t, m.CompactEpoch(ctx, []blob.ID{id}, "xs0_"))
			outputs, err := blob.ListAllBlobs(ctx, st, "xs0_")
			require.NoError(t, err)

			if tc.spills {
				require.Greater(t, len(outputs), 1)
			} else {
				require.Len(t, outputs, 1)
			}

			require.Equal(t,
				epochCompactionExpected(t, ctx, m, []blob.ID{id}, index.Version2),
				epochCompactionExpected(t, ctx, m, blob.IDsFromMetadata(outputs), index.Version2))
			requireEpochCompactionTempEmpty(t, m.cacheDirectory)
		})
	}
}

func TestCompactEpochBoundedCanceledAfterSpilling(t *testing.T) {
	baseCtx := testlogging.Context(t)
	m, st := newEpochCompactionTestManager(t, index.Version2)
	ids := []blob.ID{writeEpochCompactionTestBlob(t, baseCtx, m, index.Version2, epochCompactionTestEntries(t, 768))}
	ids = append(ids, ids[0])
	tempDir := t.TempDir()
	m.cacheDirectory = tempDir

	ctx, cancel := context.WithCancel(WithEpochCompactionOptions(baseCtx, EpochCompactionOptions{MemoryLimitBytes: testEpochCompactionMemory}))
	defer cancel()

	st.AddFault(blobtesting.MethodGetBlob).Before(func() {})
	st.AddFault(blobtesting.MethodGetBlob).Before(cancel).ErrorInstead(context.Canceled)
	require.ErrorIs(t, m.CompactEpoch(ctx, ids, "xs0_"), context.Canceled)
	outputs, err := blob.ListAllBlobs(baseCtx, m.st, "xs0_")
	require.NoError(t, err)
	require.Empty(t, outputs)
	requireEpochCompactionTempEmpty(t, tempDir)
	requireEpochCompactionOriginals(t, baseCtx, m, ids)
}
