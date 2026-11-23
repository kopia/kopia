package indexblob

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/completeset"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/logging"
)

type compactionRetryStorage struct {
	blob.Storage
	reads     int
	beforeGet func(int) error
	afterPut  func()
}

func (s *compactionRetryStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	s.reads++
	if s.beforeGet != nil {
		if err := s.beforeGet(s.reads); err != nil {
			return err
		}
	}

	return s.Storage.GetBlob(ctx, id, offset, length, output)
}

func (s *compactionRetryStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	if err := s.Storage.PutBlob(ctx, id, data, opts); err != nil {
		return err
	}

	if s.afterPut != nil {
		s.afterPut()
	}

	return nil
}

type compactionRetryFormatting struct {
	IndexFormattingOptions
	beforeBuild func()
}

func (f compactionRetryFormatting) GetMutableParameters(ctx context.Context) (format.MutableParameters, error) {
	f.beforeBuild()
	return f.IndexFormattingOptions.GetMutableParameters(ctx)
}

func compactionTestRun(t *testing.T, cacheDirectory string) string {
	t.Helper()

	runs, err := filepath.Glob(filepath.Join(cacheDirectory, "index-compaction", "kopia-index-*", "run-*"))
	require.NoError(t, err)
	require.NotEmpty(t, runs, "fault injection must happen after spilling")

	return runs[0]
}

func TestCompactEpochRetriesMissingTemporaryFiles(t *testing.T) {
	for _, stage := range []string{"reading", "merging"} {
		for _, target := range []string{"run", "builder-directory", "scratch-directory", "cache-directory"} {
			t.Run(stage+"/"+target, func(t *testing.T) {
				ctx := testlogging.Context(t)
				m, st := newEpochCompactionTestManager(t, index.Version2)
				m.cacheDirectory = filepath.Join(t.TempDir(), "cache")
				m.compactionMemoryLimit = testEpochCompactionMemory
				id := writeEpochCompactionTestBlob(t, ctx, m, index.Version2, epochCompactionTestEntries(t, 768))
				ids := []blob.ID{id, id}
				want := epochCompactionExpected(t, ctx, m, ids, index.Version2)
				deleted := false
				remove := func() {
					if deleted {
						return
					}

					path := compactionTestRun(t, m.cacheDirectory)
					switch target {
					case "builder-directory":
						path = filepath.Dir(path)
					case "scratch-directory":
						path = filepath.Join(m.cacheDirectory, "index-compaction")
					case "cache-directory":
						path = m.cacheDirectory
					}

					require.NoError(t, os.RemoveAll(path))

					deleted = true
				}

				storage := &compactionRetryStorage{Storage: st}
				if stage == "reading" {
					storage.beforeGet = func(n int) error {
						if n == 2 {
							remove()
						}

						return nil
					}
				} else {
					m.formattingOptions = compactionRetryFormatting{m.formattingOptions, remove}
				}

				m.enc.st = storage
				require.NoError(t, m.CompactEpoch(ctx, ids, "xs0_"))
				require.True(t, deleted)
				require.Equal(t, 4, storage.reads, "only the affected compaction must replay its inputs once")

				outputs, err := blob.ListAllBlobs(ctx, st, "xs0_")
				require.NoError(t, err)
				require.Greater(t, len(outputs), 1, "retry must retain the small memory budget")
				sets := completeset.FindAll(outputs)
				require.Len(t, sets, 1)
				require.Len(t, sets[0], len(outputs))
				require.Equal(t, want, epochCompactionExpected(t, ctx, m, blob.IDsFromMetadata(sets[0]), index.Version2))
				requireEpochCompactionOriginals(t, ctx, m, ids)
				requireEpochCompactionTempEmpty(t, m.cacheDirectory)
			})
		}
	}
}

func TestCompactEpochTemporaryFileRetryLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(testlogging.Context(t), 30*time.Second)
	defer cancel()

	m, st := newEpochCompactionTestManager(t, index.Version2)
	m.cacheDirectory = t.TempDir()
	m.compactionMemoryLimit = testEpochCompactionMemory
	id := writeEpochCompactionTestBlob(t, ctx, m, index.Version2, epochCompactionTestEntries(t, 768))
	storage := &compactionRetryStorage{Storage: st}
	m.enc.st = storage
	formatting := m.formattingOptions
	deletions := 0
	m.formattingOptions = compactionRetryFormatting{formatting, func() {
		require.NoError(t, os.Remove(compactionTestRun(t, m.cacheDirectory)))

		deletions++
	}}

	require.ErrorIs(t, m.CompactEpoch(ctx, []blob.ID{id}, "xs0_"), index.ErrSpillingFileMissing)
	require.Equal(t, 10, deletions, "the shared retry policy allows ten attempts including the initial attempt")
	require.Equal(t, 10, storage.reads)

	outputs, err := blob.ListAllBlobs(ctx, st, "xs0_")
	require.NoError(t, err)
	require.Empty(t, outputs)
	requireEpochCompactionOriginals(t, ctx, m, []blob.ID{id})
	requireEpochCompactionTempEmpty(t, m.cacheDirectory)

	// Exhausting the automatic retry must not prevent a later maintenance run.
	m.formattingOptions = formatting
	require.NoError(t, m.CompactEpoch(ctx, []blob.ID{id}, "xs0_"))
	outputs, err = blob.ListAllBlobs(ctx, st, "xs0_")
	require.NoError(t, err)

	sets := completeset.FindAll(outputs)
	require.Len(t, sets, 1)
	require.Equal(t, epochCompactionExpected(t, ctx, m, []blob.ID{id}, index.Version2),
		epochCompactionExpected(t, ctx, m, blob.IDsFromMetadata(sets[0]), index.Version2))
	requireEpochCompactionTempEmpty(t, m.cacheDirectory)
}

func TestCompactEpochRecoversAfterRepeatedTemporaryFileLoss(t *testing.T) {
	var logs bytes.Buffer

	ctx := logging.WithAdditionalLogger(testlogging.Context(t), logging.ToWriter(&logs))
	m, st := newEpochCompactionTestManager(t, index.Version2)
	m.cacheDirectory = t.TempDir()
	m.compactionMemoryLimit = testEpochCompactionMemory
	id := writeEpochCompactionTestBlob(t, ctx, m, index.Version2, epochCompactionTestEntries(t, 768))
	want := epochCompactionExpected(t, ctx, m, []blob.ID{id}, index.Version2)
	storage := &compactionRetryStorage{Storage: st}
	m.enc.st = storage
	deletions := 0
	m.formattingOptions = compactionRetryFormatting{m.formattingOptions, func() {
		if deletions < 3 {
			require.NoError(t, os.Remove(compactionTestRun(t, m.cacheDirectory)))

			deletions++
		}
	}}

	require.NoError(t, m.CompactEpoch(ctx, []blob.ID{id}, "xs0_"))
	require.Equal(t, 3, deletions)
	require.Equal(t, 4, storage.reads)
	require.Contains(t, logs.String(), "retry 1) after temporary files disappeared")
	require.Contains(t, logs.String(), "retry 3) after temporary files disappeared")
	require.NotContains(t, logs.String(), "retry 4)")

	outputs, err := blob.ListAllBlobs(ctx, st, "xs0_")
	require.NoError(t, err)
	require.Greater(t, len(outputs), 1, "retries must preserve the memory budget")
	sets := completeset.FindAll(outputs)
	require.Len(t, sets, 1)
	require.Len(t, sets[0], len(outputs))
	require.Equal(t, want, epochCompactionExpected(t, ctx, m, blob.IDsFromMetadata(sets[0]), index.Version2))
	requireEpochCompactionOriginals(t, ctx, m, []blob.ID{id})
	requireEpochCompactionTempEmpty(t, m.cacheDirectory)
}

func TestCompactEpochCanceledBeforeBackoffRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(testlogging.Context(t))
	defer cancel()
	// Cancel when the shared helper starts its backoff, after a retryable
	// failure and cleanup. No timer race or extra production test hook is needed.
	backoffReached := false
	ctx = logging.WithAdditionalLogger(ctx, func(module string) logging.Logger {
		return testlogging.Printf(func(string, ...any) {
			if module == "retry" {
				backoffReached = true

				cancel()
			}
		}, "")
	})
	m, st := newEpochCompactionTestManager(t, index.Version2)
	m.cacheDirectory = t.TempDir()
	m.compactionMemoryLimit = testEpochCompactionMemory
	id := writeEpochCompactionTestBlob(t, ctx, m, index.Version2, epochCompactionTestEntries(t, 768))
	storage := &compactionRetryStorage{Storage: st}
	m.enc.st = storage
	m.formattingOptions = compactionRetryFormatting{m.formattingOptions, func() {
		require.NoError(t, os.Remove(compactionTestRun(t, m.cacheDirectory)))
	}}

	require.ErrorIs(t, m.CompactEpoch(ctx, []blob.ID{id}, "xs0_"), context.Canceled)
	require.True(t, backoffReached)
	require.Equal(t, 1, storage.reads, "cancellation during backoff must prevent another attempt")

	outputs, err := blob.ListAllBlobs(context.Background(), st, "xs0_")
	require.NoError(t, err)
	require.Empty(t, outputs)
	requireEpochCompactionOriginals(t, context.Background(), m, []blob.ID{id})
	requireEpochCompactionTempEmpty(t, m.cacheDirectory)
}

func TestCompactEpochDoesNotRetryOtherFailures(t *testing.T) {
	for _, scenario := range []string{"missing-input", "permission", "corrupt-run", "canceled-after-deletion"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(testlogging.Context(t))
			defer cancel()

			m, st := newEpochCompactionTestManager(t, index.Version2)
			m.cacheDirectory = t.TempDir()
			m.compactionMemoryLimit = testEpochCompactionMemory
			id := writeEpochCompactionTestBlob(t, ctx, m, index.Version2, epochCompactionTestEntries(t, 768))
			storage := &compactionRetryStorage{Storage: st}
			m.enc.st = storage

			var wantErr error
			switch scenario {
			case "missing-input", "permission":
				wantErr = os.ErrNotExist
				if scenario == "permission" {
					wantErr = os.ErrPermission
				}

				storage.beforeGet = func(int) error { return wantErr }
			case "corrupt-run":
				m.formattingOptions = compactionRetryFormatting{m.formattingOptions, func() {
					require.NoError(t, os.Truncate(compactionTestRun(t, m.cacheDirectory), 0))
				}}
			case "canceled-after-deletion":
				wantErr = context.Canceled
				m.formattingOptions = compactionRetryFormatting{m.formattingOptions, func() {
					require.NoError(t, os.Remove(compactionTestRun(t, m.cacheDirectory)))
					cancel()
				}}
			}

			err := m.CompactEpoch(ctx, []blob.ID{id}, "xs0_")
			require.Error(t, err)

			if wantErr != nil {
				require.ErrorIs(t, err, wantErr)
			}

			require.Equal(t, 1, storage.reads, "non-disappearance failures must not replay input")

			outputs, err := blob.ListAllBlobs(context.Background(), st, "xs0_")
			require.NoError(t, err)
			require.Empty(t, outputs)
			requireEpochCompactionOriginals(t, context.Background(), m, []blob.ID{id})
			requireEpochCompactionTempEmpty(t, m.cacheDirectory)
		})
	}
}

func TestCompactEpochDeletingOpenOutput(t *testing.T) {
	ctx := testlogging.Context(t)
	m, st := newEpochCompactionTestManager(t, index.Version2)
	m.cacheDirectory = t.TempDir()
	m.compactionMemoryLimit = testEpochCompactionMemory
	id := writeEpochCompactionTestBlob(t, ctx, m, index.Version2, epochCompactionTestEntries(t, 768))
	storage := &compactionRetryStorage{Storage: st}
	m.enc.st, m.st = storage, storage
	deletionAttempted := false
	storage.afterPut = func() {
		if deletionAttempted {
			return
		}

		paths, err := filepath.Glob(filepath.Join(m.cacheDirectory, "index-compaction", "kopia-compaction-shards-*"))
		require.NoError(t, err)
		require.Len(t, paths, 1)

		err = os.Remove(paths[0])
		if runtime.GOOS == "windows" {
			// Windows can deny deleting an open file. Do not close the live
			// handle to simulate deletion: that would change the operation.
			if err != nil {
				t.Logf("Windows refused deletion of the open output: %v", err)
			}
		} else {
			require.NoError(t, err)
		}

		deletionAttempted = true
	}

	require.NoError(t, m.CompactEpoch(ctx, []blob.ID{id}, "xs0_"))
	require.True(t, deletionAttempted)
	require.Equal(t, 1, storage.reads, "an open, readable output needs no replay")

	outputs, err := blob.ListAllBlobs(ctx, st, "xs0_")
	require.NoError(t, err)

	sets := completeset.FindAll(outputs)
	require.Len(t, sets, 1)
	require.Equal(t, epochCompactionExpected(t, ctx, m, []blob.ID{id}, index.Version2),
		epochCompactionExpected(t, ctx, m, blob.IDsFromMetadata(sets[0]), index.Version2))
	requireEpochCompactionTempEmpty(t, m.cacheDirectory)
	// All handles must be released after compaction, including on Windows.
	require.NoError(t, os.RemoveAll(filepath.Join(m.cacheDirectory, "index-compaction")))
}
