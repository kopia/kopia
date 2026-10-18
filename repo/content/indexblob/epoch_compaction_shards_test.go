package indexblob

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/gather"
)

func TestCompactionShardStore(t *testing.T) {
	for _, count := range []int{0, 1, 10} {
		t.Run(fmt.Sprintf("shards-%d", count), func(t *testing.T) {
			dir := t.TempDir()
			s := &compactionShardStore{tempDirectory: dir}
			t.Cleanup(func() { require.NoError(t, s.close()) })

			var want [][]byte
			for i := range count {
				data := []byte{byte(i), 2, 3}
				want = append(want, append([]byte(nil), data...))
				require.NoError(t, s.add(context.Background(), gather.FromSlice(data)))
				data[0] = 255 // the emitter may reuse its buffer immediately
			}

			files, err := os.ReadDir(dir)
			require.NoError(t, err)

			if count <= 1 {
				require.Empty(t, files)
			} else {
				require.Len(t, files, 1)
			}

			var got [][]byte
			require.NoError(t, s.replay(context.Background(), func(data gather.Bytes) error {
				got = append(got, append([]byte(nil), data.ToByteSlice()...))
				return nil
			}))
			require.Equal(t, want, got)
			require.NoError(t, s.close())

			files, err = os.ReadDir(dir)
			require.NoError(t, err)
			require.Empty(t, files)
		})
	}
}

func TestCompactionShardStoreTrailingDataPreventsFinalEmission(t *testing.T) {
	s := &compactionShardStore{tempDirectory: t.TempDir()}
	t.Cleanup(func() { require.NoError(t, s.close()) })

	ctx := context.Background()
	require.NoError(t, s.add(ctx, gather.FromSlice([]byte("first"))))
	require.NoError(t, s.add(ctx, gather.FromSlice([]byte("second"))))
	_, err := s.file.WriteString("unexpected")
	require.NoError(t, err)

	calls := 0
	require.ErrorContains(t, s.replay(ctx, func(gather.Bytes) error {
		calls++
		return nil
	}), "trailing")
	require.Equal(t, 1, calls, "the final shard must not complete the published set")
}

func TestCompactionShardStoreMissingDirectory(t *testing.T) {
	dir := t.TempDir()
	s := &compactionShardStore{tempDirectory: dir}
	t.Cleanup(func() { require.NoError(t, s.close()) })

	ctx := context.Background()
	require.NoError(t, s.add(ctx, gather.FromSlice([]byte("first"))))
	require.NoError(t, os.RemoveAll(dir))
	// The first shard was held in memory; clearing before the second shard
	// creates its spool must be distinguishable from repository I/O errors.
	require.ErrorIs(t, s.add(ctx, gather.FromSlice([]byte("second"))), errCompactionTemporaryDirectoryMissing)
}

func TestCompactionShardStoreFailures(t *testing.T) {
	for _, scenario := range []string{"checksum", "length", "truncated", "cancel", "callback"} {
		t.Run(scenario, func(t *testing.T) {
			s := &compactionShardStore{tempDirectory: t.TempDir()}
			t.Cleanup(func() { require.NoError(t, s.close()) })

			ctx := context.Background()
			require.NoError(t, s.add(ctx, gather.FromSlice([]byte("first"))))
			require.NoError(t, s.add(ctx, gather.FromSlice([]byte("second"))))

			switch scenario {
			case "checksum":
				_, err := s.file.WriteAt([]byte("X"), 12)
				require.NoError(t, err)
			case "length":
				var length [8]byte
				binary.LittleEndian.PutUint64(length[:], ^uint64(0))
				_, err := s.file.WriteAt(length[:], 0)
				require.NoError(t, err)
			case "truncated":
				require.NoError(t, s.file.Truncate(13))
			case "cancel":
				var cancel context.CancelFunc

				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			calls := 0
			err := s.replay(ctx, func(gather.Bytes) error {
				calls++
				return errors.New("upload failed")
			})
			require.Error(t, err)

			if scenario == "callback" {
				require.Equal(t, 1, calls)
			} else {
				require.Zero(t, calls)
			}
		})
	}
}
