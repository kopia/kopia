//go:build linux || freebsd || darwin
// +build linux freebsd darwin

package filesystem

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sharded"
)

func TestFileStorage_ESTALE_ErrorHandling(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	dataDir := testutil.TempDirectory(t)

	osi := newMockOS()

	st, err := New(ctx, &Options{
		Path: dataDir,
		Options: sharded.Options{
			DirectoryShards: []int{5, 2},
		},
	}, true)
	require.NoError(t, err)

	st.(*fsStorage).Impl.(*fsImpl).osi = osi

	require.False(t, st.(*fsStorage).Impl.(*fsImpl).isRetriable(syscall.ESTALE), "ESTALE should not be retryable")

	defer st.Close(ctx)

	require.NoError(t, st.PutBlob(ctx, "someblob1234567812345678", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))

	osi.eStaleRemainingErrors.Store(1)

	_, err = st.GetMetadata(ctx, "someblob1234567812345678")
	require.ErrorIs(t, err, syscall.ESTALE)
}
