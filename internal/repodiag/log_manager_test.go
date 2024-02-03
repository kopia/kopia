package repodiag_test

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/repodiag"
	"github.com/kopia/kopia/internal/testlogging"
)

func TestLogManager_Enabled(t *testing.T) {
	d := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(d, nil, nil)
	w := repodiag.NewWriter(st, newStaticCrypter(t))
	ctx := testlogging.Context(t)
	lm := repodiag.NewLogManager(ctx, w)

	lm.Enable()
	l := lm.NewLogger()
	l.Infof("hello")

	require.Len(t, d, 0)
	l.Sync()
	w.Wait(ctx)

	// make sure log messages are written
	require.Len(t, d, 1)

	// make sure blob ID is prefixed
	for k := range d {
		require.True(t, strings.HasPrefix(string(k), repodiag.LogBlobPrefix))
	}
}

func TestLogManager_AutoFlush(t *testing.T) {
	d := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(d, nil, nil)
	w := repodiag.NewWriter(st, newStaticCrypter(t))
	ctx := testlogging.Context(t)
	lm := repodiag.NewLogManager(ctx, w)

	lm.Enable()
	l := lm.NewLogger()

	// flush happens after 4 << 20 bytes (4MB) after compression,
	// write ~10MB of base16 data which compresses to ~5MB and writes 1 blob
	for i := 0; i < 5000; i++ {
		var b [1024]byte

		rand.Read(b[:])
		l.Infof(hex.EncodeToString(b[:]))
	}

	w.Wait(ctx)

	require.Equal(t, 1, len(d))

	l.Sync()
	w.Wait(ctx)

	require.Equal(t, 2, len(d))
}

func TestLogManager_NotEnabled(t *testing.T) {
	d := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(d, nil, nil)
	w := repodiag.NewWriter(st, newStaticCrypter(t))
	ctx := testlogging.Context(t)
	lm := repodiag.NewLogManager(ctx, w)

	l := lm.NewLogger()
	l.Infof("hello")

	require.Len(t, d, 0)
	l.Sync()
	w.Wait(ctx)

	// make sure log messages are not written
	require.Len(t, d, 0)
}

func TestLogManager_Null(t *testing.T) {
	var lm *repodiag.LogManager

	lm.Enable()

	l := lm.NewLogger()
	l.Infof("hello")
	l.Sync()
}
