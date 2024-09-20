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
	l.Info("hello")

	require.Empty(t, d)
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
	for range 5000 {
		var b [1024]byte

		rand.Read(b[:])
		l.Info(hex.EncodeToString(b[:]))
	}

	w.Wait(ctx)

	require.Len(t, d, 1)

	l.Sync()
	w.Wait(ctx)

	require.Len(t, d, 2)
}

func TestLogManager_NotEnabled(t *testing.T) {
	d := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(d, nil, nil)
	w := repodiag.NewWriter(st, newStaticCrypter(t))
	ctx := testlogging.Context(t)
	lm := repodiag.NewLogManager(ctx, w)

	l := lm.NewLogger()
	l.Info("hello")

	require.Empty(t, d)
	l.Sync()
	w.Wait(ctx)

	// make sure log messages are not written
	require.Empty(t, d)
}

func TestLogManager_Null(t *testing.T) {
	var lm *repodiag.LogManager

	lm.Enable()

	l := lm.NewLogger()
	l.Info("hello")
	l.Sync()
}
