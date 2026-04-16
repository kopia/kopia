package repodiag_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/repodiag"
	"github.com/kopia/kopia/internal/testlogging"
)

func TestLogManager_Enabled(t *testing.T) {
	d := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(d, nil, nil)
	w := repodiag.NewWriter(st, newStaticCrypter(t))
	ctx := testlogging.Context(t)
	lm := repodiag.NewLogManager(ctx, w, false, io.Discard)

	lm.Enable()
	l := lm.NewLogger("test")
	contentlog.Log(ctx, l, "hello")

	require.Empty(t, d)
	lm.Sync()
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
	lm := repodiag.NewLogManager(ctx, w, false, io.Discard)

	lm.Enable()
	l := lm.NewLogger("test")

	// flush happens after 4 << 20 bytes (4MB) after compression,
	// write ~10MB of base16 data which compresses to ~5MB and writes 1 blob
	for range 5000 {
		var b [1024]byte

		rand.Read(b[:])
		contentlog.Log(ctx, l, hex.EncodeToString(b[:]))
	}

	w.Wait(ctx)

	require.Len(t, d, 1)

	lm.Sync()
	w.Wait(ctx)

	require.Len(t, d, 2)
}

func TestLogManager_NotEnabled(t *testing.T) {
	d := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(d, nil, nil)
	w := repodiag.NewWriter(st, newStaticCrypter(t))
	ctx := testlogging.Context(t)
	lm := repodiag.NewLogManager(ctx, w, false, io.Discard)

	l := lm.NewLogger("test")
	contentlog.Log(ctx, l, "hello")

	require.Empty(t, d)
	lm.Sync()
	w.Wait(ctx)

	// make sure log messages are not written
	require.Empty(t, d)
}

func TestLogManager_CancelledContext(t *testing.T) {
	d := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(d, nil, nil)
	w := repodiag.NewWriter(st, newStaticCrypter(t))
	ctx := testlogging.Context(t)
	cctx, cancel := context.WithCancel(ctx)
	lm := repodiag.NewLogManager(cctx, w, false, io.Discard)

	// cancel context, logs should still be written
	cancel()

	lm.Enable()
	l := lm.NewLogger("test")
	contentlog.Log(ctx, l, "hello")

	require.Empty(t, d)

	lm.Sync()
	w.Wait(ctx)

	// make sure log messages are written
	require.Len(t, d, 1)
}

func TestLogManager_Null(t *testing.T) {
	var lm *repodiag.LogManager

	ctx := testlogging.Context(t)

	lm.Enable()

	l := lm.NewLogger("test")
	contentlog.Log(ctx, l, "hello")
	lm.Sync()
}
