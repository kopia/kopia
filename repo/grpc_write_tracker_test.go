package repo

import (
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/content"
)

func mustContentID(t *testing.T, s string) content.ID {
	t.Helper()

	cid, err := content.ParseID(s)
	require.NoError(t, err)

	return cid
}

func TestWriteTracker_FlushForgetsOnlyWritesAcknowledgedBeforeItStarted(t *testing.T) {
	var wt writeTracker

	before := mustContentID(t, "aa11")
	during := mustContentID(t, "bb22")
	unacked := mustContentID(t, "cc33")

	k1 := wt.beginContentWrite(before)
	wt.contentWriteAcked(before, k1)

	// sent before the flush starts but acknowledged after it: the flush may not include it.
	k2 := wt.beginContentWrite(during)
	wt.beginContentWrite(unacked)

	epoch := wt.startFlush()

	wt.contentWriteAcked(during, k2)
	wt.flushSucceeded(epoch)

	ids, err := wt.contentsToVerify()
	require.NoError(t, err)
	require.ElementsMatch(t, []content.ID{during, unacked}, ids)

	// the next flush covers the write acknowledged during the previous one, but not the unacknowledged one.
	wt.flushSucceeded(wt.startFlush())

	ids, err = wt.contentsToVerify()
	require.NoError(t, err)
	require.ElementsMatch(t, []content.ID{unacked}, ids)
}

func TestWriteTracker_NotSentWritesAreForgotten(t *testing.T) {
	var wt writeTracker

	cid := mustContentID(t, "aa11")

	k := wt.beginContentWrite(cid)
	require.Equal(t, contentWriteTracked, k)

	// a second write of the same content while the first is tracked.
	k2 := wt.beginContentWrite(cid)
	require.Equal(t, contentWriteAlreadyTracked, k2)
	wt.contentWriteNotSent(cid, k2)

	ids, err := wt.contentsToVerify()
	require.NoError(t, err)
	require.ElementsMatch(t, []content.ID{cid}, ids)

	wt.contentWriteNotSent(cid, k)

	ids, err = wt.contentsToVerify()
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestWriteTracker_ManifestMutationsPreventResumeUntilFlushed(t *testing.T) {
	var wt writeTracker

	// a mutation that was never sent does not count.
	wt.beginManifestMutation()
	wt.manifestMutationDone(errRequestNotSent)

	_, err := wt.contentsToVerify()
	require.NoError(t, err)

	// an acknowledged mutation prevents resuming until a flush covers it.
	wt.beginManifestMutation()
	wt.manifestMutationDone(nil)

	_, err = wt.contentsToVerify()
	require.ErrorIs(t, err, errCannotResume)

	wt.flushSucceeded(wt.startFlush())

	_, err = wt.contentsToVerify()
	require.NoError(t, err)

	// a mutation with unknown outcome prevents resuming for good.
	wt.beginManifestMutation()
	wt.manifestMutationDone(errors.Wrap(io.EOF, "stream broken"))
	wt.flushSucceeded(wt.startFlush())

	_, err = wt.contentsToVerify()
	require.ErrorIs(t, err, errCannotResume)
}
