package server_test

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/servertesting"
	"github.com/kopia/kopia/repo/blob"
)

func TestServer_RepoStatusWhileRefreshIsBlocked(t *testing.T) {
	var fst *blobtesting.FaultyStorage

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant, repotesting.Options{
		WrapStorage: func(st blob.Storage) blob.Storage {
			fst = blobtesting.NewFaultyStorage(st)
			return fst
		},
	})
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:  srvInfo.BaseURL,
		Username: servertesting.TestUIUsername,
		Password: servertesting.TestUIPassword,
	})
	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	listBlocked := make(chan struct{})
	release := make(chan struct{})

	var blockedOnce, releaseOnce sync.Once

	releaseStorage := func() { releaseOnce.Do(func() { close(release) }) }

	// registered after StartServer() so that storage is released before the server shuts down.
	t.Cleanup(releaseStorage)

	// simulate a storage connection that stops responding: all listings hang until released.
	fst.AddFault(blobtesting.MethodListBlobs).Repeat(math.MaxInt32).Before(func() {
		blockedOnce.Do(func() { close(listBlocked) })
		<-release
	})

	syncDone := make(chan error, 1)

	go func() {
		syncDone <- cli.Post(ctx, "repo/sync", &serverapi.Empty{}, &serverapi.Empty{})
	}()

	select {
	case <-listBlocked:
	case <-time.After(10 * time.Second):
		t.Fatal("refresh did not reach the storage")
	}

	statusCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	st, err := serverapi.RepoStatus(statusCtx, cli)
	require.NoError(t, err, "repository status must not wait for a blocked refresh")
	require.True(t, st.Connected)

	releaseStorage()

	select {
	case err := <-syncDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("refresh did not finish after the storage was released")
	}
}
