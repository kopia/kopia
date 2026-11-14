package server_test

import (
	"context"
	"crypto/rand"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/servertesting"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

// time given to the client to notice that the server dropped its connection.
const connectionDropNoticeTime = 500 * time.Millisecond

func openReconnectTestRepo(t *testing.T, opts ...repotesting.Options) (context.Context, repo.Repository, *httptest.Server) {
	t.Helper()

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant, opts...)
	asi, hs := servertesting.StartServerWithHTTPServer(ctx, t, env, true)

	rep, err := servertesting.ConnectAndOpenAPIServer(t, ctx, asi, repo.ClientOptions{
		Username: servertesting.TestUsername,
		Hostname: servertesting.TestHostname,
	}, content.CachingOptions{
		CacheDirectory:        testutil.TempDirectory(t),
		ContentCacheSizeBytes: maxCacheSizeBytes,
	}, servertesting.TestPassword, &repo.Options{})
	require.NoError(t, err)

	t.Cleanup(func() { rep.Close(ctx) })

	return ctx, rep, hs
}

func randomBytes(t *testing.T) []byte {
	t.Helper()

	b := make([]byte, 100_000)
	_, err := rand.Read(b)
	require.NoError(t, err)

	return b
}

func waitForContentAcknowledged(ctx context.Context, t *testing.T, w repo.RepositoryWriter, oid object.ID) {
	t.Helper()

	cid, _, ok := oid.ContentID()
	require.True(t, ok)
	require.Eventually(t, func() bool {
		_, err := w.ContentInfo(ctx, cid)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
}

func dropClientConnections(hs *httptest.Server) {
	// simulates the server's TCP stack giving up on a client that stopped responding,
	// for example a laptop that went to sleep in the middle of a snapshot.
	hs.CloseClientConnections()
	time.Sleep(connectionDropNoticeTime)
}

// TestWriteSessionSurvivesDroppedConnection verifies that a write session can save a manifest
// after the server dropped its connection, provided that everything it wrote before is durable.
func TestWriteSessionSurvivesDroppedConnection(t *testing.T) {
	cases := []struct {
		name                string
		flushBeforeDrop     bool
		writeAfterReconnect bool
	}{
		{name: "flushed before drop", flushBeforeDrop: true},
		{name: "unflushed before drop"},
		{name: "unflushed before drop, write after", writeAfterReconnect: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, rep, hs := openReconnectTestRepo(t)

			data1 := randomBytes(t)
			data2 := randomBytes(t)

			var (
				oid1, oid2 object.ID
				manID      manifest.ID
			)

			require.NoError(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
				Purpose: "reconnect test",
			}, func(ctx context.Context, w repo.RepositoryWriter) error {
				oid1 = mustWriteObject(ctx, t, w, data1)

				if tc.flushBeforeDrop {
					require.NoError(t, w.Flush(ctx))
				} else {
					waitForContentAcknowledged(ctx, t, w, oid1)
				}

				dropClientConnections(hs)

				if tc.writeAfterReconnect {
					oid2 = mustWriteObject(ctx, t, w, data2)
				}

				var err error

				manID, err = snapshot.SaveSnapshot(ctx, w, &snapshot.Manifest{
					Source:      snapshot.SourceInfo{Host: servertesting.TestHostname, UserName: servertesting.TestUsername, Path: testPathname},
					Description: "after reconnect",
					RootEntry:   &snapshot.DirEntry{ObjectID: oid1},
				})

				return err
			}))

			mustReadObject(ctx, t, rep, oid1, data1)
			mustReadManifest(ctx, t, rep, manID, "after reconnect")

			if tc.writeAfterReconnect {
				mustReadObject(ctx, t, rep, oid2, data2)
			}
		})
	}
}

func openFaultyReconnectTestRepo(t *testing.T) (context.Context, repo.Repository, *httptest.Server, *blobtesting.FaultyStorage) {
	t.Helper()

	var fs *blobtesting.FaultyStorage

	ctx, rep, hs := openReconnectTestRepo(t, repotesting.Options{
		WrapStorage: func(st blob.Storage) blob.Storage {
			fs = blobtesting.NewFaultyStorage(st)
			return fs
		},
	})

	return ctx, rep, hs, fs
}

// TestWriteSessionFailsWhenUnflushedWritesAreLost verifies that a write session does not continue
// when contents written before the connection dropped did not reach the repository,
// for example because the server could not flush the broken session.
func TestWriteSessionFailsWhenUnflushedWritesAreLost(t *testing.T) {
	ctx, rep, hs, fs := openFaultyReconnectTestRepo(t)

	data := randomBytes(t)

	err := repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
		Purpose: "reconnect test",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		oid := mustWriteObject(ctx, t, w, data)
		waitForContentAcknowledged(ctx, t, w, oid)

		// the server fails to write the pack when it flushes the broken session.
		fs.AddFault(blobtesting.MethodPutBlob).ErrorInstead(errors.New("injected storage failure"))

		dropClientConnections(hs)

		// the client keeps looking for the lost contents for about a minute, do not wait for that.
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		_, err := snapshot.SaveSnapshot(ctx, w, &snapshot.Manifest{
			Source:      snapshot.SourceInfo{Host: servertesting.TestHostname, UserName: servertesting.TestUsername, Path: testPathname},
			Description: "must not be saved",
			RootEntry:   &snapshot.DirEntry{ObjectID: oid},
		})

		return err
	})

	require.ErrorContains(t, err, "unable to resume write session")
	mustListSnapshotCount(ctx, t, rep, 0)
}

// TestWriteSessionFailsWhenUnflushedManifestsMayBeLost verifies that a write session does not continue
// after its connection dropped while a manifest it wrote was not flushed, because manifests written
// on the broken session cannot be verified.
func TestWriteSessionFailsWhenUnflushedManifestsMayBeLost(t *testing.T) {
	ctx, rep, hs, fs := openFaultyReconnectTestRepo(t)

	data := randomBytes(t)

	err := repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
		Purpose: "reconnect test",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		oid := mustWriteObject(ctx, t, w, data)
		require.NoError(t, w.Flush(ctx))

		_, err := snapshot.SaveSnapshot(ctx, w, &snapshot.Manifest{
			Source:      snapshot.SourceInfo{Host: servertesting.TestHostname, UserName: servertesting.TestUsername, Path: testPathname},
			Description: "saved before the drop, lost with the broken session",
			RootEntry:   &snapshot.DirEntry{ObjectID: oid},
		})
		require.NoError(t, err)

		// the server fails to write the manifest when it flushes the broken session.
		fs.AddFault(blobtesting.MethodPutBlob).ErrorInstead(errors.New("injected storage failure"))

		dropClientConnections(hs)

		return w.Flush(ctx)
	})

	require.ErrorContains(t, err, "manifests were changed since the last flush")
	mustListSnapshotCount(ctx, t, rep, 0)
}
