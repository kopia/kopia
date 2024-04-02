package server_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/servertesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

func TestListAndDeleteSnapshots(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	si1 := env.LocalPathSourceInfo("/dummy/path")
	si2 := env.LocalPathSourceInfo("/another/path")

	var id11, id12, id13, id14, id21 manifest.ID

	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{Purpose: "Test"}, func(ctx context.Context, w repo.RepositoryWriter) error {
		u := snapshotfs.NewUploader(w)

		dir1 := mockfs.NewDirectory()

		dir1.AddFile("file1", []byte{1, 2, 3}, 0o644)
		dir1.AddFile("file2", []byte{1, 2, 4}, 0o644)

		man11, err := u.Upload(ctx, dir1, nil, si1)
		require.NoError(t, err)
		id11, err = snapshot.SaveSnapshot(ctx, w, man11)
		require.NoError(t, err)

		man12, err := u.Upload(ctx, dir1, nil, si1)
		require.NoError(t, err)
		id12, err = snapshot.SaveSnapshot(ctx, w, man12)
		require.NoError(t, err)

		dir1.AddFile("file3", []byte{1, 2, 5}, 0o644)

		man13, err := u.Upload(ctx, dir1, nil, si1)
		require.NoError(t, err)
		id13, err = snapshot.SaveSnapshot(ctx, w, man13)
		require.NoError(t, err)

		dir1.AddFile("file4", []byte{1, 2, 6}, 0o644)

		man14, err := u.Upload(ctx, dir1, nil, si1)
		require.NoError(t, err)
		id14, err = snapshot.SaveSnapshot(ctx, w, man14)
		require.NoError(t, err)

		dir2 := mockfs.NewDirectory()

		man21, err := u.Upload(ctx, dir2, nil, si2)
		require.NoError(t, err)
		id21, err = snapshot.SaveSnapshot(ctx, w, man21)
		require.NoError(t, err)

		t.Logf("man11: %v id11: %v", *man11, id11)
		t.Logf("man12: %v id12: %v", *man12, id12)
		t.Logf("man13: %v id13: %v", *man13, id13)
		t.Logf("man14: %v id14: %v", *man14, id14)
		t.Logf("man21: %v id21: %v", *man21, id21)

		return nil
	}))

	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	sourceList, err := serverapi.ListSources(ctx, cli, nil)
	require.NoError(t, err)

	// we have 2 sources at this point
	require.Len(t, sourceList.Sources, 2)

	// list all snapshots
	resp, err := serverapi.ListSnapshots(ctx, cli, si1, true)
	require.NoError(t, err)

	require.Len(t, resp.Snapshots, 4)
	require.Equal(t, 3, resp.UniqueCount)
	require.Equal(t, 4, resp.UnfilteredCount)

	// list unique snapshots - first and second were the same, so we get 3
	resp, err = serverapi.ListSnapshots(ctx, cli, si1, false)
	require.NoError(t, err)

	require.Len(t, resp.Snapshots, 3)
	require.Equal(t, 3, resp.UniqueCount)
	require.Equal(t, 4, resp.UnfilteredCount)

	// now delete id11 and id14 via the API
	require.NoError(t, cli.Post(ctx, "snapshots/delete", &serverapi.DeleteSnapshotsRequest{
		SourceInfo: si1,
		SnapshotManifestIDs: []manifest.ID{
			id11,
			id14,
		},
	}, &serverapi.Empty{}))

	badReq := apiclient.HTTPStatusError{HTTPStatusCode: 400, ErrorMessage: "400 Bad Request: unknown source"}
	serverError := apiclient.HTTPStatusError{HTTPStatusCode: 500, ErrorMessage: "500 Internal Server Error: internal server error: source info does not match snapshot source"}

	// make sure when deleting snapshot by ID the source must match
	require.ErrorIs(t, cli.Post(ctx, "snapshots/delete", &serverapi.DeleteSnapshotsRequest{
		SourceInfo:          si2,
		SnapshotManifestIDs: []manifest.ID{id12},
	}, &serverapi.Empty{}), serverError)

	resp, err = serverapi.ListSnapshots(ctx, cli, si1, true)
	require.NoError(t, err)

	require.Len(t, resp.Snapshots, 2)
	require.Equal(t, 2, resp.UniqueCount)
	require.Equal(t, 2, resp.UnfilteredCount)

	// now delete the entire source
	require.NoError(t, cli.Post(ctx, "snapshots/delete", &serverapi.DeleteSnapshotsRequest{
		SourceInfo:            si1,
		DeleteSourceAndPolicy: true,
	}, &serverapi.Empty{}))

	resp, err = serverapi.ListSnapshots(ctx, cli, si1, true)
	require.NoError(t, err)
	require.Empty(t, resp.Snapshots, 0)

	_ = id12
	_ = id13

	require.ErrorIs(t, cli.Post(ctx, "snapshots/delete", &serverapi.DeleteSnapshotsRequest{
		SourceInfo: si1,
	}, &serverapi.Empty{}), badReq)

	sourceList, err = serverapi.ListSources(ctx, cli, nil)
	require.NoError(t, err)

	require.Len(t, sourceList.Sources, 1)

	// now delete the remaining snapshot from si1, without deleting the entire source
	require.NoError(t, cli.Post(ctx, "snapshots/delete", &serverapi.DeleteSnapshotsRequest{
		SourceInfo:          si2,
		SnapshotManifestIDs: []manifest.ID{id21},
	}, &serverapi.Empty{}))

	// we still have 1 source.
	sourceList, err = serverapi.ListSources(ctx, cli, nil)
	require.NoError(t, err)

	require.Len(t, sourceList.Sources, 1)

	// now delete the entire si2 source
	require.NoError(t, cli.Post(ctx, "snapshots/delete", &serverapi.DeleteSnapshotsRequest{
		SourceInfo:            si2,
		DeleteSourceAndPolicy: true,
	}, &serverapi.Empty{}))

	// all sources are gone
	sourceList, err = serverapi.ListSources(ctx, cli, nil)
	require.NoError(t, err)

	require.Empty(t, sourceList.Sources)
}

func TestEditSnapshots(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	si1 := env.LocalPathSourceInfo("/dummy/path")

	var id11 manifest.ID

	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{Purpose: "Test"}, func(ctx context.Context, w repo.RepositoryWriter) error {
		u := snapshotfs.NewUploader(w)

		dir1 := mockfs.NewDirectory()

		dir1.AddFile("file1", []byte{1, 2, 3}, 0o644)
		dir1.AddFile("file2", []byte{1, 2, 4}, 0o644)

		man11, err := u.Upload(ctx, dir1, nil, si1)
		require.NoError(t, err)
		id11, err = snapshot.SaveSnapshot(ctx, w, man11)
		require.NoError(t, err)

		return nil
	}))

	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	resp, err := serverapi.ListSnapshots(ctx, cli, si1, true)
	require.NoError(t, err)

	require.Len(t, resp.Snapshots, 1)

	var (
		updated []*serverapi.Snapshot

		newDesc1 = "desc1"
		newDesc2 = "desc2"
	)

	require.NoError(t, cli.Post(ctx, "snapshots/edit", &serverapi.EditSnapshotsRequest{
		Snapshots:      []manifest.ID{id11},
		AddPins:        []string{"pin1", "pin2"},
		NewDescription: &newDesc1,
	}, &updated))

	require.Len(t, updated, 1)
	require.EqualValues(t, []string{"pin1", "pin2"}, updated[0].Pins)
	require.EqualValues(t, newDesc1, updated[0].Description)

	require.NoError(t, cli.Post(ctx, "snapshots/edit", &serverapi.EditSnapshotsRequest{
		Snapshots:      []manifest.ID{updated[0].ID},
		AddPins:        []string{"pin3"},
		RemovePins:     []string{"pin1"},
		NewDescription: &newDesc2,
	}, &updated))

	require.Len(t, updated, 1)
	require.EqualValues(t, []string{"pin2", "pin3"}, updated[0].Pins)
	require.EqualValues(t, newDesc2, updated[0].Description)

	require.NoError(t, cli.Post(ctx, "snapshots/edit", &serverapi.EditSnapshotsRequest{
		Snapshots:  []manifest.ID{updated[0].ID},
		RemovePins: []string{"pin3"},
	}, &updated))

	require.Len(t, updated, 1)
	require.EqualValues(t, []string{"pin2"}, updated[0].Pins)
	require.EqualValues(t, newDesc2, updated[0].Description)
}
