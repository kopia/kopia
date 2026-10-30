package server_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/servertesting"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func TestSnapshotCounters(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	dir := testutil.TempDirectory(t)
	si := env.LocalPathSourceInfo(dir)

	mustCreateSource(t, cli, dir, &policy.Policy{})
	require.Len(t, mustListSources(t, cli, &snapshot.SourceInfo{}), 1)

	mustSetPolicy(t, cli, si, &policy.Policy{
		FilesPolicy: policy.FilesPolicy{
			IgnoreRules: []string{"*.i"},
		},
	})

	require.NoError(t, os.WriteFile(filepath.Join(dir, "file-a"), []byte{1, 2}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file-b"), []byte{1, 2, 3}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file-c"), []byte{1, 2, 3, 4}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file2.i"), []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "dir.i"), 0o755))

	eti, err := serverapi.Estimate(ctx, cli, &serverapi.EstimateRequest{
		Root: dir,
	})
	require.NoError(t, err)
	et := waitForTask(t, cli, eti.TaskID, 15*time.Second)

	require.Equal(t, et.Counters["Bytes"], uitask.BytesCounter(9))
	require.Equal(t, et.Counters["Directories"], uitask.SimpleCounter(1))
	require.Equal(t, et.Counters["Files"], uitask.SimpleCounter(3))
	require.Equal(t, et.Counters["Excluded Directories"], uitask.SimpleCounter(1))
	require.Equal(t, et.Counters["Excluded Files"], uitask.SimpleCounter(1))

	uresp, err := serverapi.UploadSnapshots(ctx, cli, &si)

	require.True(t, uresp.Sources[si.String()].Success)
	require.NoError(t, err)

	// wait until new task for the upload is created
	deadline := clock.Now().Add(30 * time.Second)

	latest := mustGetLatestTask(t, cli)
	for latest.TaskID == et.TaskID && clock.Now().Before(deadline) {
		time.Sleep(100 * time.Microsecond)

		latest = mustGetLatestTask(t, cli)
	}

	require.NotEqual(t, latest.TaskID, et.TaskID)

	ut := waitForTask(t, cli, mustGetLatestTask(t, cli).TaskID, 15*time.Second)

	t.Logf("got latest task: %v", ut)

	allTasks := mustListTasks(t, cli)

	for tid, tsk := range allTasks {
		t.Logf("allTasks[%v] =  %v", tid, tsk)
	}

	require.Equal(t, ut.Counters["Hashed Files"], uitask.SimpleCounter(3))
	require.Equal(t, ut.Counters["Hashed Bytes"], uitask.BytesCounter(9))
	require.Equal(t, ut.Counters["Excluded Directories"], uitask.SimpleCounter(1))
	require.Equal(t, ut.Counters["Excluded Files"], uitask.SimpleCounter(1))
	require.Equal(t, ut.Counters["Processed Files"], uitask.SimpleCounter(3))
}

func TestSourceRefreshesAfterPolicy(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	_ = ctx

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	dir := testutil.TempDirectory(t)
	si := env.LocalPathSourceInfo(dir)

	currentHour := clock.Now().Hour()

	mustCreateSource(t, cli, dir, &policy.Policy{
		SchedulingPolicy: policy.SchedulingPolicy{
			TimesOfDay: []policy.TimeOfDay{
				{Hour: (currentHour + 2) % 24, Minute: 33},
			},
			RunMissed: policy.NewOptionalBool(false),
		},
	})

	sources := mustListSources(t, cli, &snapshot.SourceInfo{})
	require.Len(t, sources, 1)
	require.NotNil(t, sources[0].NextSnapshotTime)
	require.Equal(t, 33, sources[0].NextSnapshotTime.Minute())

	mustSetPolicy(t, cli, si, &policy.Policy{
		SchedulingPolicy: policy.SchedulingPolicy{
			TimesOfDay: []policy.TimeOfDay{
				{Hour: (currentHour + 2) % 24, Minute: 55},
			},
			RunMissed: policy.NewOptionalBool(false),
		},
	})

	// make sure that soon after setting policy, the next snapshot time is up-to-date.
	match := false

	for range 15 {
		sources = mustListSources(t, cli, &snapshot.SourceInfo{})
		require.Len(t, sources, 1)
		require.NotNil(t, sources[0].NextSnapshotTime)

		if sources[0].NextSnapshotTime.Minute() == 55 {
			match = true
			break
		}

		time.Sleep(time.Second)
	}

	require.True(t, match)
}
