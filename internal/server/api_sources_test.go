package server_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func TestSnapshotCounters(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := startServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            testUIUsername,
		Password:                            testUIPassword,
	})

	require.NoError(t, err)

	dir := testutil.TempDirectory(t)
	si := localSource(env, dir)

	mustCreateSource(t, cli, dir)
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

	ut := waitForTask(t, cli, mustGetLatestTask(t, cli).TaskID, 15*time.Second)

	require.Equal(t, ut.Counters["Hashed Files"], uitask.SimpleCounter(3))
	require.Equal(t, ut.Counters["Hashed Bytes"], uitask.BytesCounter(9))
	require.Equal(t, ut.Counters["Excluded Directories"], uitask.SimpleCounter(1))
	require.Equal(t, ut.Counters["Excluded Files"], uitask.SimpleCounter(1))
	require.Equal(t, ut.Counters["Processed Files"], uitask.SimpleCounter(3))
}
