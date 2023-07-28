package server_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/servertesting"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func TestPolicies(t *testing.T) {
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

	dir0 := testutil.TempDirectory(t)
	si0 := env.LocalPathSourceInfo(dir0)

	dir1 := testutil.TempDirectory(t)
	si1 := env.LocalPathSourceInfo(dir1)

	dir2 := filepath.Join(dir1, "subdir1")
	si2 := env.LocalPathSourceInfo(dir2)

	dir3 := filepath.Join(dir2, "subdir2")
	si3 := env.LocalPathSourceInfo(dir3)

	dir4 := filepath.Join(dir3, "subdir3")
	si4 := env.LocalPathSourceInfo(dir4)

	mustSetPolicy(t, cli, si1, &policy.Policy{
		CompressionPolicy: policy.CompressionPolicy{
			CompressorName: "some-compressor",
		},
	})

	mustSetPolicy(t, cli, si2, &policy.Policy{
		CompressionPolicy: policy.CompressionPolicy{
			NeverCompress: []string{"a", "b"},
		},
		SchedulingPolicy: policy.SchedulingPolicy{
			IntervalSeconds: 60,
		},
	})

	mustSetPolicy(t, cli, si3, &policy.Policy{
		CompressionPolicy: policy.CompressionPolicy{
			CompressorName: "some-compressor3",
		},
	})

	cases := []struct {
		si                              snapshot.SourceInfo
		updates                         *policy.Policy
		wantCompressorName              compression.Name
		wantCompressorNameSource        snapshot.SourceInfo
		wantNeverCompress               []string
		wantNeverCompressSource         snapshot.SourceInfo
		wantUpcomingSnapshotTimesLength int
		wantSchedulingError             string
	}{
		{
			si:                       si0,
			wantCompressorName:       compression.Name("none"),
			wantNeverCompress:        nil,
			wantCompressorNameSource: policy.GlobalPolicySourceInfo,
			wantNeverCompressSource:  policy.GlobalPolicySourceInfo,
		},
		{
			si:                       si1,
			wantCompressorName:       compression.Name("some-compressor"),
			wantNeverCompress:        nil,
			wantCompressorNameSource: si1,
			wantNeverCompressSource:  policy.GlobalPolicySourceInfo,
		},
		{
			si:                       si1,
			wantCompressorName:       compression.Name("some-compressor-updated"),
			wantNeverCompress:        nil,
			wantCompressorNameSource: si1,
			wantNeverCompressSource:  policy.GlobalPolicySourceInfo,
			updates: &policy.Policy{
				CompressionPolicy: policy.CompressionPolicy{
					CompressorName: "some-compressor-updated",
				},
			},
		},
		{
			si:                              si2,
			wantCompressorName:              compression.Name("some-compressor"), // inherited
			wantNeverCompress:               []string{"a", "b"},
			wantCompressorNameSource:        si1,
			wantNeverCompressSource:         si2,
			wantUpcomingSnapshotTimesLength: 3,
		},
		{
			si:                              si3,
			wantCompressorName:              compression.Name("some-compressor3"),
			wantNeverCompress:               []string{"a", "b"},
			wantCompressorNameSource:        si3,
			wantNeverCompressSource:         si2,
			wantUpcomingSnapshotTimesLength: 3,
		},
		{
			si:                              si4,
			wantCompressorName:              compression.Name("some-compressor3"),
			wantNeverCompress:               []string{"a", "b"},
			wantCompressorNameSource:        si3,
			wantNeverCompressSource:         si2,
			wantUpcomingSnapshotTimesLength: 3,
		},
		{
			si:                       si4,
			wantCompressorName:       compression.Name("some-compressor-updated"),
			wantNeverCompress:        []string{"a", "b"},
			wantCompressorNameSource: si4,
			wantNeverCompressSource:  si2,
			updates: &policy.Policy{
				CompressionPolicy: policy.CompressionPolicy{
					CompressorName: "some-compressor-updated",
				},
			},
			wantUpcomingSnapshotTimesLength: 3,
		},
		{
			si:                       si0,
			wantCompressorName:       compression.Name("none"),
			wantNeverCompress:        nil,
			wantCompressorNameSource: policy.GlobalPolicySourceInfo,
			wantNeverCompressSource:  policy.GlobalPolicySourceInfo,
			updates: &policy.Policy{
				SchedulingPolicy: policy.SchedulingPolicy{
					Cron: []string{"invalid"},
				},
			},
			wantSchedulingError: "invalid cron expression \"invalid\"",
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprintf("case-%v", i), func(t *testing.T) {
			res, err2 := serverapi.ResolvePolicy(ctx, cli, tc.si, &serverapi.ResolvePolicyRequest{
				Updates:                  tc.updates,
				NumUpcomingSnapshotTimes: 3,
			})
			require.NoError(t, err2)
			require.Equal(t, tc.wantCompressorName, res.Effective.CompressionPolicy.CompressorName)
			require.Equal(t, tc.wantNeverCompress, res.Effective.CompressionPolicy.NeverCompress)
			require.Equal(t, tc.wantCompressorNameSource, res.Definition.CompressionPolicy.CompressorName)
			require.Equal(t, tc.wantNeverCompressSource, res.Definition.CompressionPolicy.NeverCompress)
			require.Len(t, res.UpcomingSnapshotTimes, tc.wantUpcomingSnapshotTimesLength)
			require.Equal(t, tc.wantSchedulingError, res.SchedulingError)

			for j, ust := range res.UpcomingSnapshotTimes {
				require.Equal(t, ust.Truncate(60*time.Second), ust)
				if j > 0 {
					require.Equal(t, 60*time.Second, ust.Sub(res.UpcomingSnapshotTimes[j-1]))
				}
			}
		})
	}
}
