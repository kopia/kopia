package maintenance_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/maintenancestats"
	"github.com/kopia/kopia/repo/object"
)

func (s *formatSpecificTestSuite) TestContentRewrite(t *testing.T) {
	cases := []struct {
		numPContents int
		numQContents int
		opt          *maintenance.RewriteContentsOptions
		wantPDelta   int
		wantQDelta   int
		stats        *maintenancestats.RewriteContentsStats
	}{
		{
			numPContents: 2,
			numQContents: 3,
			opt: &maintenance.RewriteContentsOptions{
				ShortPacks: true,
			},
			wantPDelta: 1,
			wantQDelta: 1,
			stats: &maintenancestats.RewriteContentsStats{
				ToRewriteContentCount: 5,
				ToRewriteContentSize:  320,
				RewrittenContentCount: 5,
				RewrittenContentSize:  320,
			},
		},
		{
			numPContents: 2,
			numQContents: 3,
			opt: &maintenance.RewriteContentsOptions{
				ShortPacks: true,
				DryRun:     true,
			},
			wantPDelta: 0,
			wantQDelta: 0,
			stats: &maintenancestats.RewriteContentsStats{
				ToRewriteContentCount: 5,
				ToRewriteContentSize:  320,
			},
		},
		{
			numPContents: 2,
			numQContents: 3,
			opt: &maintenance.RewriteContentsOptions{
				ShortPacks: true,
				PackPrefix: "p",
			},
			wantPDelta: 1,
			wantQDelta: 0,
			stats: &maintenancestats.RewriteContentsStats{
				ToRewriteContentCount: 2,
				ToRewriteContentSize:  128,
				RewrittenContentCount: 2,
				RewrittenContentSize:  128,
			},
		},
		{
			numPContents: 1,
			numQContents: 0,
			opt: &maintenance.RewriteContentsOptions{
				ShortPacks: true,
			},
			wantPDelta: 0, // single pack won't get rewritten
			wantQDelta: 0,
			stats:      &maintenancestats.RewriteContentsStats{},
		},
		{
			numPContents: 1,
			numQContents: 1,
			opt: &maintenance.RewriteContentsOptions{
				ShortPacks: true,
			},
			wantPDelta: 0,
			wantQDelta: 0,
			stats:      &maintenancestats.RewriteContentsStats{},
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("case-%v", tc), func(t *testing.T) {
			ctx, env := repotesting.NewEnvironment(t, s.formatVersion)

			// run N sessions to create N individual pack blobs for each content prefix
			for range tc.numPContents {
				require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
					ow := w.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
					fmt.Fprintf(ow, "%v", uuid.NewString())
					_, err := ow.Result()

					return err
				}))
			}

			for range tc.numQContents {
				require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
					ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "k", MetadataCompressor: "zstd-fastest"})
					fmt.Fprintf(ow, "%v", uuid.NewString())
					_, err := ow.Result()

					return err
				}))
			}

			pBlobsBefore, err := blob.ListAllBlobs(ctx, env.RepositoryWriter.BlobStorage(), "p")
			require.NoError(t, err)

			qBlobsBefore, err := blob.ListAllBlobs(ctx, env.RepositoryWriter.BlobStorage(), "q")
			require.NoError(t, err)

			var stats *maintenancestats.RewriteContentsStats

			require.NoError(t, repo.DirectWriteSession(ctx, env.RepositoryWriter, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.DirectRepositoryWriter) error {
				stats, err = maintenance.RewriteContents(ctx, w, tc.opt, maintenance.SafetyNone)
				return err
			}))

			pBlobsAfter, err := blob.ListAllBlobs(ctx, env.RepositoryWriter.BlobStorage(), "p")
			require.NoError(t, err)

			qBlobsAfter, err := blob.ListAllBlobs(ctx, env.RepositoryWriter.BlobStorage(), "q")
			require.NoError(t, err)

			require.Equal(t, tc.wantPDelta, len(pBlobsAfter)-len(pBlobsBefore), "invalid p blob count delta")
			require.Equal(t, tc.wantQDelta, len(qBlobsAfter)-len(qBlobsBefore), "invalid q blob count delta")

			if tc.stats == nil {
				require.Nil(t, stats)
			} else {
				require.Equal(t, *tc.stats, *stats)
			}
		})
	}
}

// TestContentRewriteUnrewritableContents verifies that a failure to rewrite a content that is
// already marked deleted (e.g. because its pack blob is missing) is skipped and does not fail
// the run, while the same failure for a live content still fails it.
func (s *formatSpecificTestSuite) TestContentRewriteUnrewritableContents(t *testing.T) {
	cases := []struct {
		name                    string
		markDeleted             bool
		wantErr                 string
		wantSkippedDeletedCount uint64
	}{
		{
			name:                    "deleted content is skipped",
			markDeleted:             true,
			wantSkippedDeletedCount: 1,
		},
		{
			name:        "live content fails the run",
			markDeleted: false,
			wantErr:     "failed to rewrite 1 contents",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ta := faketime.NewClockTimeWithOffset(0)

			ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
				OpenOptions: func(o *repo.Options) {
					o.TimeNowFunc = ta.NowFunc()
				},
			})

			// write a single content, which lands in its own pack blob
			ow := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
			fmt.Fprintf(ow, "%v", uuid.NewString())
			_, err := ow.Result()
			require.NoError(t, err)
			require.NoError(t, env.RepositoryWriter.Flush(ctx))

			var infos []content.Info

			require.NoError(t, env.RepositoryWriter.ContentReader().IterateContents(ctx, content.IterateOptions{}, func(ci content.Info) error {
				infos = append(infos, ci)
				return nil
			}))
			require.Len(t, infos, 1)

			ci := infos[0]

			if tc.markDeleted {
				require.NoError(t, env.RepositoryWriter.ContentManager().DeleteContent(ctx, ci.ContentID))
				require.NoError(t, env.RepositoryWriter.Flush(ctx))
			}

			// remove the pack blob holding the content data so the content can no longer be read
			require.NoError(t, env.RepositoryWriter.BlobStorage().DeleteBlob(ctx, ci.PackBlobID))

			// advance the clock so the deleted index entry (timestamped 1s after the original
			// entry) is no longer considered too new to rewrite
			ta.Advance(2 * time.Hour)

			var stats *maintenancestats.RewriteContentsStats

			err = repo.DirectWriteSession(ctx, env.RepositoryWriter, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.DirectRepositoryWriter) error {
				var err2 error

				stats, err2 = maintenance.RewriteContents(ctx, w, &maintenance.RewriteContentsOptions{
					ContentIDs: []content.ID{ci.ContentID},
				}, maintenance.SafetyNone)

				return err2
			})

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, uint64(1), stats.ToRewriteContentCount)
				require.Equal(t, uint64(0), stats.RewrittenContentCount)
				require.Equal(t, tc.wantSkippedDeletedCount, stats.SkippedDeletedContentCount)
				require.Equal(t, stats.ToRewriteContentSize, stats.SkippedDeletedContentSize)
			}
		})
	}
}
