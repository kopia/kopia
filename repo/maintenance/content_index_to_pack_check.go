package maintenance

import (
	"context"
	"math/rand/v2"
	"os"
	"strconv"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/maintenancestats"
)

// Checks the consistency of the mapping from content index entries to packs,
// to verify that all the referenced packs are present in storage.
func checkContentIndexToPacks(ctx context.Context, r content.Reader) error {
	const verifyContentsDefaultParallelism = 5

	opts := content.VerifyOptions{
		ContentIDRange:            index.AllIDs,
		ContentReadPercentage:     0,
		IncludeDeletedContents:    true,
		ContentIterateParallelism: verifyContentsDefaultParallelism,
	}

	if err := r.VerifyContents(ctx, opts); err != nil {
		return errors.Wrap(err, "maintenance verify contents")
	}

	return nil
}

func shouldRunContentIndexVerify(ctx context.Context) bool {
	const envName = "KOPIA_MAINTENANCE_CONTENT_VERIFY_PERCENTAGE"

	v := os.Getenv(envName)
	if v == "" {
		return false
	}

	percentage, err := strconv.ParseFloat(v, 64)
	if err != nil {
		userLog(ctx).Warnf("The '%s' environment variable appears to have a non numeric value: '%q', %s", envName, v, err)

		return false
	}

	if rand.Float64() < percentage/100 { //nolint:gosec
		return true
	}

	return false
}

func reportRunAndMaybeCheckContentIndex(ctx context.Context, rep repo.DirectRepositoryWriter, taskType TaskType, s *Schedule, run func() (maintenancestats.Kind, error)) error {
	if !shouldRunContentIndexVerify(ctx) {
		return ReportRun(ctx, rep, taskType, s, run)
	}

	return ReportRun(ctx, rep, taskType, s, func() (maintenancestats.Kind, error) {
		if err := checkContentIndexToPacks(ctx, rep.ContentReader()); err != nil {
			return nil, err
		}

		stats, err := run()
		if err != nil {
			return nil, err
		}

		if err := checkContentIndexToPacks(ctx, rep.ContentReader()); err != nil {
			return nil, err
		}

		return stats, nil
	})
}
