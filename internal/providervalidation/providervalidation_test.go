package providervalidation_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob/filesystem"
)

func TestProviderValidation(t *testing.T) {
	ctx := testlogging.Context(t)
	st, err := filesystem.New(ctx, &filesystem.Options{
		Path: t.TempDir(),
	}, false)
	require.NoError(t, err)

	opt := blobtesting.TestValidationOptions
	opt.ConcurrencyTestDuration = 3 * time.Second
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, opt))
}
