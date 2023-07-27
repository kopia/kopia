package providervalidation_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
)

func TestProviderValidation(t *testing.T) {
	ctx := testlogging.Context(t)
	m := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(m, nil, nil)
	opt := providervalidation.DefaultOptions
	opt.ConcurrencyTestDuration = 3 * time.Second
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, opt))
}
