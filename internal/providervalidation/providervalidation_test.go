package providervalidation_test

import (
	"testing"
	"time"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
)

func TestProviderValidation(t *testing.T) {
	ctx := testlogging.Context(t)
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	opt := providervalidation.DefaultOptions
	opt.ConcurrencyTestDuration = 15 * time.Second
	providervalidation.ValidateProvider(ctx, st, opt)
}
