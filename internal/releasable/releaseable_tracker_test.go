package releasable_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/releasable"
)

func TestReleaseable(t *testing.T) {
	releasable.EnableTracking("some-kind")
	require.Contains(t, releasable.Active(), releasable.ItemKind("some-kind"))

	releasable.Created("some-kind", 1)
	assert.Len(t, releasable.Active()["some-kind"], 1)
	releasable.Created("some-kind", 2)
	assert.Len(t, releasable.Active()["some-kind"], 2)
	releasable.Released("some-kind", 1)
	assert.Len(t, releasable.Active()["some-kind"], 1)

	require.ErrorContains(t, releasable.Verify(), "found 1 \"some-kind\" resources that have not been released")

	releasable.Released("some-kind", 2)
	assert.Empty(t, releasable.Active()["some-kind"])
	releasable.Released("some-kind", 2)
	assert.Empty(t, releasable.Active()["some-kind"])

	releasable.DisableTracking("some-kind")
	require.NotContains(t, releasable.Active(), releasable.ItemKind("some-kind"))

	require.NoError(t, releasable.Verify())

	// no-ops
	releasable.Created("some-kind", 1)
	releasable.Released("some-kind", 2)

	releasable.EnableTracking("some-kind")
	releasable.Created("some-kind", 1)
	releasable.EnableTracking("some-kind")
	releasable.Created("some-kind", 2)
	require.ErrorContains(t, releasable.Verify(), "found 2 \"some-kind\" resources that have not been released")
	releasable.DisableTracking("some-kind")
}
