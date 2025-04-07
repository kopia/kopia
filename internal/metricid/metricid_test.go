package metricid_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/metricid"
)

func TestMappings(t *testing.T) {
	verifyMapping(t, metricid.Counters)
	verifyMapping(t, metricid.DurationDistributions)
	verifyMapping(t, metricid.SizeDistributions)
}

func verifyMapping(t *testing.T, mapping *metricid.Mapping) {
	t.Helper()

	id2name := map[int]string{}
	maxv := 0

	for k, v := range mapping.NameToIndex {
		_, ok := id2name[v]
		require.False(t, ok, "duplicate ID", v)

		if v > maxv {
			maxv = v
		}

		id2name[v] = k

		require.Equal(t, k, mapping.IndexToName[v])
	}

	// make sure we use consecurive numbers
	require.Len(t, id2name, maxv)
	require.Equal(t, mapping.MaxIndex, maxv)
}
