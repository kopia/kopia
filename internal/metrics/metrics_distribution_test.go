package metrics

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBucketForThresholds(t *testing.T) {
	buckets := IOLatencyThresholds.values
	n := len(buckets)

	assert.Equal(t, 0, bucketForThresholds(buckets, buckets[0]-1))

	for i := range n {
		assert.Equal(t, i, bucketForThresholds(buckets, buckets[i]-1))
		assert.Equal(t, i, bucketForThresholds(buckets, buckets[i]))
		assert.Equal(t, i+1, bucketForThresholds(buckets, buckets[i]+1), "looking for %v", buckets[i]+1)
	}

	assert.Equal(t, n, bucketForThresholds(buckets, math.MaxInt64))
}
