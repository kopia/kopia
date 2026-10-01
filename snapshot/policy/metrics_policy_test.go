package policy

import (
	"reflect"
	"testing"

	"github.com/kopia/kopia/snapshot"
)

func TestMetricsPolicyMerge(t *testing.T) {
	cases := []struct {
		name     string
		policy   MetricsPolicy
		src      MetricsPolicy
		expected MetricsPolicy
	}{
		{
			name:     "Both nil",
			policy:   MetricsPolicy{},
			src:      MetricsPolicy{},
			expected: MetricsPolicy{},
		},
		{
			name:     "Source true",
			policy:   MetricsPolicy{},
			src:      MetricsPolicy{ExposeMetrics: NewOptionalBool(true)},
			expected: MetricsPolicy{ExposeMetrics: NewOptionalBool(true)},
		},
		{
			name:     "Source false",
			policy:   MetricsPolicy{},
			src:      MetricsPolicy{ExposeMetrics: NewOptionalBool(false)},
			expected: MetricsPolicy{ExposeMetrics: NewOptionalBool(false)},
		},
		{
			name:     "Policy set, source different",
			policy:   MetricsPolicy{ExposeMetrics: NewOptionalBool(true)},
			src:      MetricsPolicy{ExposeMetrics: NewOptionalBool(false)},
			expected: MetricsPolicy{ExposeMetrics: NewOptionalBool(true)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			def := &MetricsPolicyDefinition{}
			tc.policy.Merge(tc.src, def, snapshot.SourceInfo{})

			if !reflect.DeepEqual(tc.policy, tc.expected) {
				t.Errorf("Merge result not as expected.\nGot: %+v\nWant: %+v", tc.policy, tc.expected)
			}
		})
	}
}
