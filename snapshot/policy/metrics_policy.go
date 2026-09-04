package policy

import "github.com/kopia/kopia/snapshot"

// MetricsPolicy controls metrics-related behavior when taking snapshots.
type MetricsPolicy struct {
	// ExposeMetrics controls whether metrics should be exposed
	ExposeMetrics *OptionalBool `json:"exposeMetrics,omitempty"`
}

// MetricsPolicyDefinition specifies which policy definition provided the value of a particular field.
type MetricsPolicyDefinition struct {
	ExposeMetrics snapshot.SourceInfo `json:"exposeMetrics,omitempty"`
}

// Merge applies default values from the provided policy.
func (p *MetricsPolicy) Merge(src MetricsPolicy, def *MetricsPolicyDefinition, si snapshot.SourceInfo) {
	mergeOptionalBool(&p.ExposeMetrics, src.ExposeMetrics, &def.ExposeMetrics, si)
}
