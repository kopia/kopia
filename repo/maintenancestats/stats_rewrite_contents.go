package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/units"
)

const rewriteContentsStatsKind = "rewriteContentsStats"

// RewriteContentsStats are the stats for rewriting contents.
type RewriteContentsStats struct {
	ToRewriteContentCount uint64 `json:"toRewriteContentCount"`
	ToRewriteContentSize  uint64 `json:"toRewriteContentSize"`
	RewrittenContentCount uint64 `json:"rewrittenContentCount"`
	RewrittenContentSize  uint64 `json:"rewrittenContentSize"`
	RetainedContentCount  uint64 `json:"retainedContentCount"`
	RetainedContentSize   uint64 `json:"retainedContentSize"`
}

// WriteValueTo writes the stats to JSONWriter.
func (rs *RewriteContentsStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(rs.Kind())
	jw.UInt64Field("toRewriteContentCount", rs.ToRewriteContentCount)
	jw.UInt64Field("toRewriteContentSize", rs.ToRewriteContentSize)
	jw.UInt64Field("rewrittenContentCount", rs.RewrittenContentCount)
	jw.UInt64Field("rewrittenContentSize", rs.RewrittenContentSize)
	jw.UInt64Field("retainedContentCount", rs.RetainedContentCount)
	jw.UInt64Field("retainedContentSize", rs.RetainedContentSize)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (rs *RewriteContentsStats) Summary() string {
	return fmt.Sprintf("Found %v(%v) contents to rewrite and rewrote %v(%v). Retained %v(%v) contents from rewrite",
		rs.ToRewriteContentCount, units.BytesString(rs.ToRewriteContentSize), rs.RewrittenContentCount, units.BytesString(rs.RewrittenContentSize), rs.RetainedContentCount, units.BytesString(rs.RetainedContentSize))
}

// Kind returns the kind name for the stats.
func (rs *RewriteContentsStats) Kind() string {
	return rewriteContentsStatsKind
}
