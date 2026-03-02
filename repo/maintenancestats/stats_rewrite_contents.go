package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/units"
)

const rewriteContentsStatsKind = "rewriteContentsStats"

// RewriteContentsStats are the stats for rewriting contents.
type RewriteContentsStats struct {
	ToRewriteContentCount int   `json:"toRewriteContentCount"`
	ToRewriteContentSize  int64 `json:"toRewriteContentSize"`
	RewrittenContentCount int   `json:"rewrittenContentCount"`
	RewrittenContentSize  int64 `json:"rewrittenContentSize"`
	RetainedContentCount  int   `json:"retainedContentCount"`
	RetainedContentSize   int64 `json:"retainedContentSize"`
}

// WriteValueTo writes the stats to JSONWriter.
func (rs *RewriteContentsStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(rs.Kind())
	jw.IntField("toRewriteContentCount", rs.ToRewriteContentCount)
	jw.Int64Field("toRewriteContentSize", rs.ToRewriteContentSize)
	jw.IntField("rewrittenContentCount", rs.RewrittenContentCount)
	jw.Int64Field("rewrittenContentSize", rs.RewrittenContentSize)
	jw.IntField("retainedContentCount", rs.RetainedContentCount)
	jw.Int64Field("retainedContentSize", rs.RetainedContentSize)
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
