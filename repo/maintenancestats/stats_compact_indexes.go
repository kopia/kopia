package maintenancestats

import (
	"fmt"
	"time"

	"github.com/kopia/kopia/internal/contentlog"
)

const compactIndexesStatsKind = "compactIndexesStats"

// CompactIndexesStats are the stats for dropping deleted contents.
type CompactIndexesStats struct {
	DroppedContentsDeletedBefore time.Time `json:"droppedContentsDeletedBefore"`
}

// WriteValueTo writes the stats to JSONWriter.
func (cs *CompactIndexesStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.TimeField("droppedContentsDeletedBefore", cs.DroppedContentsDeletedBefore)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (cs *CompactIndexesStats) Summary() string {
	return fmt.Sprintf("Dropped contents deleted before %v", cs.DroppedContentsDeletedBefore)
}

// Kind returns the kind name for the stats.
func (cs *CompactIndexesStats) Kind() string {
	return compactIndexesStatsKind
}
