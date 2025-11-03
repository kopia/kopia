package maintenancestats

import (
	"fmt"
	"time"

	"github.com/kopia/kopia/internal/contentlog"
)

const dropDeletedContentsStatsKind = "dropDeletedContentsStats"

// DropDeletedContentsStats are the stats for dropping deleted contents.
type DropDeletedContentsStats struct {
	DroppedBefore time.Time `json:"droppedBefore"`
}

// WriteValueTo writes the stats to JSONWriter.
func (ds *DropDeletedContentsStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(ds.Kind())
	jw.TimeField("droppedBefore", ds.DroppedBefore)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (ds *DropDeletedContentsStats) Summary() string {
	return fmt.Sprintf("Dropped deleted contents before %v", ds.DroppedBefore)
}

// Kind returns the kind name for the stats.
func (ds *DropDeletedContentsStats) Kind() string {
	return dropDeletedContentsStatsKind
}
