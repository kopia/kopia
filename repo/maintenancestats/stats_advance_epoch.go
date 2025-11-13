package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
)

const advanceEpochStatsKind = "advanceEpochStats"

// AdvanceEpochStats are the stats for advancing write epoch.
type AdvanceEpochStats struct {
	CurrentEpoch int  `json:"currentEpoch"`
	WasAdvanced  bool `json:"wasAdvanced"`
}

// WriteValueTo writes the stats to JSONWriter.
func (as *AdvanceEpochStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(as.Kind())
	jw.IntField("currentEpoch", as.CurrentEpoch)
	jw.BoolField("wasAdvanced", as.WasAdvanced)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (as *AdvanceEpochStats) Summary() string {
	var message string
	if as.WasAdvanced {
		message = fmt.Sprintf("Advanced epoch to %v", as.CurrentEpoch)
	} else {
		message = fmt.Sprintf("Stay at epoch %v", as.CurrentEpoch)
	}

	return message
}

// Kind returns the kind name for the stats.
func (as *AdvanceEpochStats) Kind() string {
	return advanceEpochStatsKind
}
