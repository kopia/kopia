package maintenance_test

import (
	"encoding/json"
	"testing"

	"github.com/kylelemons/godebug/pretty"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo/maintenance"
)

func (s *formatSpecificTestSuite) TestMaintenanceSchedule(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, s.formatVersion)

	sch, err := maintenance.GetSchedule(ctx, env.RepositoryWriter)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if !sch.NextFullMaintenanceTime.IsZero() {
		t.Errorf("unexpected NextFullMaintenanceTime: %v", sch.NextFullMaintenanceTime)
	}

	if !sch.NextQuickMaintenanceTime.IsZero() {
		t.Errorf("unexpected NextQuickMaintenanceTime: %v", sch.NextQuickMaintenanceTime)
	}

	sch.NextFullMaintenanceTime = clock.WallClockTime()
	sch.NextQuickMaintenanceTime = clock.WallClockTime()
	sch.ReportRun("foo", maintenance.RunInfo{
		Start:   clock.WallClockTime(),
		End:     clock.WallClockTime(),
		Success: true,
	})

	if err = maintenance.SetSchedule(ctx, env.RepositoryWriter, sch); err != nil {
		t.Fatalf("unable to set schedule: %v", err)
	}

	s2, err := maintenance.GetSchedule(ctx, env.RepositoryWriter)
	if err != nil {
		t.Fatalf("unable to get schedule: %v", err)
	}

	if got, want := toJSON(s2), toJSON(sch); got != want {
		t.Errorf("invalid schedule (-want,+got) %v", pretty.Compare(want, got))
	}
}

func toJSON(v interface{}) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}
