package maintenance

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kylelemons/godebug/pretty"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/repotesting"
)

func TestMaintenanceSchedule(t *testing.T) {
	ctx := context.Background()

	var env repotesting.Environment
	defer env.Setup(t).Close(ctx, t)

	s, err := GetSchedule(ctx, env.Repository)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if !s.NextFullMaintenanceTime.IsZero() {
		t.Errorf("unexpected NextFullMaintenanceTime: %v", s.NextFullMaintenanceTime)
	}

	if !s.NextQuickMaintenanceTime.IsZero() {
		t.Errorf("unexpected NextQuickMaintenanceTime: %v", s.NextQuickMaintenanceTime)
	}

	s.NextFullMaintenanceTime = clock.Now()
	s.NextQuickMaintenanceTime = clock.Now()
	s.ReportRun("foo", RunInfo{
		Start:   clock.Now(),
		End:     clock.Now(),
		Success: true,
	})

	if err = SetSchedule(ctx, env.Repository, s); err != nil {
		t.Fatalf("unable to set schedule: %v", err)
	}

	s2, err := GetSchedule(ctx, env.Repository)
	if err != nil {
		t.Fatalf("unable to get schedule: %v", err)
	}

	if got, want := toJSON(s2), toJSON(s); got != want {
		t.Errorf("invalid schedule (-want,+got) %v", pretty.Compare(want, got))
	}
}

func toJSON(v interface{}) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}
