package policy

import (
	"context"
	"testing"

	"github.com/kopia/kopia/internal/repotesting"
)

func TestPolicyManager(t *testing.T) {
	ctx := context.Background()

	var env repotesting.Environment

	defer env.Setup(t).Close(ctx, t)

	r1 := env.Repository
	r2 := env.MustOpenAnother(t)
	sourceInfo := GlobalPolicySourceInfo

	must(t, SetPolicy(ctx, r1, sourceInfo, &Policy{
		RetentionPolicy: RetentionPolicy{
			KeepDaily: intPtr(44),
		},
	}))

	must(t, SetPolicy(ctx, r2, sourceInfo, &Policy{
		RetentionPolicy: RetentionPolicy{
			KeepDaily: intPtr(33),
		},
	}))

	must(t, r1.Flush(ctx))
	must(t, r2.Flush(ctx))

	r3 := env.MustOpenAnother(t)

	pi, err := GetDefinedPolicy(ctx, r3, sourceInfo)
	must(t, err)

	if got := *pi.RetentionPolicy.KeepDaily; got != 33 && got != 44 {
		t.Errorf("unexpected policy returned")
	}
}

func must(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}
