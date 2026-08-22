package cli

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestApplyPolicyStringListClearAndAdd is a regression test for
// https://github.com/kopia/kopia/issues/5323: combining --clear-ignore with
// --add-ignore in a single "policy set" invocation cleared the list but
// dropped the added entries, because the clearList branch returned early
// before applying add/remove.
func TestApplyPolicyStringListClearAndAdd(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name        string
		initial     []string
		add         []string
		remove      []string
		clearList   bool
		want        []string
		wantChanged bool
	}{
		{
			name:        "clear and add together keeps the added entry",
			initial:     []string{"old"},
			add:         []string{"new"},
			clearList:   true,
			want:        []string{"new"},
			wantChanged: true,
		},
		{
			name:        "clear only empties the list",
			initial:     []string{"a", "b"},
			clearList:   true,
			want:        nil,
			wantChanged: true,
		},
		{
			name:        "add only appends to the existing list",
			initial:     []string{"a"},
			add:         []string{"b"},
			want:        []string{"a", "b"},
			wantChanged: true,
		},
		{
			name:        "clear, add and remove compose",
			initial:     []string{"old"},
			add:         []string{"keep", "drop"},
			remove:      []string{"drop"},
			clearList:   true,
			want:        []string{"keep"},
			wantChanged: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			val := append([]string(nil), tc.initial...)
			changeCount := 0

			applyPolicyStringList(ctx, "test list", &val, tc.add, tc.remove, tc.clearList, &changeCount)

			require.Equal(t, tc.want, val)
			require.Equal(t, tc.wantChanged, changeCount > 0)
		})
	}
}
