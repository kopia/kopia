package feature_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/feature"
)

func TestFeature(t *testing.T) {
	rf1 := feature.Required{
		Feature: "f1",
	}

	rf2 := feature.Required{
		Feature: "f2",
	}

	rf3 := feature.Required{
		Feature: "f3",
	}

	cases := []struct {
		required  []feature.Required
		supported []feature.Feature
		want      []feature.Required
	}{
		{nil, nil, nil},
		{
			[]feature.Required{rf1, rf2, rf3},
			[]feature.Feature{"f1", "f2"},
			[]feature.Required{rf3},
		},
		{
			[]feature.Required{rf1},
			[]feature.Feature{"f1", "f2"},
			nil,
		},
		{
			[]feature.Required{rf1, rf2, rf3},
			[]feature.Feature{"f1", "f2", "f3"},
			nil,
		},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, feature.GetUnsupportedFeatures(tc.required, tc.supported))
	}
}

func TestFeatureUnsupportedMessage(t *testing.T) {
	cases := map[feature.Required]string{
		{
			Feature:         "f1",
			IfNotUnderstood: feature.IfNotUnderstood{},
		}: "This version of Kopia does not support feature 'f1'.",
		{
			Feature: "f2",
			IfNotUnderstood: feature.IfNotUnderstood{
				URL: "http://some-url",
			},
		}: "This version of Kopia does not support feature 'f2'.\nSee: http://some-url",
		{
			Feature: "f3",
			IfNotUnderstood: feature.IfNotUnderstood{
				Message:          "Upgrade is required for better performance.",
				URL:              "http://some-url",
				UpgradeToVersion: "2.3.4",
			},
		}: "This version of Kopia does not support feature 'f3'.\nUpgrade is required for better performance.\nSee: http://some-url\nPlease upgrade to version 2.3.4 or newer.",
	}

	for input, want := range cases {
		require.Equal(t, want, input.UnsupportedMessage())
	}
}
