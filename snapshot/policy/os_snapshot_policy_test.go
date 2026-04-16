package policy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOSSnapshotMode(t *testing.T) {
	assert.Equal(t, OSSnapshotNever, (*OSSnapshotMode)(nil).OrDefault(OSSnapshotNever))
	assert.Equal(t, OSSnapshotAlways, NewOSSnapshotMode(OSSnapshotAlways).OrDefault(OSSnapshotNever))

	cases := []struct {
		m OSSnapshotMode
		s string
	}{
		{OSSnapshotNever, "never"},
		{OSSnapshotAlways, "always"},
		{OSSnapshotWhenAvailable, "when-available"},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.s, tc.m.String())
	}
}
