package sftp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitSSHArguments(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  []string
	}{
		{"-i /tmp/mykey", []string{"-i", "/tmp/mykey"}},
		{`-i "/tmp/my key"`, []string{"-i", "/tmp/my key"}},
		{`-i '/tmp/my key'`, []string{"-i", "/tmp/my key"}},
		{`-i /tmp/my\ key`, []string{"-i", "/tmp/my key"}},
		{`-i "/tmp/my key" -o StrictHostKeyChecking=no`, []string{"-i", "/tmp/my key", "-o", "StrictHostKeyChecking=no"}},
		{`-o  ProxyCommand="ssh -W %h:%p bastion"`, []string{"-o", "ProxyCommand=ssh -W %h:%p bastion"}},
		{"-o   Multiple    Spaces", []string{"-o", "Multiple", "Spaces"}},
		{`-o Empty=""`, []string{"-o", "Empty="}},
		{"", nil},
	} {
		got, err := splitSSHArguments(tc.input)
		require.NoError(t, err, tc.input)
		require.Equal(t, tc.want, got, tc.input)
	}

	_, err := splitSSHArguments(`-i "/tmp/unbalanced`)
	require.Error(t, err)

	_, err = splitSSHArguments(`-i '/tmp/unbalanced`)
	require.Error(t, err)
}
