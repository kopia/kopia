package envflag

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBool(t *testing.T) {
	const varName = "KOPIA_TESTING_ENVFLAG_BOOL"

	// ensure the variable is not set before testing it
	v, found := os.LookupEnv(varName)
	require.Falsef(t, found, "the environment var %q is expected to be unset, got %q", varName, v)
	require.False(t, Bool(varName))

	cases := []struct {
		envVal string
		want   bool
	}{
		{
			envVal: "",
			want:   false,
		},
		{
			envVal: "0",
			want:   false,
		},
		{
			envVal: "bad-value",
			want:   false,
		},
		{
			envVal: "FaLse",
			want:   false,
		},
		{
			envVal: "f",
			want:   false,
		},
		{
			envVal: "1",
			want:   true,
		},
		{
			envVal: "t",
			want:   true,
		},
		{
			envVal: "T",
			want:   true,
		},
		{
			envVal: "true",
			want:   true,
		},
		{
			envVal: "truE",
			want:   true,
		},
		{
			envVal: "True",
			want:   true,
		},
		{
			envVal: "TRUE",
			want:   true,
		},
	}

	for _, c := range cases {
		t.Run(c.envVal, func(t *testing.T) {
			t.Setenv(varName, c.envVal)
			got := Bool(varName)
			require.Equal(t, c.want, got)
		})
	}
}

func TestBoolDefault(t *testing.T) {
	const varName = "KOPIA_TESTING_ENVFLAG_BOOL"

	// ensure the variable is not set before testing it
	v, found := os.LookupEnv(varName)
	require.Falsef(t, found, "the environment var %q is expected to be unset, got %q", varName, v)
	require.False(t, BoolDefault(varName, false))
	require.True(t, BoolDefault(varName, true))

	cases := []struct {
		envVal       string
		defaultValue bool
		want         bool
	}{
		{
			defaultValue: false,
			envVal:       "",
			want:         false,
		},
		{
			defaultValue: true,
			envVal:       "",
			want:         true,
		},
		{
			defaultValue: false,
			envVal:       "0",
			want:         false,
		},
		{
			defaultValue: true,
			envVal:       "0",
			want:         false,
		},
		{
			defaultValue: false,
			envVal:       "bad-value",
			want:         false,
		},
		{
			defaultValue: true,
			envVal:       "bad-value",
			want:         true,
		},
		{
			defaultValue: true,
			envVal:       "FaLse",
			want:         false,
		},
		{
			defaultValue: false,
			envVal:       "FaLse",
			want:         false,
		},
		{
			defaultValue: false,
			envVal:       "f",
			want:         false,
		},
		{
			defaultValue: true,
			envVal:       "f",
			want:         false,
		},
		{
			defaultValue: true,
			envVal:       "1",
			want:         true,
		},
		{
			defaultValue: false,
			envVal:       "1",
			want:         true,
		},
		{
			defaultValue: true,
			envVal:       "t",
			want:         true,
		},
		{
			defaultValue: false,
			envVal:       "t",
			want:         true,
		},
		{
			defaultValue: false,
			envVal:       "T",
			want:         true,
		},
		{
			defaultValue: true,
			envVal:       "T",
			want:         true,
		},
		{
			defaultValue: false,
			envVal:       "true",
			want:         true,
		},
		{
			defaultValue: true,
			envVal:       "true",
			want:         true,
		},
		{
			defaultValue: false,
			envVal:       "truE",
			want:         true,
		},
		{
			defaultValue: true,
			envVal:       "truE",
			want:         true,
		},
		{
			defaultValue: false,
			envVal:       "True",
			want:         true,
		},
		{
			defaultValue: true,
			envVal:       "True",
			want:         true,
		},
		{
			defaultValue: false,
			envVal:       "TRUE",
			want:         true,
		},
		{
			defaultValue: true,
			envVal:       "TRUE",
			want:         true,
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprint(c.envVal, "+", c.defaultValue), func(t *testing.T) {
			t.Setenv(varName, c.envVal)
			got := BoolDefault(varName, c.defaultValue)
			require.Equal(t, c.want, got)
		})
	}
}
