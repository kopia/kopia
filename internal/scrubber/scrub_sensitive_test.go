package scrubber_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/scrubber"
)

type S struct {
	SomePassword1 string `kopia:"sensitive"`
	NonPassword   string
	Inner         *Q
}

type Q struct {
	SomePassword1 string `kopia:"sensitive"`
	NonPassword   string
}

func TestScrubber(t *testing.T) {
	input := &S{
		SomePassword1: "foo",
		NonPassword:   "bar",
		Inner: &Q{
			SomePassword1: "foo",
			NonPassword:   "bar",
		},
	}

	want := &S{
		SomePassword1: "***",
		NonPassword:   "bar",
		Inner: &Q{
			SomePassword1: "foo",
			NonPassword:   "bar",
		},
	}

	output := scrubber.ScrubSensitiveData(reflect.ValueOf(input)).Interface()
	require.Equal(t, want, output)
}

func TestScrubberPanicsOnNonStruct(t *testing.T) {
	require.Panics(t, func() {
		scrubber.ScrubSensitiveData(reflect.ValueOf(1))
	})
}
