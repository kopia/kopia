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
	InnerPtr      *Q
	InnerIf       interface{}
	InnerStruct   Q
	NilPtr        *Q
	NilIf         interface{}
}

type Q struct {
	SomePassword1 string `kopia:"sensitive"`
	NonPassword   string
}

func TestScrubber(t *testing.T) {
	input := &S{
		SomePassword1: "foo",
		NonPassword:   "bar",
		InnerPtr: &Q{
			SomePassword1: "foo",
			NonPassword:   "bar",
		},
		InnerStruct: Q{
			SomePassword1: "foo",
			NonPassword:   "bar",
		},
		InnerIf: Q{
			SomePassword1: "foo",
			NonPassword:   "bar",
		},
		NilPtr: nil,
		NilIf:  nil,
	}

	want := &S{
		SomePassword1: "***",
		NonPassword:   "bar",
		InnerPtr: &Q{
			SomePassword1: "***",
			NonPassword:   "bar",
		},
		InnerStruct: Q{
			SomePassword1: "***",
			NonPassword:   "bar",
		},
		InnerIf: Q{
			SomePassword1: "***",
			NonPassword:   "bar",
		},
		NilPtr: nil,
		NilIf:  nil,
	}

	output := scrubber.ScrubSensitiveData(reflect.ValueOf(input)).Interface()
	require.Equal(t, want, output)
}

func TestScrubberPanicsOnNonStruct(t *testing.T) {
	require.Panics(t, func() {
		scrubber.ScrubSensitiveData(reflect.ValueOf(1))
	})
}
