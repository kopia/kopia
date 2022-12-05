package metricid_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/metricid"
)

func TestMapToSlice(t *testing.T) {
	m := metricid.NewMapping(map[string]int{
		"a": 1,
		"b": 2,
		"d": 4,
	})

	cases := []struct {
		input map[string]string
		want  []string
	}{
		{nil, []string{"", "", "", ""}},
		{map[string]string{"a": "foo", "b": "bar"}, []string{"foo", "bar", "", ""}},
		{map[string]string{"a": "foo", "b": "bar", "d": "baz"}, []string{"foo", "bar", "", "baz"}},

		// 'c' is dropped since it's not in the mapping
		{map[string]string{"a": "foo", "b": "bar", "c": "qux", "d": "baz"}, []string{"foo", "bar", "", "baz"}},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, metricid.MapToSlice(m, tc.input))
	}
}

func TestSliceToMap(t *testing.T) {
	m := metricid.NewMapping(map[string]int{
		"a": 1,
		"b": 2,
		"c": 3,
	})

	cases := []struct {
		input []int
		want  map[string]int
	}{
		{[]int{3}, map[string]int{"a": 3}},
		{[]int{3, 4, 5}, map[string]int{"a": 3, "b": 4, "c": 5}},
		{[]int{3, 4, 5, 6, 7}, map[string]int{"a": 3, "b": 4, "c": 5}},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, metricid.SliceToMap(m, tc.input))
	}
}
