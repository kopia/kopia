package snapshotfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSafeNameForMount(t *testing.T) {
	cases := map[string]string{
		"/tmp/foo/bar":             "tmp_foo_bar",
		"/root":                    "root",
		"/root/":                   "root",
		"/":                        "__root",
		"C:":                       "C",
		"C:\\":                     "C",
		"C:\\foo":                  "C_foo",
		"C:\\foo/bar":              "C_foo_bar",
		"\\\\server\\root":         "__server_root",
		"\\\\server\\root\\":       "__server_root",
		"\\\\server\\root\\subdir": "__server_root_subdir",
		"\\\\server\\root\\subdir/with/forward/slashes":  "__server_root_subdir_with_forward_slashes",
		"\\\\server\\root\\subdir/with\\mixed/slashes\\": "__server_root_subdir_with_mixed_slashes",
	}

	for input, want := range cases {
		assert.Equal(t, want, safeNameForMount(input), input)
	}
}

func TestDisambiguateSafeNames(t *testing.T) {
	cases := []struct {
		input map[string]string
		want  map[string]string
	}{
		{
			input: map[string]string{
				"c:/":  "c",
				"c:\\": "c",
				"c:":   "c",
				"c":    "c",
			},
			want: map[string]string{
				"c":    "c",
				"c:":   "c (2)",
				"c:/":  "c (3)",
				"c:\\": "c (4)",
			},
		},
		{
			input: map[string]string{
				"c:/":   "c",
				"c:\\":  "c",
				"c:":    "c",
				"c":     "c",
				"c (2)": "c (2)",
			},
			want: map[string]string{
				"c":     "c",
				"c (2)": "c (2)",
				"c:":    "c (2) (2)",
				"c:/":   "c (3)",
				"c:\\":  "c (4)",
			},
		},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, disambiguateSafeNames(tc.input))
	}
}
