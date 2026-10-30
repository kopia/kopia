package ospath_test

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/ospath"
)

func TestIsAbs(t *testing.T) {
	var absCases []string

	notAbsCases := []string{
		"foo",
		"foo/",
		"foo/bar",
		"./foo",
		"./foo/bar",
		"../foo",
		"../foo/",
		"../foo/bar",
		".",
		"..",
		"../",
		"../..",
	}

	if runtime.GOOS == "windows" {
		absCases = append(absCases,
			"c:\\",
			"c:\\foo",
			"c:\\foo\\",
			"c:\\foo\\bar",
			"\\\\host\\share",
			"\\\\host\\share\\",
			"\\\\host\\share\\subdir",
		)

		notAbsCases = append(notAbsCases,
			"..\\",
			"..\\..",
			"foo",
			"foo\\",
			"foo\\bar",
			".\\foo",
			".\\foo\\bar",
			"..\\foo",
			"..\\foo\\",
			"..\\foo\\bar",
			"\\\\host",
			"\\\\host\\",
		)
	} else {
		absCases = append(absCases,
			"/",
			"/foo",
			"/foo/",
			"/foo/bar",
		)
	}

	for _, tc := range absCases {
		require.True(t, ospath.IsAbs(tc), tc)
	}

	for _, tc := range notAbsCases {
		require.False(t, ospath.IsAbs(tc), tc)
	}
}
