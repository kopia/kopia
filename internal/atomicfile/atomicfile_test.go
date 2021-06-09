package atomicfile

import (
	"runtime"
	"strings"
	"testing"
)

var veryLongSegment = strings.Repeat("f", 270)

func TestMaybePrefixLongFilenameOnWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		return
	}

	cases := []struct {
		input string
		want  string
	}{
		// too short
		{"C:\\Short.txt", "C:\\Short.txt"},

		// long paths
		{"C:\\" + veryLongSegment + "\\foo", "\\\\?\\C:\\" + veryLongSegment + "\\foo"},
		{"C:\\" + veryLongSegment + "/foo/bar", "\\\\?\\C:\\" + veryLongSegment + "\\foo\\bar"},
		{"C:\\" + veryLongSegment + "/foo/./././bar", "\\\\?\\C:\\" + veryLongSegment + "\\foo\\bar"},
		{"C:\\" + veryLongSegment + "\\.\\foo", "\\\\?\\C:\\" + veryLongSegment + "\\foo"},
		{"C:\\" + veryLongSegment + "/.\\foo", "\\\\?\\C:\\" + veryLongSegment + "\\foo"},
		{"C:\\" + veryLongSegment + "\\./foo", "\\\\?\\C:\\" + veryLongSegment + "\\foo"},
		{"\\\\?\\C:\\" + veryLongSegment + "\\foo", "\\\\?\\C:\\" + veryLongSegment + "\\foo"},

		// relative
		{veryLongSegment + "\\foo", veryLongSegment + "\\foo"},
		{"./" + veryLongSegment + "\\foo", "./" + veryLongSegment + "\\foo"},
		{"../../" + veryLongSegment + "\\foo", "../../" + veryLongSegment + "\\foo"},
		{"..\\..\\" + veryLongSegment + "\\foo", "..\\..\\" + veryLongSegment + "\\foo"},
	}

	for _, tc := range cases {
		if got := MaybePrefixLongFilenameOnWindows(tc.input); got != tc.want {
			t.Errorf("invalid result for %v: got %v, want %v", tc.input, got, tc.want)
		}
	}
}
