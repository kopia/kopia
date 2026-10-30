package ospath

import (
	"runtime"
	"strings"
	"testing"
)

func TestSafeLongFilename_Windows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows-only test")
	}

	veryLongSegment := strings.Repeat("f", 270)

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
		if got := SafeLongFilename(tc.input); got != tc.want {
			t.Errorf("invalid result for %v: got %v, want %v", tc.input, got, tc.want)
		}
	}
}
