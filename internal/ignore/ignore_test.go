package ignore_test

import (
	"testing"

	"github.com/kopia/kopia/internal/ignore"
)

func TestIgnore(t *testing.T) {
	cases := []struct {
		pattern     string
		baseDir     string
		testPath    string
		testIsDir   bool
		shouldMatch bool
	}{
		// directory-only match
		{"bin/", "/base/dir", "/base/dir/bin", true, true},
		{"bin/", "/base/dir", "/base/dir/bin", false, false},

		// trailing space.
		{"bin/ ", "/base/dir", "/base/dir/bin", true, true},

		// escaped trailing space.
		{"bin/\\ ", "/base/dir", "/base/dir/bin", false, false},

		// negated match
		{"!foo", "/base/dir", "/base/dir/foo", false, false},
		{"!foo", "/base/dir", "/base/dir/foo2", false, true},

		// escaped !
		{"\\!important.txt", "/base/dir", "/base/dir/!important.txt", false, true},

		// glob match
		{"*.foo", "/base/dir", "/base/dir/a.foo", true, true},
		{"[b-k].foo", "/base/dir", "/base/dir/a/a.foo", true, false},
		{"[a-k].foo", "/base/dir", "/base/dir/a/a.foo", true, true},
		{"??.foo", "/base/dir", "/base/dir/a/a.foo", true, false},
		{"?.foo", "/base/dir", "/base/dir/a/a.foo", true, true},
		{"*.foo", "/base/dir", "/base/dir/a/a.foo", true, true},

		{"*.foo", "/", "/a.foo", true, true},

		// absolute match (relative to baseDir)
		//  pattern must be relative to baseDir "If there is a separator at the beginning or middle (or both) of the pattern"

		{"sub/foo", "/base/dir", "/base/dir/sub/foo", false, true},
		{"sub/bar/", "/base/dir", "/base/dir/sub/bar", true, true},

		{"/sub/foo", "/base/dir", "/base/dir/sub/foo", false, true},
		{"/sub/bar/", "/base/dir", "/base/dir/sub/bar", true, true},

		{"bar/*.foo", "/base/dir", "/base/dir/bar/a.foo", false, true},
		{"bar/*.foo", "/base/dir", "/base/dir/sub/bar/a.foo", false, false},

		{"/bar/*.foo", "/base/dir", "/base/dir/bar/a.foo", false, true},
		{"/bar/*.foo", "/base/dir", "/base/dir/sub/bar/a.foo", false, false},

		// no match outside of base directory
		{"foo", "/base/dir", "/base/other-dir/foo", false, false},

		// double-star suffix
		{"**/foo", "/base/dir", "/base/dir/foo", false, true},
		{"**/foo", "/base/dir", "/base/dir/a/foo", false, true},
		{"**/foo", "/base/dir", "/base/dir/a/b/foo", false, true},
		{"**/foo", "/base/dir", "/base/dir/foo2", false, false}, // does not match 'foo2', only 'foo' and 'something/foo'

		// double-star prefix
		{"foo/**", "/base/dir", "/base/dir/foo", false, true},
		{"foo/**", "/base/dir", "/base/dir/foo/a", false, true},
		{"foo/**", "/base/dir", "/base/dir/foo/a/b", false, true},
		{"foo/**", "/base/dir", "/base/dir/foo2", false, false}, // does not match 'foo2', only 'foo' and 'foo/something'

		// double-star prefix and suffix
		{"foo/**/bar", "/base/dir", "/base/dir/foo/bar", false, true},
		{"foo/**/bar", "/base/dir", "/base/dir/foo/a/bar", false, true},
		{"foo/**/bar", "/base/dir", "/base/dir/foo/a/b/bar", false, true},
		{"foo/**/bar", "/base/dir", "/base/dir/foo2/a/b/bar", false, false}, // no match
		{"foo/**/bar", "/base/dir", "/base/dir/foo/a/b/bar2", false, false}, // no match
		{"foo/**/bar", "/base/dir", "/base/dir/foo/a/b/2bar", false, false}, // no match
	}

	for i, tc := range cases {
		m, err := ignore.ParseGitIgnore(tc.baseDir, tc.pattern)
		if err != nil {
			t.Errorf("error parsing %+v: %v", tc, err)
			continue
		}

		if got, want := m(tc.testPath, tc.testIsDir), tc.shouldMatch; got != want {
			t.Errorf("error matching #%v %+v: got %v want %v", i, tc, got, want)
		}
	}
}
