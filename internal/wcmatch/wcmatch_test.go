package wcmatch

import (
	"testing"
)

type wcCase struct {
	// The pattern to match the path against.
	pattern string
	// The path to match. A trailing slash in path indicates a directory.
	path string
	// Expected result for a case-sensitive match.
	cs bool
	// Expected result for a case-insensitive match.
	ci bool
}

func TestMatchWithBaseDir(t *testing.T) {
	cases := []struct {
		pattern, baseDir, path string
		expected               bool
	}{
		{"", "/base/", "", true},
		{"", "/base", "", true},
		{"*", "/base/", "/foo", false},
		{"*", "/base", "/foo", false},
		{"**", "/base/", "/foo", false},
		{"**", "/base", "/foo", false},
		{"*", "/base", "/base/foo", true},
		{"*", "/base/", "/base/foo", true},
		{"**", "/base", "/base/foo", true},
		{"**", "/base/", "/base/foo", true},
		{"*.txt", "/base/", "/base/foo.txt", true},
		{"*.txt", "/base/", "/other/foo.txt", false},
		{"/src/file.txt", "/base", "/base/src/file.txt", true},
		{"/src/file.txt", "/base", "/other/src/file.txt", false},
		{"**/src/file.txt", "/base", "/base/foo/src/file.txt", true},
		{"src/file.txt", "/base", "/base/foo/src/file.txt", false},
		{"src/file.txt", "/base", "/other/foo/src/file.txt", false},
		{"file.txt", "/base", "/base/foo/src/file.txt", true},
		{"file.txt", "/base", "/other/foo/src/file.txt", false},
	}

	for i, tc := range cases {
		isDir := false
		// Check if our test path has a trailing slash, indicating it is a directory...
		if len(tc.path) > 1 && tc.path[len(tc.path)-1] == '/' {
			// ...if so, remove the trailing slash and set isDir=true
			isDir = true
			tc.path = tc.path[:len(tc.path)-1]
		}

		matcher, err := NewWildcardMatcher(tc.pattern, BaseDir(tc.baseDir))
		if err != nil {
			t.Errorf("(%v) unexpected error returned for pattern %#v: %v", i, tc.pattern, err)
		} else {
			actual := matcher.Match(tc.path, isDir)
			if actual != tc.expected {
				t.Errorf("(%v) error matching  pattern %#v with path %#v (case-sensitive): got %v want %v", i, tc.pattern, tc.path, actual, tc.expected)
			}
		}
	}
}

func TestMatch(t *testing.T) {
	cases := []wcCase{
		// Basic
		{"", "", true, true},
		{"*", "", true, true},
		{"**", "", true, true},
		{"b", "a", false, false},
		{"*.*", "foo.txt", true, true},
		{"foo\\.txt", "foo.txt", true, true},
		{"*/", ".git/", true, true},

		// Sequences
		{"ab[cd]", "abc", true, true},
		{"ab[!de]", "abc", true, true},
		{"[\\\\]", "\\", true, true},
		{"[!\\\\]", "a", true, true},
		{"[!\\\\]", "\\", false, false},
		{"[-abc]", "-", true, true},
		{"[abc-]", "-", true, true},
		{"[[]", "[", true, true},
		{"[a-z]", "q", true, true},
		{"[a-\\z]", "q", true, true},
		{"[a-d]", "e", false, false},
		{"[-abc]", "-", true, true},
		{"[a\\-c]", "-", true, true},
		{"[a\\-c]", "b", false, false},
		{"[[:]", ":", true, true},
		{"[[:ab]", ":", true, true},
		{"[[:ab]", "[", true, true},
		{"[[:ab]", "b", true, true},
		{"[[:ab]", "c", false, false},
		{"[![:digit:]ab]", "a", false, false},
		{"[![:digit:]ab]", "3", false, false},
		{"[![:digit:]ab]", "c", true, true},

		// Case, general
		{"abc", "abc", true, true},
		{"abc", "AbC", false, true},
		{"AbC", "abc", false, true},
		{"AbC", "AbC", true, true},

		// Case, sequence
		{"[A-F]", "C", true, true},
		{"[A-F]", "c", false, true},
		{"[a-f]", "c", true, true},
		{"[a-f]", "C", false, true},

		// Basic wildcard
		{"ab*", "abcd", true, true},
		{"ab*cd", "abcd", true, true},
		{"ab*cd", "abxxxcd", true, true},
		{"ab***cd", "abcd", true, true},
		{"ab***cd", "abxxxcd", true, true},
		{"ab*", "ab/cd", false, false},
		{"ab**", "ab/cd", false, false},
		{"?*?", "abc", true, true},
		{"???*", "abc", true, true},
		{"*???", "abc", true, true},
		{"???", "abc", true, true},
		{"*", "abc", true, true},
		{"??", "a", false, false},

		// Recursive wildcards
		{"**", "foo", true, true},
		{"**", "foo/bar", true, true},
		{"/a/**/b/foo.txt", "/a/b/c/d/foo.txt", false, false},
		{"/a/**/b/foo.txt", "/a/b/c/b/foo.txt", true, true},
		{"/a/**/b/foo.txt", "/a/b/c/b/foo.txt/other", false, false},
		{"/a/**/b/foo.txt", "/a/b/c/b/foo.txt/e/b/foo.txt", true, true},
		{"foo/**", "/foo/bar/a/b/c.txt", true, true},
		{"/foo/**/bar/**/p.txt", "/foo/bar/p.txt", true, true},
		{"/foo/**/bar/**/p.txt", "/foo/x/bar/p.txt", true, true},
		{"/foo/**/bar/**/p.txt", "/foo/x/x/bar/x/y/p.txt", true, true},
		{"**/bar/a.txt", "/foo/x/x/bar/a.txt", true, true},

		// Rooted/Unrooted
		{"bar/a.txt", "/foo/bar/a.txt", false, false},
		{"/bar/a.txt", "/foo/bar/a.txt", false, false},
		{"bar/a.txt", "/bar/a.txt", true, true},
		{"*.txt", "/foo/bar/a.txt", true, true},
		{"/*.txt", "/foo/bar/a.txt", false, false},
		{"**/*.txt", "/foo/bar/a.txt", true, true},

		// Directories
		{"foo/", "/foo", false, false},
		{"foo/", "/foo/", true, true},
		{"foo/", "/foo/bar", false, false},
		{"foo/", "/foo/bar/", false, false},
		{"foo/", "/bar/foo", false, false},
		{"foo/", "/bar/foo/", true, true},
		{"foo/**/bar/", "/foo/bar", false, false},
		{"foo/**/bar/", "/foo/bar/", true, true},
		{"foo/**/bar/", "/foo/xx/yy/bar/", true, true},
		{"**/", "/foo", false, false},
		{"**/", "/foo/", true, true},

		// Negated
		{"!foo", "/bar", true, true},
		{"!foo", "foo/", false, false},
		{"!*", "/foo", false, false},
		{"!*/", "/.git/", false, false},
		{"!*/", "/foo/bar/", false, false},
		{"!foo/bar", "/foo/bar", false, false},
		{"!foo/bar", "/car/foo/bar", true, true},
		{"!foo/bar/", "/foo/bar/", false, false},
		{"!foo/bar/", "/car/foo/bar/", true, true},

		// Whitespace trimming
		{"  \tfoo", "foo", true, true},
		{"  foo  ", "foo", true, true},
		{" foo\\ ", "/foo ", true, true},

		{"/users/user1/logs", "/users/user1/logs", true, true},
		{"/users/**/logs", "/users/user1/logs", true, true},
		{"/users/user?/logs", "/users/user1/logs", true, true},
		{"/users/user*/logs", "/users/user1/logs", true, true},
		{"/users/*/logs", "/users/user1/logs", true, true},
	}

	testHelper(t, cases)
}

func TestErrorCases(t *testing.T) {
	cases := []struct {
		pattern string
	}{
		{"[a"},
		{"[a-"},
		{"\\"},
		{"[\\"},
		{"[a-\\"},
		{"[[:alnum"},
		{"[[:alnum:]"},
		{"[[:foobar:]]"},
	}

	for i, tc := range cases {
		m, err := NewWildcardMatcher(tc.pattern, IgnoreCase(true))
		if err == nil {
			t.Errorf("(%v) Expected error for pattern %#v but did not get one", i, tc.pattern)
		} else if m != nil {
			t.Errorf("(%v) Expected NewWildcardMatcher to return nil on error, pattern = %#v", i, tc.pattern)
		}
	}
}

func TestCharacterClasses(t *testing.T) {
	cases := []wcCase{
		// Character classes
		{"[[:alnum:]]", "c", true, true},
		{"[[:alnum:]]", "3", true, true},
		{"[[:alnum:]]", "Q", true, true},
		{"[[:alnum:]]", "Б", true, true},
		{"[[:alnum:]]", ".", false, false},
		{"[[:alnum:]]", " ", false, false},
		{"[[:alnum:]]", "\n", false, false},

		{"[[:alpha:]]", "c", true, true},
		{"[[:alpha:]]", "3", false, false},
		{"[[:alpha:]]", "Q", true, true},
		{"[[:alpha:]]", "Б", true, true},
		{"[[:alpha:]]", ".", false, false},
		{"[[:alpha:]]", " ", false, false},
		{"[[:alpha:]]", "\n", false, false},

		{"[[:ascii:]]", "c", true, true},
		{"[[:ascii:]]", "3", true, true},
		{"[[:ascii:]]", "Q", true, true},
		{"[[:ascii:]]", "Б", false, false},
		{"[[:ascii:]]", ".", true, true},
		{"[[:ascii:]]", " ", true, true},
		{"[[:ascii:]]", "\n", true, true},

		{"[[:blank:]]", "c", false, false},
		{"[[:blank:]]", "3", false, false},
		{"[[:blank:]]", "Q", false, false},
		{"[[:blank:]]", "Б", false, false},
		{"[[:blank:]]", ".", false, false},
		{"[[:blank:]]", " ", true, true},
		{"[[:blank:]]", "\t", true, true},
		{"[[:blank:]]", "\n", false, false},

		{"[[:cntrl:]]", "c", false, false},
		{"[[:cntrl:]]", "3", false, false},
		{"[[:cntrl:]]", "Q", false, false},
		{"[[:cntrl:]]", "Б", false, false},
		{"[[:cntrl:]]", ".", false, false},
		{"[[:cntrl:]]", " ", false, false},
		{"[[:cntrl:]]", "\t", true, true},
		{"[[:cntrl:]]", "\n", true, true},

		{"[[:digit:]]", "c", false, false},
		{"[[:digit:]]", "3", true, true},
		{"[[:digit:]]", "Q", false, false},
		{"[[:digit:]]", "Б", false, false},
		{"[[:digit:]]", ".", false, false},
		{"[[:digit:]]", " ", false, false},
		{"[[:digit:]]", "۳", true, true}, // a farsi digit
		{"[[:digit:]]", "\n", false, false},

		{"[[:graph:]]", "c", true, true},
		{"[[:graph:]]", "3", true, true},
		{"[[:graph:]]", "Q", true, true},
		{"[[:graph:]]", "Б", true, true},
		{"[[:graph:]]", ".", true, true},
		{"[[:graph:]]", " ", true, true},
		{"[[:graph:]]", "۳", true, true}, // a farsi digit
		{"[[:graph:]]", "\n", false, false},

		{"[[:lower:]]", "c", true, true},
		{"[[:lower:]]", "C", false, true},
		{"[[:lower:]]", "3", false, false},
		{"[[:lower:]]", "Б", false, true},
		{"[[:lower:]]", ".", false, false},
		{"[[:lower:]]", " ", false, false},
		{"[[:lower:]]", "ε", true, true},
		{"[[:lower:]]", "\n", false, false},

		{"[[:print:]]", "c", true, true},
		{"[[:print:]]", "C", true, true},
		{"[[:print:]]", "3", true, true},
		{"[[:print:]]", "Б", true, true},
		{"[[:print:]]", ".", true, true},
		{"[[:print:]]", " ", true, true},
		{"[[:print:]]", "ε", true, true},
		{"[[:print:]]", "\n", false, false},

		{"[[:punct:]]", "c", false, false},
		{"[[:punct:]]", "C", false, false},
		{"[[:punct:]]", "3", false, false},
		{"[[:punct:]]", "Б", false, false},
		{"[[:punct:]]", ".", true, true},
		{"[[:punct:]]", " ", false, false},
		{"[[:punct:]]", "!", true, true},
		{"[[:punct:]]", "\n", false, false},
		{"[[:punct:]]", "$", true, true},

		{"[[:space:]]", "c", false, false},
		{"[[:space:]]", "3", false, false},
		{"[[:space:]]", "Q", false, false},
		{"[[:space:]]", "Б", false, false},
		{"[[:space:]]", ".", false, false},
		{"[[:space:]]", " ", true, true},
		{"[[:space:]]", "\t", true, true},
		{"[[:space:]]", "\n", true, true},

		{"[[:upper:]]", "c", false, true},
		{"[[:upper:]]", "C", true, true},
		{"[[:upper:]]", "3", false, false},
		{"[[:upper:]]", "Б", true, true},
		{"[[:upper:]]", ".", false, false},
		{"[[:upper:]]", " ", false, false},
		{"[[:upper:]]", "ε", false, true},
		{"[[:upper:]]", "\n", false, false},

		{"[[:xdigit:]]", "a", true, true},
		{"[[:xdigit:]]", "A", true, true},
		{"[[:xdigit:]]", "f", true, true},
		{"[[:xdigit:]]", "F", true, true},
		{"[[:xdigit:]]", "3", true, true},
		{"[[:xdigit:]]", "Б", false, false},
		{"[[:xdigit:]]", ".", false, false},
		{"[[:xdigit:]]", " ", false, false},
		{"[[:xdigit:]]", "G", false, false},
		{"[[:xdigit:]]", "\n", false, false},
	}

	testHelper(t, cases)
}

func testHelper(t *testing.T, cases []wcCase) {
	t.Helper()

	for i, tc := range cases {
		isDir := false

		// Check if our test path has a trailing slash, indicating it is a directory...
		if len(tc.path) > 1 && tc.path[len(tc.path)-1] == '/' {
			// ...if so, remove the trailing slash and set isDir=true
			isDir = true
			tc.path = tc.path[:len(tc.path)-1]
		}

		matcherCI, err := NewWildcardMatcher(tc.pattern, IgnoreCase(true))
		if err != nil {
			t.Errorf("(%v) unexpected error returned for pattern %#v: %v", i, tc.pattern, err)
		} else {
			actualCI := matcherCI.Match(tc.path, isDir)
			if actualCI != tc.ci {
				t.Errorf("(%v) error matching  pattern %#v with path %#v (case-insensitive): got %v want %v", i, tc.pattern, tc.path, actualCI, tc.ci)
			}
		}

		matcherCS, err := NewWildcardMatcher(tc.pattern)
		if err != nil {
			t.Errorf("(%v) unexpected error returned for pattern %#v: %v", i, tc.pattern, err)
		} else {
			actualCS := matcherCS.Match(tc.path, isDir)
			if actualCS != tc.cs {
				t.Errorf("(%v) error matching  pattern %#v with path %#v (case-sensitive): got %v want %v", i, tc.pattern, tc.path, actualCS, tc.cs)
			}
		}
	}
}
