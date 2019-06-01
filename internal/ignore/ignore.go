// Package ignore implements ignoring files based on 'gitignore' syntax.
package ignore

import (
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

// Matcher returns true if the given path matches the pattern.
type Matcher func(path string, isDir bool) bool

type nameMatcher func(path string) bool

// ParseGitIgnore returns a Matcher for a given gitignore-formatted pattern.
func ParseGitIgnore(baseDir string, pattern string) (Matcher, error) {
	if !strings.HasSuffix(baseDir, "/") {
		baseDir += "/"
	}

	var dirOnly bool
	var negate bool

	// Trailing spaces are ignored unless they are quoted with backslash ("\").
	if !strings.HasSuffix(pattern, "\\ ") {
		pattern = strings.TrimSpace(pattern)
	}

	// If the pattern ends with a slash, it is removed for the purpose of the following description,
	// but it would only find a match with a directory.
	if strings.HasSuffix(pattern, "/") {
		pattern = strings.TrimSuffix(pattern, "/")
		dirOnly = true
	}

	// An optional prefix "!" which negates the pattern;
	if strings.HasPrefix(pattern, "!") {
		pattern = strings.TrimPrefix(pattern, "!")
		negate = true
	}

	// Put a backslash ("\") in front of the first "!" for patterns that begin with a literal "!"
	if strings.HasPrefix(pattern, "\\!") {
		pattern = pattern[1:]
	}

	var m nameMatcher
	if !strings.Contains(pattern, "/") {
		m = parseGlobPattern(pattern)
	} else {
		var err error
		m, err = parseNonGlobPattern(pattern)
		if err != nil {
			return nil, err
		}
	}

	return maybeNegateMatch(maybeMatchDirOnly(matchBaseDir(baseDir, m), dirOnly), negate), nil
}

func matchBaseDir(baseDir string, m nameMatcher) nameMatcher {
	return func(path string) bool {
		if !strings.HasPrefix(path, baseDir) {
			return false
		}

		path = path[len(baseDir):]
		return m(path)
	}
}

func maybeNegateMatch(m Matcher, negate bool) Matcher {
	if !negate {
		return m
	}

	return func(path string, isDir bool) bool {
		return !m(path, isDir)
	}
}

func maybeMatchDirOnly(m nameMatcher, dirOnly bool) Matcher {
	if dirOnly {
		return func(path string, isDir bool) bool {
			if !isDir {
				return false
			}

			return m(path)
		}
	}

	return func(path string, isDir bool) bool {
		return m(path)
	}
}

func parseGlobPattern(pattern string) nameMatcher {
	return func(path string) bool {
		last := path[strings.LastIndex(path, "/")+1:]
		ok, _ := filepath.Match(pattern, last)
		return ok
	}
}

func parseNonGlobPattern(pattern string) (nameMatcher, error) {
	// No double-star pattern
	if !strings.Contains(pattern, "**") {
		return func(path string) bool {
			return path == pattern
		}, nil
	}

	// A leading "**" followed by a slash means match in all directories.
	// For example, "**/foo" matches file or directory "foo" anywhere,
	// the same as pattern "foo". "**/foo/bar" matches file or directory
	// "bar" anywhere that is directly under directory "foo".
	if strings.HasPrefix(pattern, "**/") {
		suffix := strings.TrimPrefix(pattern, "**/")
		suffixWithSlash := strings.TrimPrefix(pattern, "**")
		return func(path string) bool {
			return path == suffix || strings.HasSuffix(path, suffixWithSlash)
		}, nil
	}

	// A trailing "/**" matches everything inside. For example, "abc/**" matches all files inside
	// directory "abc", relative to the location of the .gitignore file, with infinite depth.
	if strings.HasSuffix(pattern, "/**") {
		prefix := strings.TrimSuffix(pattern, "/**")
		prefixWithSlash := strings.TrimSuffix(pattern, "**")
		return func(path string) bool {
			return path == prefix || strings.HasPrefix(path, prefixWithSlash)
		}, nil
	}

	// A slash followed by two consecutive asterisks then a slash matches zero or more directories.
	// For example, "a/**/b" matches "a/b", "a/x/b", "a/x/y/b" and so on.
	if index := strings.Index(pattern, "/**/"); index >= 0 {
		prefixWithSlash := pattern[0 : index+1]
		suffixWithSlash := pattern[index+3:]

		return func(path string) bool {
			return strings.HasPrefix(path, prefixWithSlash) && strings.HasSuffix(path, suffixWithSlash)
		}, nil
	}

	// Other consecutive asterisks are considered invalid.
	return nil, errors.Errorf("invalid pattern: '%v'", pattern)
}
