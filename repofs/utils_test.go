package repofs

import "testing"

func TestDirectoryNameOrder(t *testing.T) {
	sortedNames := []string{
		"a/a/a",
		"a/a/",
		"a/b",
		"a/b1",
		"a/b2",
		"a/",
		"bar/a/a",
		"bar/a/",
		"bar/a.b",
		"bar/a.c/",
		"bar/a1/a",
		"bar/a1/",
		"bar/a2",
		"bar/a3",
		"bar/",
		"foo/a/a",
		"foo/a/",
		"foo/b",
		"foo/c/a",
		"foo/c/",
		"foo/d/",
		"foo/e1/",
		"foo/e2/",
		"foo/",
		"goo/a/a",
		"goo/a/",
		"goo/",
	}

	for i, n1 := range sortedNames {
		for j, n2 := range sortedNames {
			expected := i <= j
			actual := isLessOrEqual(n1, n2)
			if actual != expected {
				t.Errorf("unexpected value for isLessOrEqual('%v','%v'), expected: %v, got: %v", n1, n2, expected, actual)
			}
		}
	}
}
