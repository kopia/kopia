package hashcache

import "testing"

func TestDirectoryNameOrder(t *testing.T) {
	sortedNames := []string{
		"a/a/a",
		"a/b/c",
		"a/a",
		"a/b",
		"a/b1",
		"a/b2",
		"bar/a/d/a",
		"bar/a/a",
		"bar/a/b",
		"bar/a1/a",
		"bar/a.b",
		"bar/a2",
		"bar/a3",
		"foo/a/a",
		"foo/c/a",
		"foo/b",
		"goo/a/a",
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
