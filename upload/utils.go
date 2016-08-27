package upload

import "strings"

func isLess(name1, name2 string) bool {
	if name1 == name2 {
		return false
	}

	return isLessOrEqual(name1, name2)
}

func isLessOrEqual(name1, name2 string) bool {
	parts1 := strings.Split(name1, "/")
	parts2 := strings.Split(name2, "/")

	i := 0
	for i < len(parts1) && i < len(parts2) {
		if parts1[i] == parts2[i] {
			i++
			continue
		}
		if parts1[i] == "" {
			return false
		}
		if parts2[i] == "" {
			return true
		}
		return parts1[i] < parts2[i]
	}

	return len(parts1) <= len(parts2)
}
