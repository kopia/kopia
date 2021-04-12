package restore

// SafelySuffixablePath returns true if path can be suffixed with the
// placeholder suffix and written to the filesystem.
func SafelySuffixablePath(path string) bool {
	return true
}
