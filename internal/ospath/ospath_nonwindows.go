//go:build !windows

package ospath

// SafeLongFilename handles long absolute file paths in a platform-specific manner.
// Currently it only handles absolute paths on Windows. It is a no-op on other
// platforms.
//
// On Windows, the prefixes the given filename with \\?\ when the filename is
// longer than 260 characters, which is required to be able to use some
// low-level Windows APIs.
//
// Relative paths are always limited to a total of MAX_PATH characters:
// https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation
func SafeLongFilename(fname string) string {
	return fname
}
