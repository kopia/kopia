package content

import "os"

// EntryType describes the type of an backup entry.
type EntryType string

const (
	// EntryTypeFile represents a regular file.
	EntryTypeFile EntryType = "f"

	// EntryTypeDirectory represents a directory entry which is a subdirectory.
	EntryTypeDirectory EntryType = "d"

	// EntryTypeSymlink represents a symbolic link.
	EntryTypeSymlink EntryType = "l"

	// EntryTypeSocket represents a UNIX socket.
	EntryTypeSocket EntryType = "s"

	// EntryTypeDevice represents a device.
	EntryTypeDevice EntryType = "v"

	// EntryTypeNamedPipe represents a named pipe.
	EntryTypeNamedPipe EntryType = "n"
)

// FileModeToType converts os.FileMode into EntryType.
func FileModeToType(mode os.FileMode) EntryType {
	switch mode & os.ModeType {
	case os.ModeDir:
		return EntryTypeDirectory

	case os.ModeDevice:
		return EntryTypeDevice

	case os.ModeSocket:
		return EntryTypeSocket

	case os.ModeSymlink:
		return EntryTypeSymlink

	case os.ModeNamedPipe:
		return EntryTypeNamedPipe

	default:
		return EntryTypeFile
	}
}
