package content

import "time"

// BackupMetadata contains metadata information about a backup.
type BackupMetadata struct {
	// HostName is the name of the host where the backup was taken.
	HostName string `json:"hostname"`

	// Description is an optional user-provided description of the backup.
	Description string `json:"description"`

	// Directory is the directory path.
	Directory string `json:"directory"`

	// User is the name of the user who started the backup.
	User string `json:"user"`
}

// BackupManifest contains backup manifest.
type BackupManifest struct {
	BackupMetadata

	// StartTime is the time when the backup was started.
	StartTime time.Time `json:"startTime"`

	// EndTime is the time when the backup has finished.
	EndTime time.Time `json:"endTime"`

	// RootObjectID is the ObjectID of the root directory.
	RootObjectID string `json:"rootObject"`
}
