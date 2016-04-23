package backup

import "time"

type Manifest struct {
	StartTime time.Time `json:"time"`
	EndTime   time.Time `json:"time"`

	HostName        string `json:"host"`
	UserName        string `json:"userName"`
	Alias           string `json:"alias"`
	SourceDirectory string `json:"source"`
	RootObjectID    string `json:"root"`

	FileCount      int64 `json:"fileCount"`
	DirectoryCount int64 `json:"dirCount"`
	TotalFileSize  int64 `json:"totalSize"`
}
