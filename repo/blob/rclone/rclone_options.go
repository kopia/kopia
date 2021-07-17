package rclone

// Options defines options for RClone storage.
type Options struct {
	RemotePath         string   `json:"remotePath"`                   // remote:path supported by RClone
	RCloneExe          string   `json:"rcloneExe,omitempty"`          // path to rclone executable
	RCloneArgs         []string `json:"rcloneArgs,omitempty"`         // additional rclone arguments
	RCloneEnv          []string `json:"rcloneEnv,omitempty"`          // additional rclone environment variables
	StartupTimeout     int      `json:"startupTimeout,omitempty"`     // time to wait for rclone to start
	Debug              bool     `json:"debug,omitempty"`              // log rclone output
	NoWaitForTransfers bool     `json:"noWaitForTransfers,omitempty"` // when set to true, don't wait for transfers to finish when closing
	DirectoryShards    []int    `json:"dirShards"`
	EmbeddedConfig     string   `json:"embeddedConfig,omitempty"`
	ListParallelism    int      `json:"listParallelism,omitempty"`
}
