package sharded

// Options must be anonymously embedded in sharded provider options.
type Options struct {
	DirectoryShards []int `json:"dirShards"`
	ListParallelism int   `json:"listParallelism,omitempty"`
}
