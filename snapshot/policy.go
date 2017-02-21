package snapshot

// Expiration describes snapshot expiration policy.
type Expiration struct {
}

// Policy describes snapshot policy for a single source.
type Policy struct {
	Source *SourceInfo
}
