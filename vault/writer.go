package vault

// Writer allows writing to a vault.
type Writer interface {
	Put(id string, b []byte) error
}
