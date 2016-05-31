package vault

import "github.com/kopia/kopia/repo"

// Reader allows reading from a vault.
type Reader interface {
	Get(id string, content interface{}) error
	GetRaw(id string) ([]byte, error)
	List(prefix string) ([]string, error)
	ResolveObjectID(id string) (repo.ObjectID, error)
	OpenRepository() (repo.Repository, error)
}
