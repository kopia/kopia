package vault

import "github.com/kopia/kopia/repo"

// Reader allows reading from a vault.
type Reader interface {
	Get(id string) ([]byte, error)
	List(prefix string) ([]string, error)
	OpenRepository() (*repo.Repository, error)
}
