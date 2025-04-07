// Package passwordpersist manages password persistence.
package passwordpersist

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/logging"
)

// ErrPasswordNotFound is returned when a password cannot be found in a persistent storage.
var ErrPasswordNotFound = errors.New("password not found")

// ErrUnsupported is returned when a password storage is not supported.
var ErrUnsupported = errors.New("password storage not supported")

var log = logging.Module("passwordpersist")

// Strategy encapsulates persisting and fetching passwords.
type Strategy interface {
	// GetPassword gets persisted password, returns ErrNotFound or fatal errors.
	GetPassword(ctx context.Context, configFile string) (string, error)

	// PersistPassword persists a password, returns ErrUnsupported or fatal errors.
	PersistPassword(ctx context.Context, configFile, password string) error

	// DeletePassword deletes any persisted password, returns fatal errors.
	DeletePassword(ctx context.Context, configFile string) error
}

// OnSuccess is a helper that persists the given (configFile,password) if the provided err is nil
// and deletes any persisted password otherwise.
func OnSuccess(ctx context.Context, err error, s Strategy, configFile, password string) error {
	if err != nil {
		if err2 := s.DeletePassword(ctx, configFile); err2 != nil {
			log(ctx).Infof("unable to delete persistent password: %v", err2)
		}

		return err
	}

	return errors.Wrap(s.PersistPassword(ctx, configFile, password), "unable to persist password")
}
