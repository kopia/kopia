package passwordpersist

import (
	"context"

	"github.com/pkg/errors"
)

var _ Strategy = (Multiple{})

// Multiple is a Strategy that tries several underlying persistence strategies.
type Multiple []Strategy

// GetPassword retrieves the password form the first password storage that has it.
func (m Multiple) GetPassword(ctx context.Context, configFile string) (string, error) {
	for _, s := range m {
		pass, err := s.GetPassword(ctx, configFile)
		if err == nil {
			return pass, nil
		}

		if errors.Is(err, ErrPasswordNotFound) {
			// try next strategy one.
			continue
		}

		return "", errors.Wrap(err, "error getting persistent password")
	}

	return "", ErrPasswordNotFound
}

// PersistPassword persists the provided password using the first method that succeeds.
func (m Multiple) PersistPassword(ctx context.Context, configFile, password string) error {
	for _, s := range m {
		err := s.PersistPassword(ctx, configFile, password)
		if err == nil {
			return nil
		}

		if errors.Is(err, ErrUnsupported) {
			continue
		}

		return errors.Wrap(err, "error persisting password")
	}

	return ErrUnsupported
}

// DeletePassword deletes the password from all persistent storages.
func (m Multiple) DeletePassword(ctx context.Context, configFile string) error {
	for _, s := range m {
		err := s.DeletePassword(ctx, configFile)

		switch {
		case err == nil: // good
		case errors.Is(err, ErrPasswordNotFound): // ignore
		case errors.Is(err, ErrUnsupported): // ignore
		default:
			return errors.Wrap(err, "error removing password from persistent storage")
		}
	}

	return nil
}
