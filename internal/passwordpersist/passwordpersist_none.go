package passwordpersist

import "context"

// None is a strategy that does not persist the password at all.
func None() Strategy {
	return noneStrategy{}
}

type noneStrategy struct{}

//nolint:revive
func (noneStrategy) GetPassword(ctx context.Context, configFile string) (string, error) {
	return "", ErrPasswordNotFound
}

//nolint:revive
func (noneStrategy) PersistPassword(ctx context.Context, configFile, password string) error {
	// silently succeed
	return nil
}

//nolint:revive
func (noneStrategy) DeletePassword(ctx context.Context, configFile string) error {
	return nil
}
