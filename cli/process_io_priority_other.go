//go:build !windows

package cli

func (c *App) maybeApplyProcessIOPriority() error {
	return nil
}
