package repodiag

import (
	"github.com/kopia/kopia/internal/contentlog"
)

// Provider defines the methods to retrieve log manager.
type Provider interface {
	LogManager() *LogManager
}

// EnableContentLog retrieves log manager from the carrier and enable content log.
func EnableContentLog(carrier any) {
	if p, ok := carrier.(Provider); ok {
		p.LogManager().Enable()
	}
}

// DisableContentLog retrieves log manager from the carrier and disable content log.
func DisableContentLog(carrier any) {
	if p, ok := carrier.(Provider); ok {
		p.LogManager().Disable()
	}
}

// NewContentLogger retrieves log manager from the carrier and create a new content logger.
func NewContentLogger(carrier any, name string) *contentlog.Logger {
	if p, ok := carrier.(Provider); ok {
		return p.LogManager().NewLogger(name)
	}

	return nil
}
