package repolog

import "github.com/kopia/kopia/internal/repodiag"

type Provider interface {
	LogManager() *repodiag.LogManager
}
