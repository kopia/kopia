package cli

import "github.com/kopia/kopia/repo"

func init() {
	app.Flag("use-credential-manager", "Use Windows Credential Manager for storing repository password.").Default("true").BoolVar(&repo.KeyRingEnabled)
}
