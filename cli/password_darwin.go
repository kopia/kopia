package cli

import "github.com/kopia/kopia/repo"

func init() {
	app.Flag("use-keychain", "Use macOS Keychain for storing repository password.").Default("true").BoolVar(&repo.KeyRingEnabled)
}
