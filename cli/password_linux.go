package cli

import "github.com/kopia/kopia/repo"

func init() {
	app.Flag("use-keyring", "Use Gnome Keyring for storing repository password.").Default("false").BoolVar(&repo.KeyRingEnabled)
}
