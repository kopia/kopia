package cli

import (
	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/internal/secrets"
)

// secretVar is called by kingpin to handle Secret arguments.
func secretVar(s kingpin.Settings, target **secrets.Secret) {
	if *target == nil {
		secret := secrets.Secret{}
		*target = &secret
	}

	s.SetValue(*target)
}

// secretVarWithEnv is called by kingpin to handle Secret arguments with a default environment variable.
// Use this instead of kingpin's EnvName because it provides no limitations on the password value.
func secretVarWithEnv(s kingpin.Settings, envvar string, target **secrets.Secret) {
	if *target == nil {
		secret := secrets.Secret{}
		*target = &secret
	}

	(*target).Type = secrets.EnvVar
	(*target).Input = envvar

	s.SetValue(*target)
}
