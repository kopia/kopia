package cli

var keyringEnabled = app.Flag("use-keyring", "Use Gnome Keyring for storing repository password.").Default("false").Bool()
