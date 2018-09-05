package cli

var keyringEnabled = app.Flag("use-credential-manager", "Use Windows Credential Manager for storing repository password.").Default("true").Bool()
