package cli

var keyringEnabled = app.Flag("use-keychain", "Use macOS Keychain for storing repository password.").Default("true").Bool()
