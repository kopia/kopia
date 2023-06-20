package cli

//nolint:revive
func onExternalConfigReloadRequest(f func()) {
	// SIGHUP not supported on Windows.
}
