package cli

func onExternalConfigReloadRequest(f func()) {
	// SIGHUP not supported on Windows.
}
