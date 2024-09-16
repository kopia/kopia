package testutil

import "strings"

// Pattern in stderr that `kopia server` uses to pass ephemeral data.
const (
	serverOutputAddress         = "SERVER ADDRESS: "
	serverOutputCertSHA256      = "SERVER CERT SHA256: "
	serverOutputPassword        = "SERVER PASSWORD: "
	serverOutputControlPassword = "SERVER CONTROL PASSWORD: "
	serverOutputMetricsAddress  = "starting prometheus metrics on "
)

// ServerParameters encapsulates parameters captured by processing stderr of
// 'kopia server start'.
type ServerParameters struct {
	BaseURL               string
	SHA256Fingerprint     string
	Password              string
	ServerControlPassword string
	MetricsAddress        string
}

// ProcessOutput processes output lines from a server that's starting up.
func (s *ServerParameters) ProcessOutput(l string) bool {
	if strings.HasPrefix(l, serverOutputAddress) {
		s.BaseURL = strings.TrimPrefix(l, serverOutputAddress)
		return false
	}

	if strings.HasPrefix(l, serverOutputCertSHA256) {
		s.SHA256Fingerprint = strings.TrimPrefix(l, serverOutputCertSHA256)
	}

	if strings.HasPrefix(l, serverOutputPassword) {
		s.Password = strings.TrimPrefix(l, serverOutputPassword)
	}

	if strings.HasPrefix(l, serverOutputControlPassword) {
		s.ServerControlPassword = strings.TrimPrefix(l, serverOutputControlPassword)
	}

	if strings.HasPrefix(l, serverOutputMetricsAddress) {
		s.MetricsAddress = strings.TrimPrefix(l, serverOutputMetricsAddress)
	}

	return true
}
