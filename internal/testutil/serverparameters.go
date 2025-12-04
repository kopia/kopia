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
	if after, ok := strings.CutPrefix(l, serverOutputAddress); ok {
		s.BaseURL = after
		return false
	}

	if after, ok := strings.CutPrefix(l, serverOutputCertSHA256); ok {
		s.SHA256Fingerprint = after
	}

	if after, ok := strings.CutPrefix(l, serverOutputPassword); ok {
		s.Password = after
	}

	if after, ok := strings.CutPrefix(l, serverOutputControlPassword); ok {
		s.ServerControlPassword = after
	}

	if strings.HasPrefix(l, serverOutputMetricsAddress) {
		s.MetricsAddress = strings.TrimPrefix(l, serverOutputMetricsAddress)
	}

	return true
}
