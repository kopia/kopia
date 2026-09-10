package email

import "time"

// TestingSetSendTimeout overrides the SMTP send timeout and returns a function that
// restores the previous value.
func TestingSetSendTimeout(d time.Duration) func() {
	old := smtpSendTimeout
	smtpSendTimeout = d

	return func() { smtpSendTimeout = old }
}
