package email

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnvelopeAddress(t *testing.T) {
	require.Equal(t, "user@example.com", envelopeAddress("user@example.com"))
	require.Equal(t, "user@example.com", envelopeAddress("Kopia <user@example.com>"))
	require.Equal(t, "user@example.com", envelopeAddress(`"Doe, John" <user@example.com>`))
	require.Equal(t, "not-an-address", envelopeAddress("not-an-address"))
}

func TestEnvelopeAddresses(t *testing.T) {
	require.Equal(t, []string{"a@example.com"}, envelopeAddresses("a@example.com"))
	require.Equal(t, []string{"a@example.com", "b@example.com"}, envelopeAddresses("a@example.com,b@example.com"))
	require.Equal(t, []string{"a@example.com", "b@example.com"}, envelopeAddresses("a@example.com, b@example.com"))
	require.Equal(t, []string{"a@example.com", "b@example.com"}, envelopeAddresses(`"Doe, John" <a@example.com>, B <b@example.com>`))
	require.Equal(t, []string{"one", "two"}, envelopeAddresses("one,two"))
}
