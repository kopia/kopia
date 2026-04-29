package storj

import (
	"time"

	"github.com/kopia/kopia/repo/blob/throttling"
)

var (
	testGrant       = "16hYVv3kjmaiEEdx9mvLqgNVAkejuQg6qGyTTwb2u7wDn2KWDb8EeHUrSu7sdDjPqbUZrQDRQXg2B5kKCUiHDvDDKqKYTbpT6MrEYASWnCxJbQhCnTT8yY6zPDXn1txbEditr81oroDEyABUDek3dPWfWmh2XaXMzkCLf91R9QeeerrVPCZGfyuFqciNtFeheCYN7keawj1j5wYnbQBAJC65Qz9JBFGZ3t8N4ovaqYpRqy3dj6YVyen32CUptAQEDrywShd3aB7At1Yuej81gAs58DzL7W5749zB8uzekP4pCyP6X8cpdBd3w99JgeVT239WiE4G2R28xL" //nolint:lll
	testPassPhrase  = "KopiaTestStorjPass!"
	testAccessName  = "kopia-storj"
	testBucketName  = "kopia-storj"
	testId          = "my-blob-id-122345"
	optionsTestRepo = Options{
		BucketName:  testBucketName,
		Limits:      throttling.Limits{},
		AccessName:  testAccessName,
		KeyOrGrant:  testGrant,
		Passphrase:  testPassPhrase,
		PointInTime: &time.Time{},
	}

	providerCreds = map[string]*Options{
		"kopia@storj": &optionsTestRepo,
	}
)

// GetUplinkCreds returns the test uplink credentials.
func GetUplinkCreds() *Options {
	return providerCreds["kopia@storj"]
}
